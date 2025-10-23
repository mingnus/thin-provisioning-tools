use anyhow::{anyhow, Result};
use std::sync::Arc;

use crate::checksum;
use crate::io_engine::IoEngine;
use crate::pdata::btree::{self, *};
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::common::*;
use crate::pdata::space_map::metadata::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;
use crate::report::Report;

//------------------------------------------

pub struct BitmapLeak {
    pub blocknr: u64, // blocknr for the first entry in the bitmap
    pub loc: u64,     // location of the bitmap
}

//------------------------------------------

struct OverflowChecker<'a> {
    kind: &'a str,
    sm: &'a dyn SpaceMap,
}

impl<'a> OverflowChecker<'a> {
    fn new(kind: &'a str, sm: &'a dyn SpaceMap) -> OverflowChecker<'a> {
        OverflowChecker { kind, sm }
    }
}

impl NodeVisitor<u32> for OverflowChecker<'_> {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[u32],
    ) -> btree::Result<()> {
        for n in 0..keys.len() {
            let k = keys[n];
            let v = values[n];
            let expected = self.sm.get(k).unwrap();
            if expected != v {
                return Err(value_err(format!(
                    "Bad reference count for {} block {}.  Expected {}, but space map contains {}.",
                    self.kind, k, expected, v
                )));
            }
        }

        Ok(())
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> btree::Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> btree::Result<()> {
        Ok(())
    }
}

//------------------------------------------

fn inc_entries(sm: &ASpaceMap, entries: &[IndexEntry]) -> Result<()> {
    let mut sm = sm.lock().unwrap();
    for ie in entries {
        // FIXME: checksumming bitmaps?
        sm.inc(ie.blocknr, 1)?;
    }
    Ok(())
}

// Compare the reference counts in bitmaps against the expected values
//
// `sm` - The in-core space map of expected reference counts
fn check_low_ref_counts(
    engine: &dyn IoEngine,
    report: Arc<Report>,
    kind: &str,
    entries: Vec<IndexEntry>,
    sm: ASpaceMap,
) -> Result<Vec<BitmapLeak>> {
    // gathering bitmap blocknr
    let mut blocks = Vec::with_capacity(entries.len());
    for i in &entries {
        blocks.push(i.blocknr);
    }

    // read bitmap blocks
    // FIXME: we should do this in batches
    let blocks = engine.read_many(&blocks)?;

    // compare ref-counts in bitmap blocks
    let mut leaks = 0;
    let mut failed = false;
    let mut blocknr = 0;
    let mut bitmap_leaks = Vec::new();
    let sm = sm.lock().unwrap();
    let nr_blocks = sm.get_nr_blocks()?;
    for b in blocks.iter().take(entries.len()) {
        match b {
            Err(_e) => {
                return Err(anyhow!("Unable to read bitmap block"));
            }
            Ok(b) => {
                if checksum::metadata_block_type(b.get_data()) != checksum::BT::BITMAP {
                    report.fatal(&format!(
                        "Index entry points to block ({}) that isn't a bitmap",
                        b.loc
                    ));
                    failed = true;

                    // FIXME: revert the ref-count at b.loc?
                }

                let bitmap = unpack::<Bitmap>(b.get_data())?;
                let first_blocknr = blocknr;
                let mut contains_leak = false;
                for e in bitmap.entries.iter() {
                    if blocknr >= nr_blocks {
                        break;
                    }

                    match e {
                        BitmapEntry::Small(actual) => {
                            let expected = sm.get(blocknr)?;
                            if *actual == 1 && expected == 0 {
                                leaks += 1;
                                contains_leak = true;
                            } else if *actual != expected as u8 {
                                report.fatal(&format!("Bad reference count for {} block {}.  Expected {}, but space map contains {}.",
                                          kind, blocknr, expected, actual));
                                failed = true;
                            }
                        }
                        BitmapEntry::Overflow => {
                            let expected = sm.get(blocknr)?;
                            if expected < 3 {
                                report.fatal(&format!("Bad reference count for {} block {}.  Expected {}, but space map says it's >= 3.",
                                                  kind, blocknr, expected));
                                failed = true;
                            }
                        }
                    }
                    blocknr += 1;
                }
                if contains_leak {
                    bitmap_leaks.push(BitmapLeak {
                        blocknr: first_blocknr,
                        loc: b.loc,
                    });
                }
            }
        }
    }

    if leaks > 0 {
        report.non_fatal(&format!("{} {} blocks have leaked.", leaks, kind));
    }

    if failed {
        Err(anyhow!("Fatal errors in {} space map", kind))
    } else {
        Ok(bitmap_leaks)
    }
}

fn gather_disk_index_entries(
    engine: &dyn IoEngine,
    bitmap_root: u64,
    metadata_sm: ASpaceMap,
    ignore_non_fatal: bool,
) -> Result<Vec<IndexEntry>> {
    let entries_map = btree_to_map_with_sm::<IndexEntry>(
        &mut vec![0],
        engine,
        metadata_sm.clone(),
        ignore_non_fatal,
        bitmap_root,
    )?;

    let entries: Vec<IndexEntry> = entries_map.values().cloned().collect();
    inc_entries(&metadata_sm, &entries[0..])?;

    Ok(entries)
}

fn gather_metadata_index_entries(
    engine: &dyn IoEngine,
    bitmap_root: u64,
    nr_blocks: u64,
    metadata_sm: ASpaceMap,
) -> Result<Vec<IndexEntry>> {
    let b = engine.read(bitmap_root)?;
    let entries = load_metadata_index(&b, nr_blocks)?.indexes;
    metadata_sm.lock().unwrap().inc(bitmap_root, 1)?;
    inc_entries(&metadata_sm, &entries[0..])?;

    Ok(entries)
}

//------------------------------------------

// This checks the space map and returns any leak blocks for auto-repair to process.
//
// `disk_sm` - The in-core space map of expected data block ref-counts
// `metadata_sm` - The in-core space for storing ref-counts of verified blocks
pub fn check_disk_space_map(
    engine: &dyn IoEngine,
    report: Arc<Report>,
    root: SMRoot,
    disk_sm: ASpaceMap,
    metadata_sm: ASpaceMap,
    ignore_non_fatal: bool,
) -> Result<Vec<BitmapLeak>> {
    let entries = gather_disk_index_entries(
        engine,
        root.bitmap_root,
        metadata_sm.clone(),
        ignore_non_fatal,
    )?;

    // check overflow ref-counts
    {
        let sm = disk_sm.lock().unwrap();
        let v = OverflowChecker::new("data", &*sm);
        let w = BTreeWalker::new_with_sm(engine, metadata_sm.clone(), ignore_non_fatal)?;
        w.walk(&mut vec![0], &v, root.ref_count_root)?;
    }

    // check low ref-counts in bitmaps
    check_low_ref_counts(engine, report, "data", entries, disk_sm)
}

// This checks the space map and returns any leak blocks for auto-repair to process.
//
// `metadata_sm`: The in-core space map of expected metadata block ref-counts
pub fn check_metadata_space_map(
    engine: &dyn IoEngine,
    report: Arc<Report>,
    root: SMRoot,
    metadata_sm: ASpaceMap,
    ignore_non_fatal: bool,
) -> Result<Vec<BitmapLeak>> {
    count_btree_blocks::<u32>(
        engine,
        &mut vec![0],
        root.ref_count_root,
        metadata_sm.clone(),
        false,
    )?;

    let entries = gather_metadata_index_entries(
        engine,
        root.bitmap_root,
        root.nr_blocks,
        metadata_sm.clone(),
    )?;

    // check overflow ref-counts
    {
        let sm = metadata_sm.lock().unwrap();
        let v = OverflowChecker::new("metadata", &*sm);
        let w = BTreeWalker::new(engine, ignore_non_fatal);
        w.walk(&mut vec![0], &v, root.ref_count_root)?;
    }

    // check low ref-counts in bitmaps
    check_low_ref_counts(engine, report, "metadata", entries, metadata_sm)
}

//------------------------------------------
