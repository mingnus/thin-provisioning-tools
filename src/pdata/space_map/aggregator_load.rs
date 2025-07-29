use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;

use crate::checksum;
use crate::io_engine::buffer_pool::*;
use crate::io_engine::{IoEngine, ReadHandler, BLOCK_SIZE};
use crate::pdata::btree::{self, *};
use crate::pdata::btree_walker::*;
use crate::pdata::btree_layer_walker::*;
use crate::pdata::space_map::aggregator::*;
use crate::pdata::space_map::common::*;
use crate::pdata::space_map::metadata::*;
//use crate::pdata::space_map::*;
use crate::pdata::unpack::*;

//------------------------------------------

pub enum SmType {
    DataSm,
    MetadataSm,
}

use SmType::*;

//------------------------------------------

fn inc_entries(sm: &Aggregator, entries: &[IndexEntry]) -> Result<()> {
    for ie in entries {
        sm.inc_single(ie.blocknr);
    }
    Ok(())
}

fn gather_data_index_entries(
    engine: Arc<dyn IoEngine + Send + Sync>,
    bitmap_root: u64,
    metadata_sm_agg: &Aggregator,
    ignore_non_fatal: bool,
) -> Result<Vec<IndexEntry>> {

    let entries_map = btree_to_map_with_aggregator::<IndexEntry>(
        engine,
        metadata_sm_agg,
        bitmap_root,
        ignore_non_fatal,
    )?;

    let entries: Vec<IndexEntry> = entries_map.values().cloned().collect();
    inc_entries(metadata_sm_agg, &entries[0..])?;

    Ok(entries)
}

fn gather_metadata_index_entries(
    engine: Arc<dyn IoEngine + Send + Sync>,
    bitmap_root: u64,
    nr_blocks: u64,
    metadata_sm_agg: &Aggregator,
) -> Result<Vec<IndexEntry>> {
    let b = engine.read(bitmap_root)?;
    let entries = load_metadata_index(&b, nr_blocks)?.indexes;
    metadata_sm_agg.inc_single(bitmap_root);
    inc_entries(&metadata_sm_agg, &entries[0..])?;

    Ok(entries)
}

/*
fn gather_index_entries(
    engine: Arc<dyn IoEngine + Send + Sync>,
    bitmap_root: u64,
    metadata_sm: ASpaceMap,
    ignore_non_fatal: bool,
    kind: SmType,
) -> Result<Vec<IndexEntry>> {
    match kind {
        DataSm => gather_data_index_entries(engine, bitmap_root, metadata_sm, ignore_non_fatal),
        MetadataSm => gather_metadata_index_entries(
            engine,
            bitmap_root,
            metadata_sm.get_nr_blocks()?,
            metadata_sm,
        ),
    }
}
*/

//------------------------------------------

struct IndexHandler<'a> {
    loc_to_block_index: HashMap<u64, u64>,
    aggregator: &'a Aggregator,
    batch: Vec<(u64, u32)>,
}

impl<'a> IndexHandler<'a> {
    fn new(entries: HashMap<u64, u64>, aggregator: &'a Aggregator) -> Self {
        Self {
            loc_to_block_index: entries,
            aggregator,
            batch: Vec::with_capacity(4096 * 4),
        }
    }

    fn push(&mut self, blocknr: u64, count: u32) {
        self.batch.push((blocknr, count))
    }

    fn flush_batch(&mut self) {
        self.aggregator.set_batch(&self.batch);
        self.batch.clear();
    }
}

impl<'a> ReadHandler for IndexHandler<'a> {
    fn handle(&mut self, loc: u64, data: std::io::Result<&[u8]>) {
        if let Some(block_index) = self.loc_to_block_index.get(&loc) {
            match data {
                Ok(data) => {
                    if checksum::metadata_block_type(data) != checksum::BT::BITMAP {
                        todo!();
                    }

                    let bitmap = unpack::<Bitmap>(data);
                    if bitmap.is_err() {
                        todo!();
                    }
                    let bitmap = bitmap.unwrap();

                    let mut blocknr = block_index * ENTRIES_PER_BITMAP as u64;
                    for e in bitmap.entries.iter() {
                        match e {
                            BitmapEntry::Small(count) => {
                                if *count > 0 {
                                    self.push(blocknr, *count as u32);
                                }
                            }
                            BitmapEntry::Overflow => {
                                // For overflow entries, we need to check the ref count tree
                                // We'll handle this in the next step
                            }
                        }
                        blocknr += 1;
                    }
                    self.flush_batch();
                }
                Err(_e) => {
                    todo!();
                }
            }
        } else {
            todo!();
        }
    }

    fn complete(&mut self) {}
}

pub fn read_space_map(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: SMRoot,
    ignore_non_fatal: bool,
    metadata_sm_agg: &Aggregator,
    kind: SmType,
) -> Result<(Aggregator, Vec<IndexEntry>)> {
    let nr_blocks = root.nr_blocks as usize;
    let aggregator = Arc::new(Aggregator::new(nr_blocks));

    // First, load the bitmap data
    let entries = {
        match kind {
            DataSm => gather_data_index_entries(
                engine.clone(),
                root.bitmap_root,
                metadata_sm_agg,
                ignore_non_fatal,
            )?,
            MetadataSm => gather_metadata_index_entries(
                engine.clone(),
                root.bitmap_root,
                root.nr_blocks,
                metadata_sm_agg,
            )?,
        }
    };
    eprintln!("nr index entries: {}", entries.len());

    let mut loc_to_index = HashMap::new();
    for (i, e) in entries.iter().enumerate() {
        loc_to_index.insert(e.blocknr, i as u64);
    }

    // Stick with small blocks since they're likely to be spread out.
    let io_block_size = BLOCK_SIZE;
    let buffer_size = 16 * 1024 * 1024;
    let nr_io_blocks = buffer_size / io_block_size;
    let mut pool = BufferPool::new(nr_io_blocks, io_block_size);

    let mut index_handler = IndexHandler::new(loc_to_index, &aggregator);

    let start = std::time::Instant::now();
    engine.read_blocks(&mut pool, &mut entries.iter().map(|e| e.blocknr), &mut index_handler)?;
    let duration = start.elapsed();
    eprintln!("reading bitmap blocks {:?}" , duration);

    // Now, handle the overflow entries in the ref count tree
    struct OverflowVisitor {
        aggregator: Arc<Aggregator>,
    }

    impl NodeVisitor<u32> for OverflowVisitor {
        fn visit(
            &self,
            _path: &[u64],
            _kr: &KeyRange,
            _h: &NodeHeader,
            keys: &[u64],
            values: &[u32],
        ) -> btree::Result<()> {
            let batch = keys.iter().copied().zip(values.iter().copied()).collect::<Vec<(u64, u32)>>();
            self.aggregator.set_batch(&batch);
            Ok(())
        }

        fn visit_again(&self, _path: &[u64], _b: u64) -> btree::Result<()> {
            Ok(())
        }

        fn end_walk(&self) -> btree::Result<()> {
            Ok(())
        }
    }

    let start = std::time::Instant::now();

    let visitor = Arc::new(OverflowVisitor {
        aggregator: aggregator.clone(),
    });
    let buffer_size = 16 * 1024 * 1024;
    let nr_io_blocks = buffer_size / BLOCK_SIZE;
    let mut pool = BufferPool::new(nr_io_blocks, BLOCK_SIZE);
    read_nodes(engine, visitor, &mut pool, metadata_sm_agg, root.ref_count_root, ignore_non_fatal)?;

    let duration = start.elapsed();
    eprintln!("reading ref counts tree {:?}" , duration);

    Ok((Arc::try_unwrap(aggregator).unwrap(), entries))
}

pub fn read_data_space_map(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: SMRoot,
    ignore_non_fatal: bool,
    metadata_sm_agg: &Aggregator,
) -> Result<(Aggregator, Vec<IndexEntry>)> {
    read_space_map(engine, root, ignore_non_fatal, metadata_sm_agg, DataSm)
}

pub fn read_metadata_space_map(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: SMRoot,
    ignore_non_fatal: bool,
    metadata_sm_agg: &Aggregator,
) -> Result<(Aggregator, Vec<IndexEntry>)> {
    read_space_map(engine, root, ignore_non_fatal, metadata_sm_agg, MetadataSm)
}

//------------------------------------------

use crate::pdata::space_map::base::RefCount;

pub fn repair_space_map_agg(
    engine: Arc<dyn IoEngine + Send + Sync>,
    entries: Vec<crate::pdata::space_map::checker::BitmapLeak>,
    sm: &Aggregator,
) -> Result<()> {
    let mut blocks = Vec::with_capacity(entries.len());
    for i in &entries {
        blocks.push(i.loc);
    }

    // FIXME: we should do this in batches
    let rblocks = engine.read_many(&blocks[0..])?;
    let mut write_blocks = Vec::new();

    for (i, rb) in rblocks.into_iter().enumerate() {
        if let Ok(b) = rb {
            let be = &entries[i];
            let mut blocknr = be.blocknr;
            let mut bitmap = unpack::<Bitmap>(b.get_data())?;
            for e in bitmap.entries.iter_mut() {
                if blocknr >= sm.get_nr_blocks() as u64 {
                    break;
                }

                if let BitmapEntry::Small(actual) = e {
                    let expected = sm.get(blocknr)?;
                    if *actual == 1 && expected == 0 {
                        *e = BitmapEntry::Small(0);
                    }
                }

                blocknr += 1;
            }

            let mut out = std::io::Cursor::new(b.get_data());
            bitmap.pack(&mut out)?;
            checksum::write_checksum(b.get_data(), checksum::BT::BITMAP)?;

            write_blocks.push(b);
        } else {
            return Err(anyhow::anyhow!("Unable to reread bitmap blocks for repair"));
        }
    }

    let results = engine.write_many(&write_blocks[0..])?;
    for ret in results {
        if ret.is_err() {
            return Err(anyhow::anyhow!("Unable to repair space map: {:?}", ret));
        }
    }
    Ok(())
}


