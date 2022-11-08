use anyhow::{anyhow, Result};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;

use crate::commands::engine::*;
use crate::io_engine::*;
use crate::pdata::btree::{self, *};
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::checker::*;
use crate::pdata::space_map::common::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;
use crate::report::*;
use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::superblock::*;

//------------------------------------------

struct BottomLevelVisitor {
    data_sm: ASpaceMap,
    mapped_blocks: AtomicU64,
}

//------------------------------------------

impl BottomLevelVisitor {
    fn new(data_sm: ASpaceMap) -> Self {
        Self {
            data_sm,
            mapped_blocks: AtomicU64::default(),
        }
    }
}

impl NodeVisitor<BlockTime> for BottomLevelVisitor {
    type NodeSummary = u64;

    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        _k: &[u64],
        values: &[BlockTime],
    ) -> btree::Result<Self::NodeSummary> {
        // FIXME: do other checks

        if values.is_empty() {
            return Ok(0);
        }

        let mut data_sm = self.data_sm.lock().unwrap();

        let mut start = values[0].block;
        let mut len = 1;

        for b in values.iter().skip(1) {
            let block = b.block;
            if block == start + len {
                len += 1;
            } else {
                data_sm.inc(start, len).unwrap();
                start = block;
                len = 1;
            }
        }

        let nr_entries = values.len() as u64;
        data_sm.inc(start, len).unwrap();
        self.mapped_blocks.fetch_add(nr_entries, Ordering::SeqCst);
        Ok(nr_entries)
    }

    fn visit_again(&self, _path: &[u64], _b: u64, s: Self::NodeSummary) -> btree::Result<()> {
        self.mapped_blocks.fetch_add(s, Ordering::SeqCst);
        Ok(())
    }

    fn end_walk(&self) -> btree::Result<()> {
        Ok(())
    }
}

impl SubtreeSummary for u64 {
    fn append(&mut self, other: &Self) {
        *self += *other;
    }
}

//------------------------------------------

fn inc_superblock(sm: &ASpaceMap) -> Result<()> {
    let mut sm = sm.lock().unwrap();
    sm.inc(SUPERBLOCK_LOCATION, 1)?;
    Ok(())
}

//------------------------------------------

pub struct ThinCheckOptions<'a> {
    pub input: &'a Path,
    pub engine_opts: EngineOptions,
    pub sb_only: bool,
    pub skip_mappings: bool,
    pub ignore_non_fatal: bool,
    pub auto_repair: bool,
    pub clear_needs_check: bool,
    pub override_mapping_root: Option<u64>,
    pub report: Arc<Report>,
}

struct Context {
    report: Arc<Report>,
    engine: Arc<dyn IoEngine + Send + Sync>,
    pool: ThreadPool,
}

// Check the mappings filling in the data_sm as we go.
fn check_mapping_bottom_level(
    ctx: &Context,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    data_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    roots: &BTreeMap<u64, (Vec<u64>, u64)>,
    devs: &BTreeMap<u64, DeviceDetail>,
    ignore_non_fatal: bool,
) -> Result<()> {
    ctx.report.set_sub_title("mapping tree");

    let w = Arc::new(BTreeWalker::new_with_sm(
        ctx.engine.clone(),
        metadata_sm.clone(),
        ignore_non_fatal,
    )?);

    let mapped_blocks = Arc::new(Mutex::new(BTreeMap::<u64, u64>::new()));

    // We want to print out errors as we progress, so we aggregate for each thin and print
    // at that point.
    let mut failed = false;

    if roots.len() > 64 {
        let errs = Arc::new(Mutex::new(Vec::new()));
        let deferred_map = Arc::new(Mutex::new(BTreeMap::new()));

        for (thin_id, (path, root)) in roots.iter() {
            let thin_id = *thin_id;
            let data_sm = data_sm.clone();
            let root = *root;
            let v = BottomLevelVisitor::new(data_sm);
            let w = w.clone();
            let mut path = path.clone();
            let errs = errs.clone();
            let mapped = mapped_blocks.clone();
            let dmap = deferred_map.clone();

            ctx.pool.execute(move || {
                let (dlist, result) = w.walk_with_deferred_list(&mut path, &v, root);

                if let Err(e) = result {
                    let mut errs = errs.lock().unwrap();
                    errs.push(e);
                }

                let cnt = v.mapped_blocks.load(Ordering::SeqCst);
                mapped.lock().unwrap().insert(thin_id, cnt);

                if let Some(list) = dlist {
                    dmap.lock().unwrap().insert(thin_id, list);
                }
            });
        }
        ctx.pool.join();

        // Second stage: Visit the nodes where race happens.
        // FIXME: Ugly. It should be done by the BTreeWalker.
        let dmap = Arc::try_unwrap(deferred_map).unwrap().into_inner().unwrap();
        for (thin_id, dlist) in dmap {
            // actually we don't need the data sm here
            let data_sm = data_sm.clone();
            let v = BottomLevelVisitor::new(data_sm);
            let mut total = 0;

            for (b, mut path) in dlist {
                // FIXME: check errors
                let _ = w.walk(&mut path, &v, b);
                total += 1;
            }

            ctx.report.info(&format!(
                "thin device {} has {} deferred blocks",
                thin_id, total
            ));
            let cnt = v.mapped_blocks.load(Ordering::SeqCst);
            mapped_blocks
                .lock()
                .unwrap()
                .entry(thin_id)
                .and_modify(|c| *c += cnt)
                .or_insert(cnt);
        }

        let errs = Arc::try_unwrap(errs).unwrap().into_inner().unwrap();
        if !errs.is_empty() {
            failed = true;
        }
    } else {
        for (thin_id, (path, root)) in roots.iter() {
            let thin_id = *thin_id;
            let w = w.clone();
            let data_sm = data_sm.clone();
            let root = *root;
            let v = Arc::new(BottomLevelVisitor::new(data_sm));
            let mut path = path.clone();
            let mapped = mapped_blocks.clone();

            if walk_threaded(&mut path, w, &ctx.pool, v.clone(), root).is_err() {
                failed = true;
            }

            let cnt = v.mapped_blocks.load(Ordering::SeqCst);
            mapped.lock().unwrap().insert(thin_id, cnt);
        }
    }

    let mapped_blocks = mapped_blocks.lock().unwrap();
    for (dev_id, details) in devs {
        let cnt = mapped_blocks.get(dev_id).cloned().unwrap_or(0);
        if cnt != details.mapped_blocks {
            ctx.report.fatal(&format!(
                "Device {} has unexpected number of mapped blocks: expected {}, actual {}.",
                dev_id, details.mapped_blocks, cnt
            ));
            failed = true;
        }
    }

    if failed {
        Err(anyhow!("Check of mappings failed"))
    } else {
        Ok(())
    }
}

fn read_sb(opts: &ThinCheckOptions, engine: Arc<dyn IoEngine + Sync + Send>) -> Result<Superblock> {
    // superblock
    let sb = if opts.engine_opts.use_metadata_snap {
        read_superblock_snap(engine.as_ref())?
    } else {
        read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?
    };
    Ok(sb)
}

fn mk_context_(engine: Arc<dyn IoEngine + Send + Sync>, report: Arc<Report>) -> Result<Context> {
    let nr_threads = engine.suggest_nr_threads();
    let pool = ThreadPool::new(nr_threads);

    Ok(Context {
        report,
        engine,
        pool,
    })
}

fn mk_context(opts: &ThinCheckOptions) -> Result<Context> {
    let engine = EngineBuilder::new(opts.input, &opts.engine_opts)
        .write(opts.auto_repair || opts.clear_needs_check)
        .build()?;
    mk_context_(engine, opts.report.clone())
}

fn print_info(sb: &Superblock, report: Arc<Report>) -> Result<()> {
    let root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    report.to_stdout(&format!("TRANSACTION_ID={}", sb.transaction_id));
    report.to_stdout(&format!(
        "METADATA_FREE_BLOCKS={}",
        root.nr_blocks - root.nr_allocated
    ));
    Ok(())
}

pub fn check(opts: ThinCheckOptions) -> Result<()> {
    let ctx = mk_context(&opts)?;

    // FIXME: temporarily get these out
    let report = &ctx.report;
    let engine = &ctx.engine;

    let mut sb = read_sb(&opts, engine.clone())?;
    let _ = print_info(&sb, report.clone());

    report.set_title("Checking thin metadata");

    sb.mapping_root = opts.override_mapping_root.unwrap_or(sb.mapping_root);

    if opts.sb_only {
        if opts.clear_needs_check {
            let cleared = clear_needs_check_flag(ctx.engine.clone())?;
            if cleared {
                ctx.report.info("Cleared needs_check flag");
            }
        }
        return Ok(());
    }

    let metadata_root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    let mut path = vec![0];

    // Device details.   We read this once to get the number of thin devices, and hence the
    // maximum metadata ref count.  Then create metadata space map, and reread to increment
    // the ref counts for that metadata.
    let devs = btree_to_map::<DeviceDetail>(
        &mut path,
        engine.clone(),
        opts.ignore_non_fatal,
        sb.details_root,
    )?;
    let nr_devs = devs.len();
    let metadata_sm = core_sm(engine.get_nr_blocks(), nr_devs as u32);
    inc_superblock(&metadata_sm)?;

    report.set_sub_title("device details tree");
    let _devs = btree_to_map_with_sm::<DeviceDetail>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        opts.ignore_non_fatal,
        sb.details_root,
    )?;

    let mon_sm = metadata_sm.clone();
    let monitor = ProgressMonitor::new(report.clone(), metadata_root.nr_allocated, move || {
        mon_sm.lock().unwrap().get_nr_allocated().unwrap()
    });

    // mapping top level
    report.set_sub_title("mapping top level");
    let roots = btree_to_map_with_path::<u64>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        opts.ignore_non_fatal,
        sb.mapping_root,
    )?;

    if opts.skip_mappings {
        let cleared = clear_needs_check_flag(ctx.engine.clone())?;
        if cleared {
            ctx.report.info("Cleared needs_check flag");
        }
        return Ok(());
    }

    // mapping bottom level
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let data_sm = core_sm(root.nr_blocks, nr_devs as u32);
    check_mapping_bottom_level(
        &ctx,
        &metadata_sm,
        &data_sm,
        &roots,
        &devs,
        opts.ignore_non_fatal,
    )?;

    // trees in metadata snap
    if sb.metadata_snap > 0 {
        {
            let mut metadata_sm = metadata_sm.lock().unwrap();
            metadata_sm.inc(sb.metadata_snap, 1)?;
        }
        let sb_snap = read_superblock(engine.as_ref(), sb.metadata_snap)?;

        // device details
        let devs_snap = btree_to_map_with_sm::<DeviceDetail>(
            &mut path,
            engine.clone(),
            metadata_sm.clone(),
            opts.ignore_non_fatal,
            sb_snap.details_root,
        )?;

        // mapping top level
        let roots_snap = btree_to_map_with_path::<u64>(
            &mut path,
            engine.clone(),
            metadata_sm.clone(),
            opts.ignore_non_fatal,
            sb_snap.mapping_root,
        )?;

        // mapping bottom level
        check_mapping_bottom_level(
            &ctx,
            &metadata_sm,
            &data_sm,
            &roots_snap,
            &devs_snap,
            opts.ignore_non_fatal,
        )?;
    }

    //-----------------------------------------

    report.set_sub_title("data space map");
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let data_leaks = check_disk_space_map(
        engine.clone(),
        report.clone(),
        root,
        data_sm.clone(),
        metadata_sm.clone(),
        opts.ignore_non_fatal,
    )?;

    //-----------------------------------------

    report.set_sub_title("metadata space map");
    let root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;

    // Now the counts should be correct and we can check it.
    let metadata_leaks = check_metadata_space_map(
        engine.clone(),
        report.clone(),
        root,
        metadata_sm.clone(),
        opts.ignore_non_fatal,
    )?;

    //-----------------------------------------

    if (opts.auto_repair || opts.clear_needs_check)
        && (opts.engine_opts.use_metadata_snap || opts.override_mapping_root.is_some())
    {
        return Err(anyhow!("cannot perform repair outside the actual metadata"));
    }

    if !data_leaks.is_empty() {
        if opts.auto_repair {
            ctx.report.info("Repairing data leaks.");
            repair_space_map(ctx.engine.clone(), data_leaks, data_sm.clone())?;
        } else if !opts.ignore_non_fatal {
            return Err(anyhow!("data space map contains leaks"));
        }
    }

    if !metadata_leaks.is_empty() {
        if opts.auto_repair {
            ctx.report.info("Repairing metadata leaks.");
            repair_space_map(ctx.engine.clone(), metadata_leaks, metadata_sm.clone())?;
        } else if !opts.ignore_non_fatal {
            return Err(anyhow!("metadata space map contains leaks"));
        }
    }

    if opts.auto_repair || opts.clear_needs_check {
        let cleared = clear_needs_check_flag(ctx.engine.clone())?;
        if cleared {
            ctx.report.info("Cleared needs_check flag");
        }
    }

    monitor.stop();

    Ok(())
}

pub fn clear_needs_check_flag(engine: Arc<dyn IoEngine + Send + Sync>) -> Result<bool> {
    let mut sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    if !sb.flags.needs_check {
        return Ok(false);
    }
    sb.flags.needs_check = false;
    write_superblock(engine.as_ref(), SUPERBLOCK_LOCATION, &sb).map(|_| true)
}

//------------------------------------------

// Some callers wish to know which blocks are allocated.
pub struct CheckMaps {
    pub metadata_sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    pub data_sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
}

pub fn check_with_maps(
    engine: Arc<dyn IoEngine + Send + Sync>,
    report: Arc<Report>,
) -> Result<CheckMaps> {
    let ctx = mk_context_(engine.clone(), report.clone())?;
    report.set_title("Checking thin metadata");

    // superblock
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;

    let metadata_root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    let mut path = vec![0];

    // Device details.   We read this once to get the number of thin devices, and hence the
    // maximum metadata ref count.  Then create metadata space map, and reread to increment
    // the ref counts for that metadata.
    let devs = btree_to_map::<DeviceDetail>(&mut path, engine.clone(), false, sb.details_root)?;
    let nr_devs = devs.len();
    let metadata_sm = core_sm(engine.get_nr_blocks(), nr_devs as u32);
    inc_superblock(&metadata_sm)?;

    report.set_sub_title("device details tree");
    let _devs = btree_to_map_with_sm::<DeviceDetail>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        false,
        sb.details_root,
    )?;

    let mon_sm = metadata_sm.clone();
    let monitor = ProgressMonitor::new(report.clone(), metadata_root.nr_allocated, move || {
        mon_sm.lock().unwrap().get_nr_allocated().unwrap()
    });

    // mapping top level
    report.set_sub_title("mapping tree");
    let roots = btree_to_map_with_path::<u64>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        false,
        sb.mapping_root,
    )?;

    // mapping bottom level
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let data_sm = core_sm(root.nr_blocks, nr_devs as u32);
    check_mapping_bottom_level(&ctx, &metadata_sm, &data_sm, &roots, &devs, false)?;

    //-----------------------------------------

    report.set_sub_title("data space map");
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let _data_leaks = check_disk_space_map(
        engine.clone(),
        report.clone(),
        root,
        data_sm.clone(),
        metadata_sm.clone(),
        false,
    )?;

    //-----------------------------------------

    report.set_sub_title("metadata space map");
    let root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;

    // Now the counts should be correct and we can check it.
    let _metadata_leaks =
        check_metadata_space_map(engine.clone(), report, root, metadata_sm.clone(), false)?;

    //-----------------------------------------

    monitor.stop();

    Ok(CheckMaps {
        metadata_sm: metadata_sm.clone(),
        data_sm: data_sm.clone(),
    })
}

//------------------------------------------
