use anyhow::{anyhow, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use threadpool::ThreadPool;

use crate::checksum;
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
}

//------------------------------------------

impl NodeVisitor<BlockTime> for BottomLevelVisitor {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        _k: &[u64],
        values: &[BlockTime],
    ) -> btree::Result<()> {
        // FIXME: do other checks

        if values.is_empty() {
            return Ok(());
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

        data_sm.inc(start, len).unwrap();
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

fn inc_superblock(sm: &ASpaceMap) -> Result<()> {
    let mut sm = sm.lock().unwrap();
    sm.inc(SUPERBLOCK_LOCATION, 1)?;
    Ok(())
}

//------------------------------------------

pub const MAX_CONCURRENT_IO: u32 = 1024;

pub struct ThinCheckOptions<'a> {
    pub input: &'a Path,
    pub engine_opts: EngineOptions,
    pub sb_only: bool,
    pub skip_mappings: bool,
    pub ignore_non_fatal: bool,
    pub auto_repair: bool,
    pub clear_needs_check: bool,
    pub report: Arc<Report>,
}

fn spawn_progress_thread(
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    nr_allocated_metadata: u64,
    report: Arc<Report>,
) -> Result<(JoinHandle<()>, Arc<AtomicBool>)> {
    let tid;
    let stop_progress = Arc::new(AtomicBool::new(false));

    {
        let stop_progress = stop_progress.clone();
        tid = thread::spawn(move || {
            let interval = std::time::Duration::from_millis(250);
            loop {
                if stop_progress.load(Ordering::Relaxed) {
                    break;
                }

                let sm = sm.lock().unwrap();
                let mut n = sm.get_nr_allocated().unwrap();
                drop(sm);

                n *= 100;
                n /= nr_allocated_metadata;

                let _r = report.progress(n as u8);
                thread::sleep(interval);
            }
        });
    }

    Ok((tid, stop_progress))
}

struct Context {
    report: Arc<Report>,
    engine: Arc<dyn IoEngine + Send + Sync>,
    pool: ThreadPool,
}

//----------------------------------------

// BTree nodes can get scattered across the metadata device.  Which can
// result in a lot of seeks on spindle devices if we walk the trees in
// depth first order.  To get around this we walk the upper levels of
// the btrees to build a list of the leaf nodes.  Then process the leaf
// nodes in location order.

// We know the metadata area is limited to 16G, so u32 is large enough
// hold block numbers.

#[derive(Debug, Clone)]
struct InternalNodeInfo {
    keys: KeyRange,

    // Errors found in _this_ node only; children may have errors
    error: Option<btree::BTreeError>,
    children_are_leaves: bool,
    children: Vec<u32>,
    nr_entries: u64,
}

#[derive(Debug, Clone)]
enum NodeInfo {
    Internal(InternalNodeInfo),
    Leaf { keys: KeyRange, nr_entries: u64 },
}

/// block_nr -> node info
type NodeMap = BTreeMap<u32, NodeInfo>;

// FIXME: add context to errors

fn verify_checksum(b: &Block) -> Result<()> {
    let bt = checksum::metadata_block_type(b.get_data());
    if bt != checksum::BT::NODE {
        return Err(anyhow!("corrupt block: checksum failed"));
    }
    Ok(())
}

fn is_seen(
    loc: u32,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    nodes: &mut NodeMap,
) -> bool {
    let mut sm = metadata_sm.lock().unwrap();
    sm.inc(loc as u64, 1).expect("space map inc failed");
    sm.get(loc as u64).unwrap_or(0) > 1 // nodes.contains_key(&loc)
}

fn read_node_(
    ctx: &Context,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    b: &Block,
    depth: usize,
    kr: &KeyRange,
    ignore_non_fatal: bool,
    is_root: bool,
    nodes: &mut NodeMap,
) -> Result<NodeInfo> {
    verify_checksum(&b)?;

    // FIXME: use proper path, actually can we recreate the path from the node info?
    let path = Vec::new();
    let node = unpack_node::<u64>(&path, b.get_data(), ignore_non_fatal, is_root)?;

    use btree::Node::*;
    if let Internal { keys, values, .. } = node {
        let children = values.iter().map(|v| *v as u32).collect::<Vec<u32>>();
        let child_keys = split_key_ranges(&path, kr, &keys)?;

        // filter out previously visited nodes
        let mut new_values = Vec::with_capacity(values.len());
        let mut new_keys = Vec::with_capacity(child_keys.len());
        for i in 0..values.len() {
            if !is_seen(values[i] as u32, metadata_sm, nodes) {
                new_values.push(values[i].clone());
                new_keys.push(child_keys[i].clone());
            }
        }
        let values = new_values;
        let child_keys = new_keys;

        if depth == 0 {
            let mut sm = metadata_sm.lock().unwrap();
            for (loc, kr) in values.iter().zip(child_keys) {
                nodes.insert(
                    *loc as u32,
                    NodeInfo::Leaf {
                        keys: kr.clone(),
                        nr_entries: 0,
                    },
                );
            }
        } else {
            // we could error each child rather than the current node
            let bs = ctx.engine.read_many(&values)?;

            for (i, (b, kr)) in bs.iter().zip(child_keys).enumerate() {
                if let Ok(b) = b {
                    read_node(
                        ctx,
                        metadata_sm,
                        &b,
                        depth - 1,
                        &kr,
                        ignore_non_fatal,
                        false,
                        nodes,
                    );
                } else {
                    // FIXME: we don't have to store the node content if it is broke
                    let info = InternalNodeInfo {
                        keys: kr.clone(),
                        error: Some(BTreeError::IoError),
                        children_are_leaves: depth == 0,
                        children: Vec::new(),
                        nr_entries: 0,
                    };
                    nodes.insert(values[i] as u32, NodeInfo::Internal(info));
                }
            }
        }

        let info = InternalNodeInfo {
            keys: kr.clone(),
            error: None,
            children_are_leaves: depth == 0,
            children,
            nr_entries: 0,
        };

        Ok(NodeInfo::Internal(info))
    } else {
        return Err(anyhow!("btree nodes are not all at the same depth."));
    }
}

/// Reads a btree node and all internal btree nodes below it into the
/// nodes parameter.  No errors are returned, instead the optional
/// error field of the nodes will be filled in.
fn read_node(
    ctx: &Context,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    b: &Block,
    depth: usize,
    keys: &KeyRange,
    ignore_non_fatal: bool,
    is_root: bool,
    nodes: &mut NodeMap,
) {
    let block_nr = b.loc as u32;
    match read_node_(
        ctx,
        metadata_sm,
        b,
        depth,
        keys,
        ignore_non_fatal,
        is_root,
        nodes,
    ) {
        Err(e) => {
            let info = InternalNodeInfo {
                keys: keys.clone(),
                error: Some(BTreeError::IoError), // FIXME: gather the error from the children
                children_are_leaves: depth == 0,
                children: Vec::new(),
                nr_entries: 0,
            };
            nodes.insert(block_nr, NodeInfo::Internal(info));
        }
        Ok(n) => {
            nodes.insert(block_nr, n);
        }
    }
}

/// Gets the depth of a bottom level mapping tree.  0 means the root is a leaf node.
// FIXME: what if there's an error on the path to the leftmost leaf?
fn get_depth(ctx: &Context, path: &mut Vec<u64>, root: u64, is_root: bool) -> Result<usize> {
    use Node::*;

    let b = ctx.engine.read(root).map_err(|_| io_err(path))?;
    verify_checksum(&b)?;

    let node = unpack_node::<BlockTime>(path, b.get_data(), true, is_root)?;

    match node {
        Internal { values, .. } => {
            let n = get_depth(ctx, path, values[0], false)?;
            Ok(n + 1)
        }
        Leaf { .. } => Ok(0),
    }
}

fn read_internal_nodes(
    ctx: &Context,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    root: u32,
    ignore_non_fatal: bool,
    nodes: &mut NodeMap,
) {
    let keys = KeyRange::new();
    if is_seen(root, metadata_sm, nodes) {
        return;
    }

    let mut path = Vec::new();
    // FIXME: make get-depth more resilient
    let depth = get_depth(ctx, &mut path, root as u64, true).expect("get_depth failed");

    if depth == 0 {
        nodes.insert(
            root as u32,
            NodeInfo::Leaf {
                keys: KeyRange::new(),
                nr_entries: 0,
            },
        );
        return;
    }

    if let Ok(b) = ctx.engine.read(root as u64) {
        read_node(
            ctx,
            metadata_sm,
            &b,
            depth - 1,
            &keys,
            ignore_non_fatal,
            true,
            nodes,
        );
    } else {
        let info = InternalNodeInfo {
            keys: keys.clone(),
            error: Some(BTreeError::IoError),
            children_are_leaves: depth == 0,
            children: Vec::new(),
            nr_entries: 0,
        };
        // FIXME: factor out common code
        nodes.insert(root, NodeInfo::Internal(info));
    }
}

// FIXME: we cannot do this if info is a borrow from nodes
fn visit_children(info: &mut InternalNodeInfo, nodes: &mut NodeMap) -> u64 {
    for b in info.children.iter() {
        info.nr_entries += visit_node(*b, nodes);
    }
    info.nr_entries
}

fn visit_node(b: u32, nodes: &mut NodeMap) -> u64 {
    let nr_entries = match nodes.get(&b).cloned() {
        Some(NodeInfo::Internal(info)) => {
            if info.nr_entries > 0 {
                info.nr_entries
            } else {
                let mut nr_entries = 0;
                for b in info.children.iter() {
                    nr_entries += visit_node(*b, nodes);
                }
                nr_entries
            }
        }
        Some(NodeInfo::Leaf { nr_entries, .. }) => nr_entries,
        _ => 0,
    };

    // FIXME: avoid separated query & update
    if let Some(NodeInfo::Internal(info)) = nodes.get_mut(&b) {
        info.nr_entries = nr_entries;
    }

    nr_entries
}

// Check the mappings filling in the data_sm as we go.
fn check_mapping_bottom_level(
    ctx: &Context,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    data_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    roots: &BTreeMap<u64, (Vec<u64>, u64)>,
    ignore_non_fatal: bool,
    devs: &BTreeMap<u64, DeviceDetail>,
) -> Result<()> {
    ctx.report.set_sub_title("mapping tree");

    let mut nodes = BTreeMap::new();
    let mut tree_roots = BTreeSet::new();

    let start = std::time::Instant::now();
    let mut last = 0;
    for (thin_id, (_path, root)) in roots {
        tree_roots.insert(*root);
        read_internal_nodes(ctx, metadata_sm, *root as u32, ignore_non_fatal, &mut nodes);
        last = nodes.len();
    }
    let duration = start.elapsed();
    eprintln!("read_internal_nodes: {:?}", duration);

    // Build a vec of the leaf locations.  These will be in disk location
    // order.
    // FIXME: use with_capacity
    let start = std::time::Instant::now();
    let mut leaves = Vec::new();
    for (loc, info) in nodes.iter() {
        match info {
            NodeInfo::Leaf { .. } => {
                leaves.push(*loc as u64);
            }
            _ => {
                // do nothing
            }
        }
    }
    let duration = start.elapsed();
    eprintln!("collecting leaf nodes blocknr: {:?}", duration);

    let start = std::time::Instant::now();
    //std::thread::sleep(std::time::Duration::from_secs(30));
    // Process chunks of leaves at once so the io engine can aggregate reads.
    let leaves = Arc::new(leaves);
    let nodes = Arc::new(Mutex::new(nodes));
    let mut chunk_start = 0;
    let tree_roots = Arc::new(tree_roots);
    while chunk_start < leaves.len() {
        let len = std::cmp::min(1024, leaves.len() - chunk_start);
        let engine = ctx.engine.clone();
        let data_sm = data_sm.clone();
        let leaves = leaves.clone();
        let tree_roots = tree_roots.clone();
        let nodes = nodes.clone();

        ctx.pool.execute(move || {
            let c = &leaves[chunk_start..(chunk_start + len)];
            //std::thread::sleep(std::time::Duration::from_secs(30));

            let blocks = engine.read_many(c).expect("lazy");

            for (loc, b) in c.iter().zip(blocks) {
                let b = b.expect("lazy");
                verify_checksum(&b).expect("lazy programmer");
                let is_root = tree_roots.contains(loc);

                let mut path = Vec::new();
                let node =
                    unpack_node::<BlockTime>(&mut path, b.get_data(), ignore_non_fatal, is_root)
                        .expect("lazy");
                match node {
                    Node::Leaf { keys, values, .. } => {
                        // FIXME: check keys are within range
                        {
                            let mut data_sm = data_sm.lock().unwrap();
                            for v in values {
                                data_sm.inc(v.block, 1).expect("data_sm.inc() failed");
                            }
                        }

                        {
                            let mut nodes = nodes.lock().unwrap();
                            nodes.entry(*loc as u32).and_modify(|info| {
                                if let NodeInfo::Leaf {
                                    keys: ref mut k,
                                    ref mut nr_entries,
                                } = info
                                {
                                    k.start = keys.first().cloned();
                                    k.end = keys.last().cloned();
                                    *nr_entries = keys.len() as u64;
                                } else {
                                    panic!("unexpected node type");
                                }
                            });
                        }
                    }
                    _ => {
                        panic!("node changed it's type under me");
                    }
                }
            }
        });
        chunk_start += len;
    }
    ctx.pool.join();
    let duration = start.elapsed();
    eprintln!("reading leaf nodes: {:?}", duration);

    let start = std::time::Instant::now();
    // stage2: DFS traverse subtree to gather subtree information (single threaded)
    let mut nodes = Arc::try_unwrap(nodes).unwrap().into_inner().unwrap();
    let tree_roots = Arc::try_unwrap(tree_roots).unwrap();
    for root in tree_roots.iter() {
        visit_node(*root as u32, &mut nodes);
    }
    let duration = start.elapsed();
    eprintln!("counting mapped blocks: {:?}", duration);

    let start = std::time::Instant::now();
    for ((thin_id, (_, root)), details) in roots.into_iter().zip(devs.values()) {
        match nodes.get(&(*root as u32)) {
            Some(NodeInfo::Internal(info)) => {
                if info.nr_entries != details.mapped_blocks {
                    eprintln!("Thin device {} has unexpected number of mapped block, expected {}, actual {}", thin_id,
                        details.mapped_blocks, info.nr_entries);
                }
            }
            Some(NodeInfo::Leaf { nr_entries, .. }) => {
                if *nr_entries != details.mapped_blocks {
                    eprintln!("Thin device {} has unexpected number of mapped block, expected {}, actual {}", thin_id,
                        details.mapped_blocks, nr_entries);
                }
            }
            _ => {
                eprintln!("error");
            }
        }
    }
    let duration = start.elapsed();
    eprintln!("checking mapped blocks: {:?}", duration);

    /*if failed {
        Err(anyhow!("Check of mappings failed"))
    } else {
        Ok(())
    }
    */
    Ok(())
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
    // let nr_threads = engine.suggest_nr_threads();
    let nr_threads = 16;
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

pub fn check(opts: ThinCheckOptions) -> Result<()> {
    eprintln!(
        "size of InternalNodeInfo: {:?}",
        std::mem::size_of::<InternalNodeInfo>()
    );
    eprintln!("size of NodeInfo: {:?}", std::mem::size_of::<NodeInfo>());

    let ctx = mk_context(&opts)?;

    // FIXME: temporarily get these out
    let report = &ctx.report;
    let engine = &ctx.engine;

    report.set_title("Checking thin metadata");

    let sb = read_sb(&opts, engine.clone())?;
    report.to_stdout(&format!("TRANSACTION_ID={}", sb.transaction_id));

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

    let (tid, stop_progress) = spawn_progress_thread(
        metadata_sm.clone(),
        metadata_root.nr_allocated,
        report.clone(),
    )?;

    // mapping top level
    report.set_sub_title("mapping tree");
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
        opts.ignore_non_fatal,
        &devs,
    )?;

    // trees in metadata snap
    if sb.metadata_snap > 0 {
        {
            let mut metadata_sm = metadata_sm.lock().unwrap();
            metadata_sm.inc(sb.metadata_snap, 1)?;
        }
        let sb_snap = read_superblock(engine.as_ref(), sb.metadata_snap)?;

        // device details
        btree_to_map_with_sm::<DeviceDetail>(
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
            opts.ignore_non_fatal,
            &devs,
        )?;
    }

    //-----------------------------------------

    report.set_sub_title("data space map");
    let start = std::time::Instant::now();
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let data_leaks = check_disk_space_map(
        engine.clone(),
        report.clone(),
        root,
        data_sm.clone(),
        metadata_sm.clone(),
        opts.ignore_non_fatal,
    )?;
    let duration = start.elapsed();
    eprintln!("checking data space map: {:?}", duration);

    //-----------------------------------------

    report.set_sub_title("metadata space map");
    let start = std::time::Instant::now();
    let root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    report.to_stdout(&format!(
        "METADATA_FREE_BLOCKS={}",
        root.nr_blocks - root.nr_allocated
    ));

    // Now the counts should be correct and we can check it.
    let metadata_leaks = check_metadata_space_map(
        engine.clone(),
        report.clone(),
        root,
        metadata_sm.clone(),
        opts.ignore_non_fatal,
    )?;
    let duration = start.elapsed();
    eprintln!("checking metadata space map: {:?}", duration);

    //-----------------------------------------

    if !opts.auto_repair && !opts.ignore_non_fatal {
        if !data_leaks.is_empty() {
            return Err(anyhow!("data space map contains leaks"));
        }

        if !metadata_leaks.is_empty() {
            return Err(anyhow!("metadata space map contains leaks"));
        }
    }

    // TODO: check override-mapping-root
    if !opts.engine_opts.use_metadata_snap {
        if !data_leaks.is_empty() && opts.auto_repair {
            ctx.report.info("Repairing data leaks.");
            repair_space_map(ctx.engine.clone(), data_leaks, data_sm.clone())?;
        }

        if !metadata_leaks.is_empty() && opts.auto_repair {
            ctx.report.info("Repairing metadata leaks.");
            repair_space_map(ctx.engine.clone(), metadata_leaks, metadata_sm.clone())?;
        }

        if opts.auto_repair || opts.clear_needs_check {
            let cleared = clear_needs_check_flag(ctx.engine.clone())?;
            if cleared {
                ctx.report.info("Cleared needs_check flag");
            }
        }
    }

    stop_progress.store(true, Ordering::Relaxed);
    tid.join().unwrap();

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

    report.info(&format!("TRANSACTION_ID={}", sb.transaction_id));

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

    let (tid, stop_progress) = spawn_progress_thread(
        metadata_sm.clone(),
        metadata_root.nr_allocated,
        report.clone(),
    )?;

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
    check_mapping_bottom_level(&ctx, &metadata_sm, &data_sm, &roots, false, &devs)?;

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
    report.info(&format!(
        "METADATA_FREE_BLOCKS={}",
        root.nr_blocks - root.nr_allocated
    ));

    // Now the counts should be correct and we can check it.
    let _metadata_leaks =
        check_metadata_space_map(engine.clone(), report, root, metadata_sm.clone(), false)?;

    //-----------------------------------------

    stop_progress.store(true, Ordering::Relaxed);
    tid.join().unwrap();

    Ok(CheckMaps {
        metadata_sm: metadata_sm.clone(),
        data_sm: data_sm.clone(),
    })
}

//------------------------------------------
