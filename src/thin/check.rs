use anyhow::{anyhow, Result};
use fixedbitset::FixedBitSet;
use std::collections::{BTreeMap, BTreeSet, HashMap};
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

// TODO: store the keys
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
struct LeafNodeInfo {
    keys: KeyRange,
    nr_entries: u64,
}

#[derive(PartialEq)]
enum NodeType {
    None,
    Internal,
    Leaf,
    Error,
}

#[derive(Debug)]
struct NodeMap {
    node_type: FixedBitSet,
    leaf_nodes: FixedBitSet,
    nr_leaves: u32,
    internal_info: Vec<InternalNodeInfo>,
    internal_map: HashMap<u32, u32>,
    leaf_info: Vec<LeafNodeInfo>,
    leaf_map: HashMap<u32, u32>,
    node_errors: Vec<BTreeError>, // TODO: use NodeError instead
    error_map: HashMap<u32, u32>,
}

impl NodeMap {
    fn new(nr_blocks: u32) -> NodeMap {
        NodeMap {
            node_type: FixedBitSet::with_capacity((nr_blocks as usize) * 2),
            leaf_nodes: FixedBitSet::with_capacity(nr_blocks as usize),
            nr_leaves: 0,
            internal_info: Vec::new(),
            internal_map: HashMap::new(),
            leaf_info: Vec::new(),
            leaf_map: HashMap::new(),
            node_errors: Vec::new(),
            error_map: HashMap::new(),
        }
    }

    fn get_type(&self, blocknr: u32) -> NodeType {
        // FIXME: query two bits at once
        let lsb = self.node_type.contains(blocknr as usize * 2);
        let msb = self.node_type.contains(blocknr as usize * 2 + 1);
        if !lsb && msb {
            NodeType::Leaf
        } else if lsb && !msb {
            NodeType::Internal
        } else if lsb && msb {
            NodeType::Error
        } else {
            NodeType::None
        }
    }

    fn set_node_type(&mut self, blocknr: u32, t: NodeType) {
        // FIXME: update two bits at once
        match t {
            NodeType::Leaf => {
                self.node_type.insert(blocknr as usize * 2 + 1);
                self.leaf_nodes.insert(blocknr as usize);
                self.nr_leaves += 1;
            }
            NodeType::Internal => {
                self.node_type.insert(blocknr as usize * 2);
            }
            NodeType::Error => {
                self.node_type.insert(blocknr as usize * 2 + 1);
                self.node_type.insert(blocknr as usize * 2);
            }
            _ => {}
        }
    }

    fn update_leaf_node(&mut self, blocknr: u32, info: LeafNodeInfo) -> Result<()> {
        if self.get_type(blocknr) != NodeType::Leaf {
            return Err(anyhow!("type changed"));
        }
        self.leaf_map.insert(blocknr, self.leaf_info.len() as u32);
        self.leaf_info.push(info);
        Ok(())
    }

    fn insert_internal_node(&mut self, blocknr: u32, info: InternalNodeInfo) -> Result<()> {
        if self.get_type(blocknr) != NodeType::None {
            return Err(anyhow!("type changed"));
        }
        self.internal_map
            .insert(blocknr, self.internal_info.len() as u32);
        self.internal_info.push(info);
        self.set_node_type(blocknr, NodeType::Internal);
        Ok(())
    }

    fn insert_error(&mut self, blocknr: u32, e: BTreeError) -> Result<()> {
        if self.get_type(blocknr) != NodeType::None {
            return Err(anyhow!("type changed"));
        }
        self.error_map
            .insert(blocknr, self.node_errors.len() as u32);
        self.node_errors.push(e);
        self.set_node_type(blocknr, NodeType::Error);
        Ok(())
    }
}

// FIXME: add context to errors

fn verify_checksum(b: &Block) -> btree::Result<()> {
    let bt = checksum::metadata_block_type(b.get_data());
    if bt != checksum::BT::NODE {
        return Err(BTreeError::NodeError(String::from(
            "corrupt block: checksum failed",
        )));
    }
    Ok(())
}

fn is_seen(loc: u32, metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>) -> bool {
    let mut sm = metadata_sm.lock().unwrap();
    let old = sm.inc_one(loc as u64).expect("space map inc failed");
    old > 0
}

// FIXME: split up this function. e.g., return the internal node,
// and let the caller decide what to do.
fn read_node_(
    ctx: &Context,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    b: &Block,
    depth: usize,
    kr: &KeyRange,
    ignore_non_fatal: bool,
    is_root: bool,
    nodes: &mut NodeMap,
) -> btree::Result<InternalNodeInfo> {
    verify_checksum(&b)?;

    // Allow underfull nodes in the first pass
    // FIXME: use proper path, actually can we recreate the path from the node info?
    let path = Vec::new();
    let node = unpack_node::<u64>(&path, b.get_data(), ignore_non_fatal, true)?;

    use btree::Node::*;
    if let Internal { keys, values, .. } = node {
        let children = values.iter().map(|v| *v as u32).collect::<Vec<u32>>(); // FIXME: slow
        let child_keys = split_key_ranges(&path, kr, &keys)?;

        // filter out previously visited nodes
        let mut new_values = Vec::with_capacity(values.len());
        let mut new_keys = Vec::with_capacity(child_keys.len());
        for i in 0..values.len() {
            if !is_seen(values[i] as u32, metadata_sm) {
                new_values.push(values[i].clone());
                new_keys.push(child_keys[i].clone());
            }
        }
        let values = new_values;
        let child_keys = new_keys;

        if depth == 0 {
            // FIXME: do this by the caller?
            for loc in values {
                nodes.set_node_type(loc as u32, NodeType::Leaf);
            }
        } else {
            // we could error each child rather than the current node
            match ctx.engine.read_many(&values) {
                Ok(bs) => {
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
                            nodes.insert_error(values[i] as u32, BTreeError::IoError);
                        }
                    }
                }
                Err(_) => {
                    // error every children
                    for loc in values {
                        nodes.insert_error(loc as u32, BTreeError::IoError);
                    }
                }
            };
        }

        // FIXME: is it necessary to return the node info in postfix fashion?
        //        e.g., gather information from the children or something else
        // TODO: store all the child keys for further validation
        let info = InternalNodeInfo {
            keys: kr.clone(),
            error: None,
            children_are_leaves: depth == 0,
            children,
            nr_entries: 0,
        };

        Ok(info)
    } else {
        return Err(BTreeError::NodeError(String::from(
            "btree nodes are not all at the same depth.",
        )));
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
            nodes.insert_error(block_nr, e);
        }
        Ok(n) => {
            nodes.insert_internal_node(block_nr, n);
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
    if is_seen(root, metadata_sm) {
        return;
    }

    let mut path = Vec::new();
    // FIXME: make get-depth more resilient
    let depth = get_depth(ctx, &mut path, root as u64, true).expect("get_depth failed");

    if depth == 0 {
        nodes.set_node_type(root as u32, NodeType::Leaf);
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
        // FIXME: factor out common code
        nodes.insert_error(root, BTreeError::IoError);
    }
}

fn visit_node(b: u32, nodes: &mut NodeMap) -> u64 {
    match nodes.get_type(b) {
        NodeType::Internal => {
            if let Some(i) = nodes.internal_map.get(&b).cloned() {
                let info = &nodes.internal_info[i as usize];
                // FIXME: use another flag to indicate that the node had been visited
                if info.nr_entries > 0 {
                    info.nr_entries
                } else {
                    let children = info.children.clone();
                    let mut nr_entries = 0;
                    for b in children {
                        nr_entries += visit_node(b, nodes);
                    }

                    nodes.internal_info[i as usize].nr_entries = nr_entries;
                    nr_entries
                }
            } else {
                0
            }
        }
        NodeType::Leaf => {
            if let Some(i) = nodes.leaf_map.get(&b).cloned() {
                nodes.leaf_info[i as usize].nr_entries
            } else {
                0
            }
        }
        _ => 0,
    }
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

    let mut nodes = NodeMap::new(ctx.engine.get_nr_blocks() as u32);

    let mut tree_roots = BTreeSet::new();

    let start = std::time::Instant::now();
    for (thin_id, (_path, root)) in roots {
        tree_roots.insert(*root);
        read_internal_nodes(ctx, metadata_sm, *root as u32, ignore_non_fatal, &mut nodes);
    }
    let duration = start.elapsed();
    eprintln!("read_internal_nodes: {:?}", duration);
    eprintln!("nr internal nodes: {}", nodes.internal_info.len());
    eprintln!("nr leaves: {}", nodes.nr_leaves);
    eprintln!("nr errors: {}", nodes.node_errors.len());

    // Build a vec of the leaf locations.  These will be in disk location
    // order.
    let start = std::time::Instant::now();
    let mut leaves = Vec::with_capacity(nodes.nr_leaves as usize);
    for loc in nodes.leaf_nodes.ones() {
        leaves.push(loc as u64);
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
                            match nodes.get_type(*loc as u32) {
                                NodeType::Leaf => {
                                    // FIXME: copy the min & max key instead
                                    let info = LeafNodeInfo {
                                        keys: KeyRange {start: keys.first().cloned(), end: keys.last().cloned()},
                                        nr_entries: keys.len() as u64,
                                    };
                                    nodes.update_leaf_node(*loc as u32, info);
                                }
                                _ => {
                                    panic!("unexpected node type");
                                }
                            }
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
        // TODO: move into get_nr_mappings() or get_summary()
        let mapped = match nodes.get_type(*root as u32) {
            NodeType::Internal => {
                if let Some(i) = nodes.internal_map.get(&(*root as u32)) {
                    nodes.internal_info[*i as usize].nr_entries
                } else {
                    0
                }
            },
            NodeType::Leaf => {
                if let Some(i) = nodes.leaf_map.get(&(*root as u32)) {
                    nodes.leaf_info[*i as usize].nr_entries
                } else {
                    0
                }
            },
            _ => 0,
        };

        if mapped != details.mapped_blocks {
            eprintln!("Thin device {} has unexpected number of mapped block, expected {}, actual {}", thin_id,
                    details.mapped_blocks, mapped);
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
    eprintln!(
        "size of LeafNodeInfo: {:?}",
        std::mem::size_of::<LeafNodeInfo>()
    );

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
    let data_sm = core_sm(root.nr_blocks, nr_devs as u32); // consumes a lot of mem
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
