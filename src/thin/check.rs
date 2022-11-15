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

#[derive(Debug, Clone)]
struct InternalNodeInfo {
    keys: Vec<u64>,
    children: Vec<u32>,
    children_are_leaves: bool,
}

#[derive(Debug, Clone, Default)]
struct NodeSummary {
    key_low: u64, // min mapped block
    key_high: u64, // max mapped block, inclusive
    nr_entries: u64,
}

impl NodeSummary {
    fn append(&mut self, other: &NodeSummary) -> anyhow::Result<()> {
        if other.nr_entries > 0 {
            if self.nr_entries == 0 {
                *self = other.clone();
            } else {
                if other.key_low <= self.key_high {
                    return Err(anyhow!("overlapped keys"));
                }
                self.key_high = other.key_high;
                self.nr_entries += other.nr_entries;
            }
        }

        Ok(())
    }
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

    // Stores errors found in _this_ node only; children errors not included
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

    fn set_type_(&mut self, blocknr: u32, t: NodeType) {
        match t {
            NodeType::Leaf => {
                // FIXME: update two bits at once
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

    fn insert_internal_node(&mut self, blocknr: u32, info: InternalNodeInfo) -> Result<()> {
        if self.get_type(blocknr) != NodeType::None {
            return Err(anyhow!("type changed"));
        }
        self.internal_map
            .insert(blocknr, self.internal_info.len() as u32);
        self.internal_info.push(info);
        self.set_type_(blocknr, NodeType::Internal);
        Ok(())
    }

    fn insert_leaf(&mut self, blocknr: u32) -> Result<()> {
        if self.get_type(blocknr) != NodeType::None {
            return Err(anyhow!("type changed"));
        }
        self.set_type_(blocknr, NodeType::Leaf);
        Ok(())
    }

    fn insert_error(&mut self, blocknr: u32, e: BTreeError) -> Result<()> {
        if self.get_type(blocknr) != NodeType::None {
            return Err(anyhow!("type changed"));
        }
        self.error_map
            .insert(blocknr, self.node_errors.len() as u32);
        self.node_errors.push(e);
        self.set_type_(blocknr, NodeType::Error);
        Ok(())
    }

    // returns total number of registered nodes
    fn len(&self) -> u32 {
        return self.internal_info.len() as u32 + self.nr_leaves
    }
}

#[derive(Debug)]
struct SummaryMap {
    summaries: Vec<NodeSummary>,
    node_map: HashMap<u32, u32>,
}

impl SummaryMap {
    fn new(capacity: u32) -> Self {
        Self {
            summaries: Vec::with_capacity(capacity as usize),
            node_map: HashMap::with_capacity(capacity as usize),
        }
    }

    fn push(&mut self, blocknr: u32, summary: NodeSummary) -> Result<()> {
        if self.node_map.contains_key(&blocknr) {
            return Err(anyhow!("already summarized"));
        }
        self.node_map.insert(blocknr, self.summaries.len() as u32);
        self.summaries.push(summary);
        Ok(())
    }

    fn get(&self, blocknr: u32) -> Option<&NodeSummary> {
        if let Some(i) = self.node_map.get(&blocknr) {
            Some(&self.summaries[*i as usize])
        } else {
            None
        }
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
    ignore_non_fatal: bool,
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

        // filter out previously visited nodes
        let mut new_values = Vec::with_capacity(values.len());
        for i in 0..values.len() {
            if !is_seen(values[i] as u32, metadata_sm) {
                new_values.push(values[i].clone());
            }
        }
        let values = new_values;

        if depth == 0 {
            // FIXME: do this by the caller?
            for loc in values {
                nodes.insert_leaf(loc as u32);
            }
        } else {
            // we could error each child rather than the current node
            match ctx.engine.read_many(&values) {
                Ok(bs) => {
                    for (i, b) in bs.iter().enumerate() {
                        if let Ok(b) = b {
                            read_node(
                                ctx,
                                metadata_sm,
                                &b,
                                depth - 1,
                                ignore_non_fatal,
                                nodes,
                            );
                        } else {
                            nodes.insert_error(values[i] as u32, BTreeError::IoError);
                        }
                    }
                }
                Err(_) => {
                    // error every block
                    for loc in values {
                        nodes.insert_error(loc as u32, BTreeError::IoError);
                    }
                }
            };
        }

        // FIXME: Is it necessary to return the node info in postfix fashion?
        //        Returning nodes in postfix ordering doesn't help gathering
        //        information from the children since we haven't visit the
        //        leaves yet. However, it could help the caller to check
        //        whether the internal node is good or not, e.g., there's any
        //        error in unpack_node or split_key_range
        let info = InternalNodeInfo {
            keys: keys.clone(),
            children_are_leaves: depth == 0,
            children,
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
    ignore_non_fatal: bool,
    nodes: &mut NodeMap,
) {
    let block_nr = b.loc as u32;
    match read_node_(
        ctx,
        metadata_sm,
        b,
        depth,
        ignore_non_fatal,
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
    if is_seen(root, metadata_sm) {
        return;
    }

    let mut path = Vec::new();
    // FIXME: make get-depth more resilient
    let depth = get_depth(ctx, &mut path, root as u64, true).expect("get_depth failed");

    if depth == 0 {
        nodes.insert_leaf(root as u32);
        return;
    }

    if let Ok(b) = ctx.engine.read(root as u64) {
        read_node(
            ctx,
            metadata_sm,
            &b,
            depth - 1,
            ignore_non_fatal,
            nodes,
        );
    } else {
        // FIXME: factor out common code
        nodes.insert_error(root, BTreeError::IoError);
    }
}

// TODO: check underfull?
fn visit_node(path: &mut Vec<u64>, kr: &KeyRange, b: u32, nodes: &NodeMap, summaries: &mut SummaryMap) -> NodeSummary {
    if let Some(sum) = summaries.get(b) {
        if sum.nr_entries > 0 {
            if let Some(n) = kr.start {
                // the parent key could be less than or equal to,
                // but not greater than the child's first key
                if n > sum.key_low {
                    return NodeSummary::default();
                }
            }
            if let Some(n) = kr.end {
                // note that KeyRange is a right-opened interval
                if n < sum.key_high {
                    return NodeSummary::default();
                }
            }
        }

        return sum.clone();
    }

    match nodes.get_type(b) {
        NodeType::Internal => {
            if let Some(i) = nodes.internal_map.get(&b).cloned() {
                let info = &nodes.internal_info[i as usize];

                // gather information from the children
                let child_keys = split_key_ranges(path, kr, &info.keys).unwrap();

                let mut sum = NodeSummary::default();
                for (i, b) in info.children.iter().enumerate() {
                    path.push(*b as u64);
                    let child_info = visit_node(path, &child_keys[i], *b, nodes, summaries);
                    sum.append(&child_info);
                    path.pop();
                }

                summaries.push(b, sum.clone());

                sum
            } else {
                // missing info
                NodeSummary::default()
            }
        }
        _ => {
            NodeSummary::default()
        }
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
    let nodes = Arc::new(nodes);
    let summaries = Arc::new(Mutex::new(SummaryMap::new(nodes.len())));
    let mut chunk_start = 0;
    let tree_roots = Arc::new(tree_roots);
    while chunk_start < leaves.len() {
        let len = std::cmp::min(1024, leaves.len() - chunk_start);
        let engine = ctx.engine.clone();
        let data_sm = data_sm.clone();
        let leaves = leaves.clone();
        let tree_roots = tree_roots.clone();
        let nodes = nodes.clone();
        let summaries = summaries.clone();

        ctx.pool.execute(move || {
            let c = &leaves[chunk_start..(chunk_start + len)];
            //std::thread::sleep(std::time::Duration::from_secs(30));

            let blocks = engine.read_many(c).expect("lazy");

            for (loc, b) in c.iter().zip(blocks) {
                let b = b.expect("lazy");
                verify_checksum(&b).expect("lazy programmer");

                // allow underfull nodes at this stage
                let mut path = Vec::new();
                let node =
                    unpack_node::<BlockTime>(&mut path, b.get_data(), ignore_non_fatal, true)
                        .expect("lazy");
                match node {
                    Node::Leaf { keys, values, .. } => {
                        {
                            let mut data_sm = data_sm.lock().unwrap();
                            for v in values {
                                data_sm.inc(v.block, 1).expect("data_sm.inc() failed");
                            }
                        }

                        {
                            let nr_entries = keys.len();
                            let key_low = if nr_entries > 0 {keys[0]} else {0};
                            let key_high = if nr_entries > 0 {keys[nr_entries - 1]} else {0};
                            let sum = NodeSummary {
                                key_low,
                                key_high,
                                nr_entries: nr_entries as u64,
                            };
                            let mut sums = summaries.lock().unwrap();
                            sums.push(*loc as u32, sum);
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
    let nodes = Arc::try_unwrap(nodes).unwrap();
    let mut summaries = Arc::try_unwrap(summaries).unwrap().into_inner().unwrap();
    let tree_roots = Arc::try_unwrap(tree_roots).unwrap();
    for root in tree_roots.iter() {
        let mut path = vec![0, *root]; // FIXME: use actual path from the top-level tree
        let kr = KeyRange::new();
        visit_node(&mut path, &kr, *root as u32, &nodes, &mut summaries);
    }
    let duration = start.elapsed();
    eprintln!("counting mapped blocks: {:?}", duration);

    let start = std::time::Instant::now();
    println!("{}", roots.len());
    println!("{}", devs.len());
    for ((thin_id, (_, root)), details) in roots.into_iter().zip(devs.values()) {
        //eprintln!("dev {} root {} details {:?}", thin_id, root, details.mapped_blocks);
        let mapped = if let Some(sum) = summaries.get(*root as u32) {
            sum.nr_entries
        } else {
            0
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
        "size of NodeSummary: {:?}",
        std::mem::size_of::<NodeSummary>()
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
        eprintln!("roots_snap {:?}", roots_snap);
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
