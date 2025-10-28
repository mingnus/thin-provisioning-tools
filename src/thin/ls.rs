use anyhow::{anyhow, Result};
use fixedbitset::FixedBitSet;
use rand::seq::SliceRandom;
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::str::FromStr;
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::commands::engine::*;
use crate::grid_layout::GridLayout;
use crate::io_engine::SECTOR_SHIFT;
use crate::io_engine::*;
use crate::pdata::btree::*;
use crate::pdata::btree_utils::*;
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::aggregator::*;
use crate::pdata::space_map::common::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;
use crate::report::{ProgressMonitor, Report};
use crate::thin::block_time::BlockTime;
use crate::thin::device_detail::DeviceDetail;
use crate::thin::metadata_repair::is_superblock_consistent;
use crate::thin::superblock::*;
use crate::units::*;
use crate::utils::prof::*;
use crate::utils::ranged_bitset_iter::*;

//------------------------------------------

// minimum number of entries of a node with 64-bit mapped type
const MIN_ENTRIES: u8 = 84;

//------------------------------------------

#[derive(Clone)]
pub enum OutputField {
    DeviceId,

    MappedBlocks,
    ExclusiveBlocks,
    SharedBlocks,
    HighestMappedBlock,

    MappedSectors,
    ExclusiveSectors,
    SharedSectors,
    HighestMappedSector,

    MappedBytes,
    ExclusiveBytes,
    SharedBytes,
    HighestMappedByte,

    Mapped,
    Exclusive,
    Shared,
    HighestMapped,

    TransactionId,
    CreationTime,
    SnapshottedTime,
}

impl FromStr for OutputField {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use OutputField::*;

        match s {
            "DEV" => Ok(DeviceId),
            "MAPPED_BLOCKS" => Ok(MappedBlocks),
            "EXCLUSIVE_BLOCKS" => Ok(ExclusiveBlocks),
            "SHARED_BLOCKS" => Ok(SharedBlocks),
            "HIGHEST_BLOCK" => Ok(HighestMappedBlock),

            "MAPPED_SECTORS" => Ok(MappedSectors),
            "EXCLUSIVE_SECTORS" => Ok(ExclusiveSectors),
            "SHARED_SECTORS" => Ok(SharedSectors),
            "HIGHEST_SECTOR" => Ok(HighestMappedSector),

            "MAPPED_BYTES" => Ok(MappedBytes),
            "EXCLUSIVE_BYTES" => Ok(ExclusiveBytes),
            "SHARED_BYTES" => Ok(SharedBytes),
            "HIGHEST_BYTE" => Ok(HighestMappedByte),

            "MAPPED" => Ok(Mapped),
            "EXCLUSIVE" => Ok(Exclusive),
            "SHARED" => Ok(Shared),
            "HIGHEST_MAPPED" => Ok(HighestMapped),

            "TRANSACTION" => Ok(TransactionId),
            "CREATE_TIME" => Ok(CreationTime),
            "SNAP_TIME" => Ok(SnapshottedTime),

            _ => Err(anyhow!("Unknown field")),
        }
    }
}

impl std::fmt::Display for OutputField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use OutputField::*;

        let text = match self {
            DeviceId => "DEV",
            MappedBlocks => "MAPPED_BLOCKS",
            ExclusiveBlocks => "EXCLUSIVE_BLOCKS",
            SharedBlocks => "SHARED_BLOCKS",
            HighestMappedBlock => "HIGHEST_BLOCK",

            MappedSectors => "MAPPED_SECTORS",
            ExclusiveSectors => "EXCLUSIVE_SECTORS",
            SharedSectors => "SHARED_SECTORS",
            HighestMappedSector => "HIGHEST_SECTOR",

            MappedBytes => "MAPPED_BYTES",
            ExclusiveBytes => "EXCLUSIVE_BYTES",
            SharedBytes => "SHARED_BYTES",
            HighestMappedByte => "HIGHEST_BYTE",

            Mapped => "MAPPED",
            Exclusive => "EXCLUSIVE",
            Shared => "SHARED",
            HighestMapped => "HIGHEST_MAPPED",

            TransactionId => "TRANSACTION",
            CreationTime => "CREATE_TIME",
            SnapshottedTime => "SNAP_TIME",
        };

        write!(f, "{}", text)
    }
}

//------------------------------------------

pub struct LsTable<'a> {
    fields: &'a [OutputField],
    grid: GridLayout,
    data_block_size: u64,
}

impl LsTable<'_> {
    fn new(fields: &[OutputField], nr_rows: usize, bs: u32) -> LsTable<'_> {
        let grid = GridLayout::new_with_size(nr_rows, fields.len());

        LsTable {
            fields,
            grid,
            data_block_size: bs as u64,
        }
    }

    fn push_headers(&mut self) {
        if self.fields.is_empty() {
            return;
        }

        for i in self.fields {
            self.grid.field(i.to_string());
        }
        self.grid.new_row();
    }

    fn push_row(
        &mut self,
        dev_id: u64,
        detail: &DeviceDetail,
        mapped_blocks: u64,
        shared_blocks: u64,
        highest_mapped_block: u64,
    ) {
        use OutputField::*;

        if self.fields.is_empty() {
            return;
        }

        let bs = self.data_block_size;
        let ex_blocks = mapped_blocks - shared_blocks;

        for field in self.fields {
            let val: u64 = match field {
                DeviceId => dev_id,
                TransactionId => detail.transaction_id,
                CreationTime => detail.creation_time as u64,
                SnapshottedTime => detail.snapshotted_time as u64,
                MappedBlocks => mapped_blocks,
                MappedSectors => mapped_blocks * bs,
                MappedBytes | Mapped => (mapped_blocks * bs) << SECTOR_SHIFT as u64,
                ExclusiveBlocks => ex_blocks,
                ExclusiveSectors => ex_blocks * bs,
                ExclusiveBytes | Exclusive => (ex_blocks * bs) << SECTOR_SHIFT as u64,
                SharedBlocks => shared_blocks,
                SharedSectors => shared_blocks * bs,
                SharedBytes | Shared => (shared_blocks * bs) << SECTOR_SHIFT as u64,
                HighestMappedBlock => highest_mapped_block,
                HighestMappedSector => (highest_mapped_block + 1) * bs - 1,
                HighestMappedByte | HighestMapped => {
                    (((highest_mapped_block + 1) * bs) << SECTOR_SHIFT) - 1
                }
            };

            let cell = match field {
                Mapped | Exclusive | Shared | HighestMapped => {
                    let (val, unit) = to_pretty_print_size(val);
                    let mut s = val.to_string();
                    s.push_str(&unit.to_string_short());
                    s
                }
                _ => val.to_string(),
            };

            self.grid.field(cell);
        }
        self.grid.new_row();
    }

    // grid
    pub fn render(&self, w: &mut dyn Write) -> Result<()> {
        self.grid.render(w)
    }
}

//------------------------------------------

#[derive(Debug, Clone)]
struct InternalNodeInfo {
    keys: Vec<u64>,
    children: Vec<u32>,
}

#[derive(Debug, Clone, Default)]
struct NodeSummary {
    key_low: u64,     // min mapped block
    key_high: u64,    // max mapped block, inclusive
    nr_mappings: u64, // number of valid mappings in this subtree
    nr_shared: u64,   // number of shared mappings in this subtree
    nr_entries: u8,   // number of entries in this node
    nr_errors: u8,    // number of errors found in this subtree, up to 255
}

impl NodeSummary {
    fn from_leaf(keys: &[u64], nr_shared: u64) -> Self {
        let nr_entries = keys.len();
        let key_low = if nr_entries > 0 { keys[0] } else { 0 };
        let key_high = if nr_entries > 0 {
            keys[nr_entries - 1]
        } else {
            0
        };

        NodeSummary {
            key_low,
            key_high,
            nr_mappings: nr_entries as u64,
            nr_shared,
            nr_entries: nr_entries as u8,
            nr_errors: 0,
        }
    }

    fn error() -> Self {
        Self {
            key_low: 0,
            key_high: 0,
            nr_mappings: 0,
            nr_shared: 0,
            nr_entries: 0,
            nr_errors: 1,
        }
    }

    fn append(&mut self, child: &NodeSummary) -> anyhow::Result<()> {
        if self.nr_mappings == 0 {
            self.key_low = child.key_low;
            self.key_high = child.key_high;
        } else if child.nr_mappings > 0 {
            if child.key_low <= self.key_high {
                return Err(anyhow!("overlapped keys"));
            }
            self.key_high = child.key_high;
        }
        self.nr_mappings += child.nr_mappings;
        self.nr_shared += child.nr_shared;
        self.nr_entries += 1;
        self.nr_errors = self.nr_errors.saturating_add(child.nr_errors);

        Ok(())
    }
}

//------------------------------------------

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
    leaf_nodes: FixedBitSet, // FIXME: remove this one
    nr_leaves: u32,
    internal_info: HashMap<u32, InternalNodeInfo>,

    // Stores errors of the node itself; errors in children are not included
    node_errors: HashMap<u32, NodeError>,
}

impl NodeMap {
    fn new(nr_blocks: u32) -> NodeMap {
        NodeMap {
            node_type: FixedBitSet::with_capacity((nr_blocks as usize) * 2),
            leaf_nodes: FixedBitSet::with_capacity(nr_blocks as usize),
            nr_leaves: 0,
            internal_info: HashMap::new(),
            node_errors: HashMap::new(),
        }
    }

    fn get_type(&self, blocknr: u32) -> NodeType {
        // FIXME: query two bits at once
        let lsb = self.node_type.contains(blocknr as usize * 2);
        let msb = self.node_type.contains(blocknr as usize * 2 + 1);
        if !lsb && msb {
            NodeType::Error
        } else if lsb && !msb {
            NodeType::Leaf
        } else if lsb && msb {
            NodeType::Internal
        } else {
            NodeType::None
        }
    }

    fn set_type_(&mut self, blocknr: u32, t: NodeType) {
        match t {
            NodeType::Leaf => {
                self.node_type.insert(blocknr as usize * 2);
                self.leaf_nodes.insert(blocknr as usize);
                self.nr_leaves += 1;
            }
            NodeType::Internal => {
                // FIXME: update two bits at once
                self.node_type.insert(blocknr as usize * 2);
                self.node_type.insert(blocknr as usize * 2 + 1);
                if self.leaf_nodes.contains(blocknr as usize) {
                    self.leaf_nodes.toggle(blocknr as usize);
                    self.nr_leaves -= 1;
                }
            }
            NodeType::Error => {
                // FIXME: update two bits at once
                self.node_type.insert(blocknr as usize * 2 + 1);
                if self.leaf_nodes.contains(blocknr as usize) {
                    self.node_type.toggle(blocknr as usize * 2);
                    self.leaf_nodes.toggle(blocknr as usize);
                    self.nr_leaves -= 1;
                }
            }
            _ => {}
        }
    }

    fn insert_internal_node(&mut self, blocknr: u32, info: InternalNodeInfo) -> Result<()> {
        // Only accepts converting a potential unread leaf
        let node_type = self.get_type(blocknr);
        if node_type != NodeType::None && node_type != NodeType::Leaf {
            return Err(anyhow!("type changed"));
        }
        self.internal_info.insert(blocknr, info);
        self.set_type_(blocknr, NodeType::Internal);
        Ok(())
    }

    fn insert_leaf(&mut self, blocknr: u32) -> Result<()> {
        // Only accepts an unread block
        if self.get_type(blocknr) != NodeType::None {
            return Err(anyhow!("type changed"));
        }
        self.set_type_(blocknr, NodeType::Leaf);
        Ok(())
    }

    fn insert_error(&mut self, blocknr: u32, e: NodeError) -> Result<()> {
        // Only accepts converting a potential unread leaf
        let node_type = self.get_type(blocknr);
        if node_type != NodeType::None && node_type != NodeType::Leaf {
            return Err(anyhow!("type changed"));
        }
        self.node_errors.insert(blocknr, e);
        self.set_type_(blocknr, NodeType::Error);
        Ok(())
    }

    // Returns total number of nodes found
    fn _len(&self) -> u32 {
        self.internal_info.len() as u32 + self.nr_leaves
    }
}

//------------------------------------------

struct NodeUpdate {
    loc: u32,
    info: NodeInfo,
}

enum NodeInfo {
    Leaf(),
    Internal(InternalNodeInfo),
    Error(NodeError),
}

const NODE_MAP_BATCH_SIZE: usize = 1024;

struct BatchedNodeMap {
    inner: Mutex<NodeMap>,
}

impl BatchedNodeMap {
    fn new(inner: NodeMap) -> Self {
        Self {
            inner: Mutex::new(inner),
        }
    }

    fn batch_update(&self, updates: Vec<NodeUpdate>) {
        let mut guard = self.inner.lock().unwrap();
        for update in updates {
            match update.info {
                NodeInfo::Leaf() => {
                    let _ = guard.insert_leaf(update.loc);
                }
                NodeInfo::Internal(info) => {
                    let _ = guard.insert_internal_node(update.loc, info);
                }
                NodeInfo::Error(error) => {
                    let _ = guard.insert_error(update.loc, error);
                }
            }
        }
    }
}

//--------------------------------

struct LayerHandler<'a> {
    is_root: bool,
    metadata_sm: &'a Aggregator,
    ignore_non_fatal: bool,
    nodes: Arc<BatchedNodeMap>,
    children: FixedBitSet,
    updates: Vec<NodeUpdate>,
}

impl<'a> LayerHandler<'a> {
    fn new(
        is_root: bool,
        metadata_sm: &'a Aggregator,
        ignore_non_fatal: bool,
        nodes: Arc<BatchedNodeMap>,
    ) -> Self {
        Self {
            is_root,
            metadata_sm,
            ignore_non_fatal,
            nodes,
            children: FixedBitSet::with_capacity(metadata_sm.get_nr_blocks()),
            updates: Vec::new(),
        }
    }

    fn flush_updates(&mut self) {
        if !self.updates.is_empty() {
            self.nodes.batch_update(std::mem::take(&mut self.updates));
        }
    }

    fn maybe_flush(&mut self) {
        if self.updates.len() >= NODE_MAP_BATCH_SIZE {
            self.flush_updates();
        }
    }

    fn push_error(&mut self, loc: u32, e: NodeError) {
        self.updates.push(NodeUpdate {
            loc,
            info: NodeInfo::Error(e),
        });
    }

    fn push_internal(&mut self, loc: u32, info: InternalNodeInfo) {
        self.updates.push(NodeUpdate {
            loc,
            info: NodeInfo::Internal(info),
        });
    }

    fn get_children(self) -> FixedBitSet {
        self.children
    }
}

impl<'a> ReadHandler for LayerHandler<'a> {
    fn handle(&mut self, loc: u64, data: std::io::Result<&[u8]>) {
        self.maybe_flush();

        match data {
            Ok(data) => {
                if let Err(e) = verify_checksum(data) {
                    self.push_error(loc as u32, e);
                    return;
                }

                let node = unpack_node_raw::<u64>(data, self.ignore_non_fatal, self.is_root);

                if let Err(e) = &node {
                    self.push_error(loc as u32, *e);
                    return;
                }

                let node = node.unwrap();
                if node.get_header().block != loc {
                    self.push_error(loc as u32, NodeError::BlockNrMismatch);
                    return;
                }

                if let Node::Internal { keys, values, .. } = node {
                    // insert the node info in pre-order fashion to better detect loops in the path
                    let children = values.iter().map(|v| *v as u32).collect::<Vec<u32>>();
                    let info = InternalNodeInfo { keys, children };
                    self.push_internal(loc as u32, info);

                    let seen = self.metadata_sm.test_and_inc(&values);
                    let nr_blocks = self.metadata_sm.get_nr_blocks() as u64;

                    for (i, v) in values.iter().enumerate() {
                        if !seen.contains(i) && *v < nr_blocks {
                            self.children.insert(*v as usize);
                        }
                    }
                }
            }
            Err(_) => {
                self.push_error(loc as u32, NodeError::IoError);
            }
        }
    }

    fn complete(&mut self) {
        self.flush_updates();
    }
}

//------------------------------------------

// Verify the checksum of a node
fn verify_checksum(data: &[u8]) -> std::result::Result<(), NodeError> {
    use crate::checksum;
    match checksum::metadata_block_type(data) {
        checksum::BT::NODE => Ok(()),
        checksum::BT::UNKNOWN => Err(NodeError::ChecksumError),
        _ => Err(NodeError::NotANode),
    }
}

#[inline(always)]
fn convert_result<V>(r: nom::IResult<&[u8], V>) -> std::result::Result<(&[u8], V), NodeError> {
    r.map_err(|_e| NodeError::IncompleteData)
}

fn examine_leaf_(
    loc: u64,
    data: &[u8],
    ignore_non_fatal: bool,
    data_sm_size: u64,
    data_blocks: &mut Vec<u64>,
) -> std::result::Result<NodeSummary, NodeError> {
    use nom::{bytes::complete::take, number::complete::*};

    verify_checksum(data)?;

    let (i, header) = NodeHeader::unpack(data).map_err(|_e| NodeError::IncompleteData)?;

    if header.is_leaf && header.value_size != BlockTime::disk_size() {
        return Err(NodeError::ValueSizeMismatch);
    }

    let elt_size = header.value_size + 8;
    if elt_size as usize * header.max_entries as usize + NODE_HEADER_SIZE > BLOCK_SIZE {
        return Err(NodeError::MaxEntriesTooLarge);
    }

    if header.block != loc {
        return Err(NodeError::BlockNrMismatch);
    }

    if header.nr_entries > header.max_entries {
        return Err(NodeError::NumEntriesTooLarge);
    }

    if !ignore_non_fatal && header.max_entries % 3 != 0 {
        return Err(NodeError::MaxEntriesNotDivisible);
    }

    let mut key_low = 0;
    let mut key_high = 0;
    let mut input = i;
    if header.nr_entries > 0 {
        let (i, k) = convert_result(le_u64(input))?;
        input = i;
        key_low = k;
        key_high = k;

        let mut last = k;
        for idx in 1..header.nr_entries {
            let (i, k) = convert_result(le_u64(input))?;
            input = i;

            if k < last {
                return Err(NodeError::KeysOutOfOrder);
            }
            last = k;

            if idx == header.nr_entries - 1 {
                key_high = k;
            }
        }
    }
    let i = input;

    let nr_free = header.max_entries - header.nr_entries;
    let (i, _padding) = convert_result(take(nr_free * 8)(i))?;

    let mut input = i;
    let mut error_mappings = 0;
    for _ in 0..header.nr_entries {
        let (i, bt) = convert_result(BlockTime::unpack(input))?;
        input = i;
        if bt.block >= data_sm_size {
            error_mappings += 1;
            continue;
        }
        data_blocks.push(bt.block);
    }

    let sum = NodeSummary {
        key_low,
        key_high,
        nr_mappings: header.nr_entries as u64 - error_mappings as u64,
        nr_shared: 0, // Will be calculated later
        nr_entries: header.nr_entries as u8 - error_mappings,
        nr_errors: if error_mappings > 0 { 1 } else { 0 },
    };

    Ok(sum)
}

struct LeafHandler {
    data_sm: Arc<Aggregator>,
    nodes: Arc<BatchedNodeMap>,
    summaries: Arc<Mutex<HashMap<u32, NodeSummary>>>,
    ignore_non_fatal: bool,
    inc_batch: Vec<u64>,
    summary_batch: Vec<(u32, NodeSummary)>,
    updates: Vec<NodeUpdate>,
}

const INC_BATCH_SIZE: usize = 1024;
const SUMMARY_BATCH_SIZE: usize = 1024;

impl LeafHandler {
    fn new(
        data_sm: Arc<Aggregator>,
        nodes: Arc<BatchedNodeMap>,
        summaries: Arc<Mutex<HashMap<u32, NodeSummary>>>,
        ignore_non_fatal: bool,
    ) -> Self {
        Self {
            data_sm,
            nodes,
            summaries,
            ignore_non_fatal,
            inc_batch: Vec::with_capacity(INC_BATCH_SIZE + 256),
            summary_batch: Vec::with_capacity(SUMMARY_BATCH_SIZE + 256),
            updates: Vec::new(),
        }
    }

    fn flush_incs(&mut self) {
        if self.inc_batch.is_empty() {
            return;
        }
        self.data_sm.increment(&self.inc_batch);
        self.inc_batch.clear();
    }

    fn maybe_flush_incs(&mut self) {
        if self.inc_batch.len() >= INC_BATCH_SIZE {
            self.flush_incs();
        }
    }

    fn flush_summaries(&mut self) {
        if self.summary_batch.is_empty() {
            return;
        }

        let mut summaries = self.summaries.lock().unwrap();
        for (loc, sum) in &self.summary_batch {
            summaries.insert(*loc, sum.clone());
        }
        self.summary_batch.clear();
    }

    fn maybe_flush_summaries(&mut self) {
        if self.summary_batch.len() >= SUMMARY_BATCH_SIZE {
            self.flush_summaries();
        }
    }

    fn flush_updates(&mut self) {
        if !self.updates.is_empty() {
            self.nodes.batch_update(std::mem::take(&mut self.updates));
        }
    }

    fn maybe_flush_updates(&mut self) {
        if self.updates.len() >= NODE_MAP_BATCH_SIZE {
            self.flush_updates();
        }
    }

    fn push_error(&mut self, loc: u32, e: NodeError) {
        self.updates.push(NodeUpdate {
            loc,
            info: NodeInfo::Error(e),
        });
    }
}

impl ReadHandler for LeafHandler {
    fn handle(&mut self, loc: u64, data: std::io::Result<&[u8]>) {
        match data {
            Ok(data) => {
                // Allow under full nodes in this phase.  The under full
                // property will be check later based on the path context.
                let sum = examine_leaf_(
                    loc,
                    data,
                    self.ignore_non_fatal,
                    self.data_sm.get_nr_blocks() as u64,
                    &mut self.inc_batch,
                );
                self.maybe_flush_incs();

                match sum {
                    Ok(sum) => {
                        self.summary_batch.push((loc as u32, sum));
                        self.maybe_flush_summaries();
                    }
                    Err(e) => {
                        self.maybe_flush_updates();
                        self.push_error(loc as u32, e);
                    }
                }
            }
            Err(_e) => {
                self.maybe_flush_updates();
                self.push_error(loc as u32, NodeError::IoError);
            }
        }
    }

    fn complete(&mut self) {
        self.flush_incs();
        self.flush_summaries();
        self.flush_updates();
    }
}

fn unpacker(
    engine: Arc<dyn IoEngine>,
    leaves: &mut dyn Iterator<Item = u64>,
    data_sm: Arc<Aggregator>,
    nodes: Arc<BatchedNodeMap>,
    summaries: Arc<Mutex<HashMap<u32, NodeSummary>>>,
    ignore_non_fatal: bool,
) -> Result<()> {
    let io_block_size = 64 * 1024;
    let buffer_size = 16 * 1024 * 1024; // 16m
    let nr_io_blocks = buffer_size / io_block_size;
    let mut buffers = BufferPool::new(nr_io_blocks, io_block_size);

    let mut handler = LeafHandler::new(data_sm, nodes, summaries, ignore_non_fatal);
    engine.read_blocks(&mut buffers, leaves, &mut handler)?;

    Ok(())
}

fn read_internal_nodes(
    ctx: &Context,
    io_buffers: &mut BufferPool,
    metadata_sm: &Aggregator,
    root: u32,
    ignore_non_fatal: bool,
    nodes: Arc<BatchedNodeMap>,
) -> Result<()> {
    let nr_blocks = metadata_sm.get_nr_blocks();
    if root as usize >= nr_blocks {
        return Err(anyhow::anyhow!("block {} out of space map boundary", root));
    }

    let seen = metadata_sm.test_and_inc(&[root as u64]);
    if seen.contains(0) {
        return Ok(());
    }

    let depth = get_depth::<BlockTime>(ctx.engine.as_ref(), root as u64)?;
    if depth == 0 {
        nodes.batch_update(vec![NodeUpdate {
            loc: root,
            info: NodeInfo::Leaf(),
        }]);
        return Ok(());
    }

    let mut current_layer = FixedBitSet::with_capacity(nr_blocks);
    current_layer.insert(root as usize);

    // Read the internal nodes, layer by layer.
    let mut is_root = true;
    for _d in (0..depth).rev() {
        let mut handler = LayerHandler::new(is_root, metadata_sm, ignore_non_fatal, nodes.clone());
        is_root = false;

        ctx.engine.read_blocks(
            io_buffers,
            &mut current_layer.ones().map(|n| n as u64),
            &mut handler,
        )?;
        current_layer = handler.get_children();
    }

    // insert leaves
    let blocks = current_layer
        .ones()
        .map(|loc| NodeUpdate {
            loc: loc as u32,
            info: NodeInfo::Leaf(),
        })
        .collect();
    nodes.batch_update(blocks);

    Ok(())
}

fn collect_nodes_in_use(
    ctx: &Context,
    metadata_sm: &Aggregator,
    roots: &[u64],
    ignore_non_fatal: bool,
) -> Result<NodeMap> {
    const NR_THREADS: usize = 4;

    let nodes = NodeMap::new(metadata_sm.get_nr_blocks() as u32);

    if roots.is_empty() {
        return Ok(nodes);
    }

    let batch_nodes = Arc::new(BatchedNodeMap::new(nodes));

    let mut roots: Vec<u64> = roots.to_vec();
    roots.shuffle(&mut rand::rng());

    let chunk_size = roots.len().div_ceil(NR_THREADS);
    let root_chunks: Vec<&[u64]> = roots.chunks(chunk_size).collect();

    thread::scope(|s| {
        for chunk in root_chunks {
            let batch_nodes = batch_nodes.clone();
            s.spawn(move || {
                // FIXME: factor out
                let io_block_size = BLOCK_SIZE;
                let buffer_size = 16 * 1024 * 1024;
                let nr_io_blocks = buffer_size / io_block_size;
                let mut pool = BufferPool::new(nr_io_blocks, io_block_size);

                for &root in chunk {
                    // Allow errors in this phase. Node errors will be captured
                    // later when counting the number of mapped blocks.
                    let _ = read_internal_nodes(
                        ctx,
                        &mut pool,
                        metadata_sm,
                        root as u32,
                        ignore_non_fatal,
                        batch_nodes.clone(),
                    );
                }
            });
        }
    });

    let nodes = Arc::into_inner(batch_nodes)
        .unwrap()
        .inner
        .into_inner()
        .unwrap();

    Ok(nodes)
}

//------------------------------------------

// Summarize a subtree rooted at the speicifc block.
// Only a good internal node will have a summary stored.
// TODO: check the tree is balanced by comparing the height of visited nodes
#[allow(clippy::too_many_arguments)]
fn summarize_tree(
    path: &mut Vec<u64>,
    kr: &KeyRange,
    root: u32,
    is_root: bool,
    nodes: &NodeMap,
    metadata_sm: &Aggregator,
    summaries: &mut HashMap<u32, NodeSummary>,
    ignore_non_fatal: bool,
) -> NodeSummary {
    if let Some(sum) = summaries.get(&root) {
        // Check underfull
        if !ignore_non_fatal && !is_root && sum.nr_entries < MIN_ENTRIES {
            return NodeSummary::error();
        }

        // Check the key range against the parent keys.
        if sum.nr_mappings > 0 {
            if let Some(n) = kr.start {
                // The parent key could be less than or equal to,
                // but not greater than the child's first key
                if n > sum.key_low {
                    return NodeSummary::error();
                }
            }
            if let Some(n) = kr.end {
                // note that KeyRange is a right-opened interval
                if n < sum.key_high {
                    return NodeSummary::error();
                }
            }
        }

        return sum.clone();
    }

    match nodes.get_type(root) {
        NodeType::Internal => {
            if let Some(info) = nodes.internal_info.get(&root) {
                // Check underfull
                if !ignore_non_fatal && !is_root && info.keys.len() < MIN_ENTRIES as usize {
                    return NodeSummary::error();
                }

                // Split up the key range for the children.
                // Return immediately if the keys don't match.
                let child_keys = match split_key_ranges(path, kr, &info.keys) {
                    Ok(keys) => keys,
                    Err(_) => return NodeSummary::error(),
                };

                // Gather information from the children
                let mut sum = NodeSummary::default();
                for (i, b) in info.children.iter().enumerate() {
                    path.push(*b as u64);
                    let child_sums = summarize_tree(
                        path,
                        &child_keys[i],
                        *b,
                        false,
                        nodes,
                        metadata_sm,
                        summaries,
                        ignore_non_fatal,
                    );
                    let _ = sum.append(&child_sums);
                    path.pop();
                }

                // Adjust the number of shared blocks if it is a shared internal node
                // TODO: Need to implement shared block detection with Aggregator
                if metadata_sm.get(root as u64).unwrap_or(0) > 1 {
                    sum.nr_shared = sum.nr_mappings;
                }

                summaries.insert(root, sum.clone());

                sum
            } else {
                // This is unexpected. However, we would like to skip that kind of error.
                NodeSummary::error()
            }
        }

        // This is unexpected since a leaf should have been summarized
        _ => NodeSummary::error(),
    }
}

fn count_mapped_blocks(
    roots: &[u64],
    nodes: &NodeMap,
    metadata_sm: &Aggregator,
    summaries: &mut HashMap<u32, NodeSummary>,
    ignore_non_fatal: bool,
) {
    for root in roots.iter() {
        let mut path = vec![0, *root]; // the path is just for error reporting
        let kr = KeyRange::new();
        summarize_tree(
            &mut path,
            &kr,
            *root as u32,
            true,
            nodes,
            metadata_sm,
            summaries,
            ignore_non_fatal,
        );
    }
}

//------------------------------------------

fn read_leaf_nodes(
    ctx: &Context,
    nodes: NodeMap,
    data_sm: &Arc<Aggregator>,
    ignore_non_fatal: bool,
) -> Result<(NodeMap, HashMap<u32, NodeSummary>)> {
    const NR_UNPACKERS: usize = 4;

    let leaves = nodes.leaf_nodes.clone();
    let summaries = Arc::new(Mutex::new(HashMap::new()));
    let batch_nodes = Arc::new(BatchedNodeMap::new(nodes));

    // Kick off the unpackers
    thread::scope(|s| {
        let chunk_size = leaves.len().div_ceil(NR_UNPACKERS);
        for i in 0..NR_UNPACKERS {
            let l_begin = i * chunk_size;
            let l_end = ((i + 1) * chunk_size).min(leaves.len());
            let mut leaves = RangedBitsetIter::new(&leaves, l_begin..l_end);
            let batch_nodes = batch_nodes.clone();
            let data_sm = data_sm.clone();
            let summaries = summaries.clone();

            s.spawn(move || {
                unpacker(
                    ctx.engine.clone(),
                    &mut leaves,
                    data_sm,
                    batch_nodes,
                    summaries,
                    ignore_non_fatal,
                )
            });
        }
    });

    // extract the results
    let nodes = Arc::into_inner(batch_nodes)
        .unwrap()
        .inner
        .into_inner()
        .unwrap();
    let summaries = Arc::into_inner(summaries).unwrap().into_inner().unwrap();

    Ok((nodes, summaries))
}

fn exclusive_unpacker(
    blocks_rx: &Arc<Mutex<mpsc::Receiver<Vec<Block>>>>,
    nodes_tx: SyncSender<Vec<Node<BlockTime>>>,
    node_map: Arc<Mutex<NodeMap>>,
    ignore_non_fatal: bool,
) {
    loop {
        let blocks = {
            let blocks_rx = blocks_rx.lock().unwrap();
            if let Ok(blocks) = blocks_rx.recv() {
                blocks
            } else {
                break;
            }
        };

        let mut nodes = Vec::with_capacity(blocks.len());
        let mut errs = Vec::new();

        for b in blocks {
            // Allow under full nodes in this phase.  The under full
            // property will be check later based on the path context.
            match check_and_unpack_node::<BlockTime>(&b, ignore_non_fatal, true) {
                Ok(n) => {
                    nodes.push(n);
                }
                Err(e) => {
                    errs.push((b.loc, e));
                }
            }
        }

        if !errs.is_empty() {
            let mut node_map = node_map.lock().unwrap();
            for (b, e) in errs {
                // theoretically never fail
                let _ = node_map.insert_error(b as u32, e);
            }
        }

        if nodes_tx.send(nodes).is_err() {
            break;
        }
    }
}

fn exclusive_leaves_summariser(
    nodes_rx: mpsc::Receiver<Vec<Node<BlockTime>>>,
    metadata_sm: &Arc<Aggregator>,
    data_sm: &Arc<Aggregator>,
    summaries: &Arc<Mutex<HashMap<u32, NodeSummary>>>,
) {
    let mut summaries = summaries.lock().unwrap();

    loop {
        let nodes = {
            if let Ok(nodes) = nodes_rx.recv() {
                nodes
            } else {
                break;
            }
        };

        for n in nodes {
            if let Node::Leaf {
                keys,
                values,
                header,
            } = n
            {
                // TODO: Need to implement exclusive/shared detection with Aggregator
                if metadata_sm.get(header.block).unwrap_or(0) == 1 {
                    let mut nr_shared: u64 = 0;
                    for bt in values {
                        if data_sm.get(bt.block).unwrap_or(0) > 1 {
                            nr_shared += 1;
                        }
                    }

                    let sum = NodeSummary::from_leaf(&keys, nr_shared);
                    summaries.insert(header.block as u32, sum);
                }
            } else {
                // Do not report error here. The error will be captured
                // in the second phase.
            }
        }
    }
}

fn read_exclusive_leaves(
    ctx: &Context,
    nodes: NodeMap,
    metadata_sm: &Arc<Aggregator>,
    data_sm: &Arc<Aggregator>,
    summaries: HashMap<u32, NodeSummary>,
    ignore_non_fatal: bool,
) -> Result<(NodeMap, HashMap<u32, NodeSummary>)> {
    const QUEUE_DEPTH: usize = 4;
    const NR_UNPACKERS: usize = 4;

    // Single IO thread reads vecs of blocks
    // Many unpackers take the block vecs and turn them into btree nodes
    // Single 'summariser' thread processes the nodes

    // Build a vec of the leaf locations.  These will be in disk location
    // order.
    let mut leaves = Vec::with_capacity(nodes.nr_leaves as usize);
    {
        // TODO: Need to implement exclusive detection with Aggregator
        for loc in nodes.leaf_nodes.ones() {
            if metadata_sm.get(loc as u64).unwrap_or(0) == 1 {
                leaves.push(loc as u64);
            }
        }
    }

    let (blocks_tx, blocks_rx) = mpsc::sync_channel::<Vec<Block>>(QUEUE_DEPTH);
    let blocks_rx = Arc::new(Mutex::new(blocks_rx));

    let (nodes_tx, nodes_rx) = mpsc::sync_channel::<Vec<Node<BlockTime>>>(QUEUE_DEPTH);

    // Process chunks of leaves at once so the io engine can aggregate reads.
    let summaries = Arc::new(Mutex::new(summaries));
    let nodes = Arc::new(Mutex::new(nodes));

    // Kick off the unpackers
    let mut unpackers = Vec::with_capacity(NR_UNPACKERS);
    for _i in 0..NR_UNPACKERS {
        let blocks_rx = blocks_rx.clone();
        let nodes_tx = nodes_tx.clone();
        let node_map = nodes.clone();
        unpackers.push(thread::spawn(move || {
            exclusive_unpacker(&blocks_rx, nodes_tx, node_map, ignore_non_fatal)
        }));
    }
    drop(blocks_rx);
    drop(nodes_tx);

    // Kick off the summariser
    let summariser_tid = {
        let metadata_sm = metadata_sm.clone();
        let data_sm = data_sm.clone();
        let summaries = summaries.clone();
        thread::spawn(move || {
            exclusive_leaves_summariser(nodes_rx, &metadata_sm, &data_sm, &summaries);
        })
    };

    // IO is done in the main thread
    let engine = ctx.engine.clone();
    for c in leaves.chunks(1024) {
        let mut bs = Vec::with_capacity(c.len());

        // TODO: Retry blocks ignored by vectored io
        if let Ok(blocks) = engine.read_many(c) {
            for b in blocks {
                if b.is_err() {
                    continue;
                }

                let b = b.unwrap();
                bs.push(b);
            }

            blocks_tx
                .send(bs)
                .expect("couldn't send blocks to unpacker");
        } else {
            let mut nodes = nodes.lock().unwrap();
            for b in c {
                let _ = nodes.insert_error(*b as u32, NodeError::IoError);
            }
        }
    }

    drop(blocks_tx);

    // Wait for child threads
    for tid in unpackers {
        tid.join().expect("couldn't join unpacker");
    }
    summariser_tid.join().expect("couldn't join summariser");

    // extract the results
    let nodes = Arc::try_unwrap(nodes).unwrap().into_inner().unwrap();
    let summaries = Arc::try_unwrap(summaries).unwrap().into_inner().unwrap();

    Ok((nodes, summaries))
}

//------------------------------------------

fn count_data_mappings_(
    ctx: &Context,
    metadata_sm: &Arc<Aggregator>,
    data_sm: &Arc<Aggregator>,
    roots: &[u64],
    ignore_non_fatal: bool,
) -> Result<HashMap<u32, NodeSummary>> {
    let start = std::time::Instant::now();
    let nodes = collect_nodes_in_use(ctx, metadata_sm, roots, ignore_non_fatal)?;
    let duration = start.elapsed();
    ctx.report
        .debug(&format!("reading internal nodes: {:?}", duration));

    print_mem(&ctx.report, "memory usage before read_leaf_nodes");
    let start = std::time::Instant::now();
    let (nodes, summaries) = read_leaf_nodes(ctx, nodes, data_sm, ignore_non_fatal)?;
    let duration = start.elapsed();
    print_mem(&ctx.report, "memory usage after read_leaf_nodes");
    ctx.report
        .debug(&format!("reading leaf nodes: {:?}", duration));

    let start = std::time::Instant::now();
    let nr_summarized = summaries.len();
    let (nodes, mut summaries) = read_exclusive_leaves(
        ctx,
        nodes,
        metadata_sm,
        data_sm,
        summaries,
        ignore_non_fatal,
    )?;
    let nr_exclusive_leaves = summaries.len() - nr_summarized;
    let duration = start.elapsed();
    print_mem(&ctx.report, "memory usage after read_exclusive_leaves");
    ctx.report
        .debug(&format!("reading exclusive leaves: {:?}", duration));

    let start = std::time::Instant::now();
    count_mapped_blocks(roots, &nodes, metadata_sm, &mut summaries, ignore_non_fatal);
    let duration = start.elapsed();
    ctx.report
        .debug(&format!("counting mapped blocks: {:?}", duration));

    ctx.report
        .info(&format!("nr internal nodes: {}", nodes.internal_info.len()));
    ctx.report.info(&format!("nr leaves: {}", nodes.nr_leaves));
    ctx.report
        .info(&format!("nr exclusive leaves: {}", nr_exclusive_leaves));

    Ok(summaries)
}

fn count_data_mappings(
    ctx: &Context,
    sb: &Superblock,
    mapping_root: u64,
    ignore_non_fatal: bool,
) -> Result<Vec<NodeSummary>> {
    let mut path = Vec::new();
    let roots = btree_to_value_vec(
        &mut path,
        ctx.engine.as_ref(),
        ignore_non_fatal,
        mapping_root,
    )?;

    let data_root = unpack::<SMRoot>(&sb.data_sm_root[..])?;
    let data_sm = Arc::new(Aggregator::new(data_root.nr_blocks as usize));
    let metadata_sm = Arc::new(Aggregator::new(ctx.engine.get_nr_blocks() as usize));

    ctx.report.set_title("Scanning data mappings");
    let mon_meta_sm = metadata_sm.clone();
    let mon_data_sm = data_sm.clone();
    let metadata_root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    let monitor = ProgressMonitor::new(
        ctx.report.clone(),
        metadata_root.nr_allocated + (data_root.nr_allocated / 8),
        move || {
            mon_meta_sm.get_nr_allocated().unwrap_or(0)
                + (mon_data_sm.get_nr_allocated().unwrap_or(0) / 8)
        },
    );

    let summaries = count_data_mappings_(ctx, &metadata_sm, &data_sm, &roots, ignore_non_fatal)?;
    let summaries: Vec<NodeSummary> = roots
        .iter()
        .map_while(|root| {
            summaries
                .get(&(*root as u32))
                .filter(|sum| sum.nr_errors == 0)
        })
        .cloned()
        .collect();

    monitor.stop();
    ctx.report.complete();

    if summaries.len() != roots.len() {
        return Err(anyhow!("metadata contains errors"));
    }

    Ok(summaries)
}

//------------------------------------------

pub struct ThinLsOptions<'a> {
    pub input: &'a Path,
    pub engine_opts: EngineOptions,
    pub fields: Vec<OutputField>,
    pub no_headers: bool,
    pub report: Arc<Report>,
}

struct Context {
    engine: Arc<dyn IoEngine + Send + Sync>,
    report: Arc<Report>,
}

fn mk_context(opts: &ThinLsOptions) -> Result<Context> {
    let engine = EngineBuilder::new(opts.input, &opts.engine_opts)
        .exclusive(!opts.engine_opts.use_metadata_snap)
        .build()?;

    Ok(Context {
        engine,
        report: opts.report.clone(),
    })
}

fn some_counting_fields(fields: &[OutputField]) -> bool {
    use OutputField::*;

    for field in fields.iter() {
        match field {
            DeviceId | TransactionId | CreationTime | SnapshottedTime => {
                continue;
            }
            _ => {
                return true;
            }
        }
    }

    false
}

pub fn ls(opts: ThinLsOptions) -> Result<()> {
    let ctx = mk_context(&opts)?;

    let sb = if opts.engine_opts.use_metadata_snap {
        read_superblock_snap(ctx.engine.as_ref())?
    } else {
        read_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION)?
    };

    // ensure the metadata is consistent
    is_superblock_consistent(sb.clone(), ctx.engine.clone(), false)?;

    let mut path = vec![0];
    let details =
        btree_to_map::<DeviceDetail>(&mut path, ctx.engine.as_ref(), false, sb.details_root)?;

    let mut table = LsTable::new(&opts.fields, details.len(), sb.data_block_size);
    if !opts.no_headers {
        table.push_headers();
    }

    if some_counting_fields(&opts.fields) {
        let actual_sb = read_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION)?;
        let mapped = count_data_mappings(&ctx, &actual_sb, sb.mapping_root, false)?;
        for ((dev_id, detail), summary) in details.iter().zip(mapped) {
            table.push_row(
                *dev_id,
                detail,
                summary.nr_mappings,
                summary.nr_shared,
                summary.key_high,
            );
        }
    } else {
        for (dev_id, detail) in details.iter() {
            table.push_row(*dev_id, detail, 0, 0, 0);
        }
    }

    table.render(&mut std::io::stdout())
}

//------------------------------------------
