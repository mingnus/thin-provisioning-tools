use fixedbitset::FixedBitSet;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::io_engine::*;
use crate::pdata::btree::{self, *};
use crate::pdata::btree_walker::{NodeVisitor, ValueCollector};
use crate::pdata::space_map::aggregator::*;
use crate::pdata::unpack::Unpack;
use crate::utils::ranged_bitset_iter::RangedBitsetIter;

struct LayerHandler<'a> {
    aggregator: &'a Aggregator,
    children: FixedBitSet,
    is_root: bool,
    ignore_non_fatal: bool,
}

impl<'a> LayerHandler<'a> {
    fn new(is_root: bool, aggregator: &'a Aggregator, ignore_non_fatal: bool) -> Self {
        Self {
            is_root,
            aggregator,
            ignore_non_fatal,
            children: FixedBitSet::with_capacity(aggregator.get_nr_blocks()),
        }
    }

    fn get_children(self) -> FixedBitSet {
        self.children
    }
}

impl<'a> ReadHandler for LayerHandler<'a> {
    fn handle(&mut self, loc: u64, data: std::io::Result<&[u8]>) {
        match data {
            Ok(data) => {
                let node =
                    check_and_unpack_node_::<u64>(data, loc, self.ignore_non_fatal, self.is_root);

                if node.is_err() {
                    return;
                }

                let node = node.unwrap();

                if let Node::Internal { keys, values, .. } = node {
                    // insert the node info in pre-order fashion to better detect loops in the path
                    let children = values.iter().map(|v| *v as u32).collect::<Vec<u32>>(); // FIXME: slow
                    let seen = self.aggregator.test_and_inc(&values);

                    for (i, v) in values.iter().enumerate() {
                        if !seen.contains(i) {
                            self.children.insert(*v as usize);
                        }
                    }
                }
            }
            Err(_) => {
                // TOOD: do something
            }
        }
    }

    fn complete(&mut self) {}
}

/// Gets the depth of a bottom level mapping tree.  0 means the root is a leaf node.
// FIXME: what if there's an error on the path to the leftmost leaf?
fn get_depth<V: Unpack>(
    engine: &dyn IoEngine,
    path: &mut Vec<u64>,
    root: u64,
    is_root: bool,
) -> btree::Result<usize> {
    use Node::*;

    let b = engine.read(root).map_err(|_| io_err(path))?;
    let node = check_and_unpack_node::<V>(&b, true, is_root).map_err(|e| node_err(path, e))?;

    match node {
        Internal { values, .. } => {
            // recurse down to the first good leaf
            let mut last_err = None;
            for child in values {
                if path.contains(&child) {
                    continue; // skip loops
                }

                path.push(child);
                match get_depth::<V>(engine, path, child, false) {
                    Ok(n) => return Ok(n + 1),
                    Err(e) => {
                        last_err = Some(e);
                    }
                }
                path.pop();
            }
            Err(last_err.unwrap_or_else(|| node_err(path, NodeError::NumEntriesTooSmall).into()))
        }
        Leaf { .. } => Ok(0),
    }
}

pub fn read_internal_nodes<V: Unpack>(
    engine: Arc<dyn IoEngine>,
    io_buffers: &mut BufferPool,
    aggregator: &Aggregator,
    root: u64,
    ignore_non_fatal: bool,
) -> anyhow::Result<(usize, FixedBitSet)> {
    let seen = aggregator.test_and_inc(&[root as u64]);
    if seen.contains(0) {
        return Ok((0, FixedBitSet::new()));
    }

    let nr_blocks = aggregator.get_nr_blocks();
    let mut current_layer = FixedBitSet::with_capacity(nr_blocks as usize);
    current_layer.insert(root as usize);

    let mut path = Vec::new();
    let depth = get_depth::<V>(engine.as_ref(), &mut path, root as u64, true)?;
    if depth == 0 {
        return Ok((0, current_layer)); // TODO: avoid allocating the bitset in this situation
    }

    // Read the internal nodes, layer by layer.
    let mut is_root = true;
    for _d in (0..depth).rev() {
        let mut handler = LayerHandler::new(is_root, aggregator, ignore_non_fatal);
        is_root = false;

        engine.read_blocks(
            io_buffers,
            &mut current_layer.ones().map(|n| n as u64),
            &mut handler,
        )?;
        current_layer = handler.get_children();
    }

    Ok((depth, current_layer))
}

struct LeafHandler<V: Unpack> {
    visitor: Arc<dyn NodeVisitor<V>>,
    is_root: bool,
    ignore_non_fatal: bool,
    dummy: std::marker::PhantomData<V>,
}

impl<V: Unpack> LeafHandler<V> {
    fn new(nv: Arc<dyn NodeVisitor<V>>, is_root: bool, ignore_non_fatal: bool) -> Self {
        Self {
            visitor: nv,
            is_root,
            ignore_non_fatal,
            dummy: std::marker::PhantomData,
        }
    }
}

impl<V: Unpack> ReadHandler for LeafHandler<V> {
    fn handle(&mut self, loc: u64, data: std::io::Result<&[u8]>) {
        match data {
            Ok(data) => {
                // Allow under full nodes in this phase.  The under full
                // property will be check later based on the path context.
                if let Ok(Node::Leaf {
                    header,
                    keys,
                    values,
                }) = &check_and_unpack_node_(data, loc, true, true)
                {
                    // FIXME: avoid unpacking the node, which copies the key-values
                    let _ = self
                        .visitor
                        .visit(&[], &KeyRange::default(), header, keys, values);
                }
            }
            Err(_e) => {
                todo!();
            }
        }
    }

    fn complete(&mut self) {}
}

fn unpacker<V: Unpack>(
    engine: Arc<dyn IoEngine>,
    nv: Arc<dyn NodeVisitor<V>>,
    leaves: &mut dyn Iterator<Item = u64>,
    is_root: bool,
    ignore_non_fatal: bool,
) -> anyhow::Result<()> {
    let io_block_size = 64 * 1024;
    let buffer_size = 16 * 1024 * 1024; // 16m
    let nr_io_blocks = buffer_size / io_block_size;
    let mut buffers = BufferPool::new(nr_io_blocks, io_block_size);

    let mut handler = LeafHandler::<V>::new(nv, is_root, ignore_non_fatal);
    engine.read_blocks(&mut buffers, leaves, &mut handler)?;

    Ok(())
}

pub fn read_leaf_nodes<V: Unpack>(
    engine: Arc<dyn IoEngine>,
    nv: Arc<dyn NodeVisitor<V> + Send + Sync>,
    leaves: &FixedBitSet,
    depth: usize,
    nr_unpackers: usize,
    ignore_non_fatal: bool,
) {
    // Kick off the unpackers
    std::thread::scope(|s| {
        let chunk_size = (leaves.len() + nr_unpackers - 1) / nr_unpackers;
        for i in 0..nr_unpackers {
            let engine = engine.clone();
            let l_begin = i * chunk_size;
            let l_end = ((i + 1) * chunk_size).min(leaves.len());
            let mut leaves = RangedBitsetIter::new(&leaves, l_begin..l_end);
            let v = nv.clone();

            s.spawn(move || unpacker::<V>(engine, v, &mut leaves, depth == 0, ignore_non_fatal));
        }
    });
}

pub fn read_nodes<V: Unpack>(
    engine: Arc<dyn IoEngine>,
    nv: Arc<dyn NodeVisitor<V> + Send + Sync>,
    io_buffers: &mut BufferPool,
    aggregator: &Aggregator,
    root: u64,
    ignore_non_fatal: bool,
) -> anyhow::Result<()> {
    let (depth, leaves) = read_internal_nodes::<V>(
        engine.clone(),
        io_buffers,
        aggregator,
        root,
        ignore_non_fatal,
    )?;
    read_leaf_nodes(engine, nv, &leaves, depth, 4, ignore_non_fatal);
    Ok(())
}

//------------------------------------------

pub fn btree_to_map_with_aggregator<V: Unpack + Copy + Send + Sync + 'static>(
    engine: Arc<dyn IoEngine>,
    aggregator: &Aggregator,
    root: u64,
    ignore_non_fatal: bool,
) -> anyhow::Result<BTreeMap<u64, V>> {
    let buffer_size = 16 * 1024 * 1024;
    let nr_io_blocks = buffer_size / BLOCK_SIZE;
    let mut pool = BufferPool::new(nr_io_blocks, BLOCK_SIZE);

    // FIXME: the mutex lock inside ValueCollector might slow down the ReadHandler
    let visitor = Arc::new(ValueCollector::new());
    read_nodes(
        engine,
        visitor.clone(),
        &mut pool,
        aggregator,
        root,
        ignore_non_fatal,
    )?;

    let mut results = BTreeMap::new();
    {
        let mut r = visitor.values.lock().unwrap();
        std::mem::swap(&mut results, &mut r);
    }
    Ok(results)
}

//------------------------------------------
