use anyhow::{anyhow, Result};
use std::cmp::Ordering;
use std::path::Path;
use std::sync::{mpsc, Arc};
use std::thread;

use thinp::commands::engine::*;
use thinp::io_engine::IoEngine;
use thinp::pdata::btree_walker::btree_to_map;
use thinp::pdata::space_map::common::SMRoot;
use thinp::pdata::space_map::metadata::core_metadata_sm;
use thinp::pdata::unpack::unpack;
use thinp::report::Report;
use thinp::thin::block_time::*;
use thinp::thin::device_detail::DeviceDetail;
use thinp::thin::dump::RunBuilder;
use thinp::thin::ir::{self, MetadataVisitor};
use thinp::thin::metadata_repair::is_superblock_consistent;
use thinp::thin::restore::Restorer;
use thinp::thin::superblock::*;
use thinp::write_batcher::WriteBatcher;

//------------------------------------------

use std::collections::BTreeMap;
use thinp::io_engine::Block;
use thinp::pdata::btree::{self, *};
use thinp::pdata::btree_error::KeyRange;
use thinp::pdata::btree_leaf_walker::LeafVisitor;
use thinp::pdata::btree_leaf_walker::LeafWalker;
use thinp::pdata::space_map::RestrictedSpaceMap;
use thinp::pdata::unpack::Unpack;

struct CollectLeaves {
    leaves: Vec<u64>,
}

impl CollectLeaves {
    fn new() -> CollectLeaves {
        CollectLeaves { leaves: Vec::new() }
    }
}

impl LeafVisitor<BlockTime> for CollectLeaves {
    fn visit(&mut self, _kr: &KeyRange, b: u64) -> btree::Result<()> {
        self.leaves.push(b);
        Ok(())
    }

    fn visit_again(&mut self, b: u64) -> btree::Result<()> {
        self.leaves.push(b);
        Ok(())
    }

    fn end_walk(&mut self) -> btree::Result<()> {
        Ok(())
    }
}

fn collect_leaves(
    engine: Arc<dyn IoEngine + Send + Sync>,
    roots: &[u64],
) -> Result<BTreeMap<u64, Vec<u64>>> {
    let mut map: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    let mut sm = RestrictedSpaceMap::new(engine.get_nr_blocks());

    for r in roots {
        let mut w = LeafWalker::new(engine.clone(), &mut sm, false);
        let mut v = CollectLeaves::new();
        let mut path = vec![0];
        w.walk::<CollectLeaves, BlockTime>(&mut path, &mut v, *r)?;

        map.insert(*r, v.leaves);
    }

    Ok(map)
}

//------------------------------------------

struct MappingIterator {
    engine: Arc<dyn IoEngine + Send + Sync>,
    leaves: Vec<u64>,
    batch_size: usize,
    cached_leaves: Vec<Block>,
    node: Node<BlockTime>,
    nr_entries: usize, // nr_entries in the current visiting node
    pos: [usize; 2],   // leaf index and entry index in leaf
}

impl MappingIterator {
    fn new(engine: Arc<dyn IoEngine + Send + Sync>, leaves: Vec<u64>) -> Result<Self> {
        let batch_size = engine.get_batch_size();
        let len = std::cmp::min(batch_size, leaves.len());
        let cached_leaves = Self::read_blocks(&engine, &leaves[..len])?;
        let node =
            unpack_node::<BlockTime>(&[], cached_leaves[0].get_data(), true, leaves.len() > 1)?;
        let nr_entries = Self::get_nr_entries(&node);

        let pos = [0, 0];

        Ok(Self {
            engine,
            leaves,
            batch_size,
            cached_leaves,
            node,
            nr_entries,
            pos,
        })
    }

    fn read_blocks(
        engine: &Arc<dyn IoEngine + Send + Sync>,
        blocks: &[u64],
    ) -> std::io::Result<Vec<Block>> {
        engine.read_many(blocks)?.into_iter().collect()
    }

    fn get(&self) -> Option<(u64, &BlockTime)> {
        if self.pos[0] < self.leaves.len() {
            match &self.node {
                Node::Internal { .. } => {
                    panic!("not a leaf");
                }
                Node::Leaf { keys, values, .. } => {
                    if keys.is_empty() {
                        None
                    } else {
                        Some((keys[self.pos[1]], &values[self.pos[1]]))
                    }
                }
            }
        } else {
            None
        }
    }

    fn get_nr_entries<V: Unpack>(node: &Node<V>) -> usize {
        match node {
            Node::Internal { header, .. } => header.nr_entries as usize,
            Node::Leaf { header, .. } => header.nr_entries as usize,
        }
    }

    fn inc_pos(&mut self) -> bool {
        if self.pos[0] < self.leaves.len() {
            self.pos[1] += 1;
            self.pos[1] >= self.nr_entries
        } else {
            false
        }
    }

    fn next_node(&mut self) -> Result<()> {
        self.pos[0] += 1;
        self.pos[1] = 0;

        if self.pos[0] == self.leaves.len() {
            return Ok(()); // reach the end
        }

        let idx = self.pos[0] % self.batch_size;

        // FIXME: reuse the code in the constructor
        if idx == 0 {
            let endpos = std::cmp::min(self.pos[0] + self.batch_size, self.leaves.len());
            self.cached_leaves =
                Self::read_blocks(&self.engine, &self.leaves[self.pos[0]..endpos])?;
        }

        self.node = unpack_node::<BlockTime>(&[], self.cached_leaves[idx].get_data(), true, true)?;
        self.nr_entries = Self::get_nr_entries(&self.node);

        Ok(())
    }

    fn step(&mut self) -> Result<()> {
        if self.inc_pos() {
            self.next_node()?;
        }
        Ok(())
    }
}

//------------------------------------------

struct MappingStream {
    iter: MappingIterator,
    current: Option<(u64, BlockTime)>,
}

impl MappingStream {
    fn new(engine: Arc<dyn IoEngine + Send + Sync>, leaves: Vec<u64>) -> Result<Self> {
        let iter = MappingIterator::new(engine, leaves)?;
        let current = iter.get().map(|(k, v)| (k, *v));
        Ok(Self { iter, current })
    }

    fn more_mappings(&self) -> bool {
        self.current.is_some()
    }

    fn get_mapping(&self) -> Option<&(u64, BlockTime)> {
        self.current.as_ref()
    }

    fn consume(&mut self) -> Result<Option<(u64, BlockTime)>> {
        /*self.iter.step()?;
        match self.iter.get() {
            Some(m) => {
                let prev = self.current.replace(m);
                Ok(prev)
            }
            None => {
                let prev = self.current.take();
                Ok(prev)
            }
        }*/

        /*if self.more_mappings() {
            self.iter.step()?;
            let prev = match self.iter.get() {
                Some((k, &v)) => self.current.replace((k, v)),
                None => self.current.take(),
            };
            Ok(prev)
        } else {
            Ok(None)
        }*/

        match self.get_mapping() {
            Some(&m) => {
                let r = Ok(Some(m));
                self.iter.step()?;
                self.current = self.iter.get().map(|(k, &v)| (k, v));
                r
            }
            None => Ok(None),
        }
    }

    fn step(&mut self) -> Result<()> {
        /*if self.more_mappings() {
            self.iter.step()?;
            if let Some((k, &v)) = self.iter.get() {
                self.current.replace((k, v));
            } else {
                self.current = None;
            }
        }*/

        if self.more_mappings() {
            self.iter.step()?;
            self.current = self.iter.get().map(|(k, &v)| (k, v));
        }
        Ok(())
    }
}

struct MergeIterator {
    base_stream: MappingStream,
    snap_stream: MappingStream,
}

impl MergeIterator {
    fn new(
        engine: Arc<dyn IoEngine + Send + Sync>,
        base_root: u64,
        snap_root: u64,
    ) -> Result<Self> {
        let mut leaves = collect_leaves(engine.clone(), &[base_root, snap_root])?;
        let base_stream = MappingStream::new(engine.clone(), leaves.remove(&base_root).unwrap())?;
        let snap_stream = MappingStream::new(engine, leaves.remove(&snap_root).unwrap())?;

        Ok(Self {
            base_stream,
            snap_stream,
        })
    }

    /*fn consume_base(&mut self) -> Result<Option<(u64, BlockTime)>> {
        match self.base_stream.get_mapping() {
            Some(m) => {
                let m = m.clone();
                self.base_stream.next_mapping()?;
                Ok(m)
            }
            None => {
                Ok(None)
            }
        }
    }*/

    fn next(&mut self) -> Result<Option<(u64, BlockTime)>> {
        match (
            self.base_stream.more_mappings(),
            self.snap_stream.more_mappings(),
        ) {
            (true, true) => {
                let base_map = self.base_stream.get_mapping().unwrap();
                let snap_map = self.snap_stream.get_mapping().unwrap();

                match base_map.0.cmp(&snap_map.0) {
                    Ordering::Less => self.base_stream.consume(),
                    Ordering::Equal => {
                        self.base_stream.step()?;
                        self.snap_stream.consume()
                    }
                    Ordering::Greater => self.snap_stream.consume(),
                }
            }
            (true, false) => self.base_stream.consume(),
            (false, true) => self.snap_stream.consume(),
            (false, false) => Ok(None),
        }
    }
}

//------------------------------------------

fn merge(
    engine: Arc<dyn IoEngine + Send + Sync>,
    out: &mut dyn MetadataVisitor,
    sb: &Superblock,
    origin_id: u64,
    snap_id: u64,
) -> Result<()> {
    const QUEUE_DEPTH: usize = 4;

    let roots = btree_to_map::<u64>(&mut vec![], engine.clone(), false, sb.mapping_root)?;
    let details =
        btree_to_map::<DeviceDetail>(&mut vec![], engine.clone(), false, sb.details_root)?;

    let origin_root = *roots
        .get(&origin_id)
        .ok_or_else(|| anyhow!("Unable to find mapping tree for the origin"))?;
    let snap_dev = *details
        .get(&snap_id)
        .ok_or_else(|| anyhow!("Unable to find the details for the origin"))?;
    let snap_root = *roots
        .get(&snap_id)
        .ok_or_else(|| anyhow!("Unable to find mapping tree for the snapshot"))?;

    let mut iter = MergeIterator::new(engine, origin_root, snap_root)?;

    let data_root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let out_sb = ir::Superblock {
        uuid: "".to_string(),
        time: sb.time,
        transaction: sb.transaction_id,
        flags: None,
        version: Some(sb.version),
        data_block_size: sb.data_block_size,
        nr_data_blocks: data_root.nr_blocks,
        metadata_snap: None,
    };

    let out_dev = ir::Device {
        dev_id: snap_id as u32,
        mapped_blocks: snap_dev.mapped_blocks,
        transaction: snap_dev.transaction_id,
        creation_time: snap_dev.creation_time,
        snap_time: snap_dev.snapshotted_time,
    };

    let (tx, rx) = mpsc::sync_channel::<Vec<ir::Map>>(QUEUE_DEPTH);

    let merger = thread::spawn(move || -> Result<()> {
        let mut builder = RunBuilder::new();
        let mut runs = Vec::with_capacity(1024);

        while let Some((k, v)) = iter.next()? {
            if let Some(run) = builder.next(k, v.block, v.time) {
                runs.push(run);
                if runs.len() == 1024 {
                    tx.send(runs)?;
                    runs = Vec::with_capacity(1024);
                }
            }
        }

        if let Some(run) = builder.complete() {
            runs.push(run);
        }

        if !runs.is_empty() {
            tx.send(runs)?;
        }

        drop(tx);
        Ok(())
    });

    out.superblock_b(&out_sb)?;
    out.device_b(&out_dev)?;

    while let Ok(runs) = rx.recv() {
        for run in &runs {
            out.map(run)?;
        }
    }

    merger
        .join()
        .expect("unexpected error")
        .expect("metadata contains error");

    out.device_e()?;
    out.superblock_e()?;
    out.eof()?;

    Ok(())
}

//------------------------------------------

pub struct ThinMergeOptions<'a> {
    pub input: &'a Path,
    pub output: &'a Path,
    pub engine_opts: EngineOptions,
    pub report: Arc<Report>,
    pub origin: u64,
    pub snapshot: u64,
}

struct Context {
    report: Arc<Report>,
    engine_in: Arc<dyn IoEngine + Send + Sync>,
    engine_out: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &ThinMergeOptions) -> Result<Context> {
    let engine_in = EngineBuilder::new(opts.input, &opts.engine_opts).build()?;

    let mut out_opts = opts.engine_opts.clone();
    out_opts.engine_type = EngineType::Sync; // sync write temporarily
    let engine_out = EngineBuilder::new(opts.output, &out_opts)
        .write(true)
        .build()?;

    Ok(Context {
        report: opts.report.clone(),
        engine_in,
        engine_out,
    })
}

pub fn merge_thins(opts: ThinMergeOptions) -> Result<()> {
    let ctx = mk_context(&opts)?;

    let sb = if opts.engine_opts.use_metadata_snap {
        read_superblock_snap(ctx.engine_in.as_ref())?
    } else {
        read_superblock(ctx.engine_in.as_ref(), SUPERBLOCK_LOCATION)?
    };

    // ensure the metadata is consistent
    is_superblock_consistent(sb.clone(), ctx.engine_in.clone(), false)?;

    let sm = core_metadata_sm(ctx.engine_out.get_nr_blocks(), 2);
    let batch_size = ctx.engine_out.get_batch_size();
    let mut w = WriteBatcher::new(ctx.engine_out, sm.clone(), batch_size);
    let mut restorer = Restorer::new(&mut w, ctx.report);

    merge(
        ctx.engine_in,
        &mut restorer,
        &sb,
        opts.origin,
        opts.snapshot,
    )
}

//------------------------------------------
