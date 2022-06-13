use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;

use crate::checksum;
use crate::io_engine::*;
use crate::pdata::array::{self, *};
use crate::pdata::btree::{self, *};
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;

#[cfg(test)]
mod tests;

//------------------------------------------

pub struct ArrayWalker {
    engine: Arc<dyn IoEngine + Send + Sync>,
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    ignore_non_fatal: bool,
}

pub trait ArrayVisitor<V: Unpack> {
    fn visit(&self, index: u64, b: ArrayBlock<V>) -> array::Result<()>;
}

//------------------------------------------

struct BlockValueVisitor<'a, V> {
    engine: Arc<dyn IoEngine + Send + Sync>,
    array_visitor: &'a dyn ArrayVisitor<V>,
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
}

impl<'a, V: Unpack> BlockValueVisitor<'a, V> {
    pub fn new(
        e: Arc<dyn IoEngine + Send + Sync>,
        sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
        v: &'a dyn ArrayVisitor<V>,
    ) -> BlockValueVisitor<'a, V> {
        BlockValueVisitor {
            engine: e,
            array_visitor: v,
            sm,
        }
    }

    fn visit_array_block(&self, path: &[u64], index: u64, b: &Block) -> array::Result<()> {
        let mut array_path = path.to_vec();
        array_path.push(b.loc);

        let bt = checksum::metadata_block_type(b.get_data());
        if bt != checksum::BT::ARRAY {
            return Err(array_block_err(
                &array_path,
                &format!("checksum failed for array block {}, {:?}", b.loc, bt),
            ));
        }

        let array_block = unpack_array_block::<V>(&array_path, b.get_data())?;
        self.array_visitor.visit(index, array_block)
    }
}

impl<'a, V: Unpack> NodeVisitor<u64> for BlockValueVisitor<'a, V> {
    fn visit(
        &self,
        path: &[u64],
        kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[u64],
    ) -> btree::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        // Verify key's continuity.
        // The ordering of keys had been verified in unpack_node(),
        // so comparing the keys against its context is sufficient.
        if *keys.first().unwrap() + keys.len() as u64 != *keys.last().unwrap() + 1 {
            return Err(btree::value_err("gaps in array indicies".to_string()));
        }
        if let Some(end) = kr.end {
            if *keys.last().unwrap() + 1 != end {
                return Err(btree::value_err(
                    "non-contiguous array indicies".to_string(),
                ));
            }
        }

        let mut errs = Vec::<btree::BTreeError>::new();
        match self.engine.read_many(values) {
            Err(_) => {
                // IO completely failed on all the child blocks
                for (i, b) in values.iter().enumerate() {
                    // TODO: report the affected range of entries in the array?
                    errs.push(array::io_err(path, *b).index_context(keys[i]).into());
                }
            }
            Ok(rblocks) => {
                for (i, rb) in rblocks.into_iter().enumerate() {
                    match rb {
                        Err(_) => {
                            errs.push(array::io_err(path, values[i]).index_context(keys[i]).into());
                        }
                        Ok(b) => {
                            if let Err(e) = self.visit_array_block(path, keys[i], &b) {
                                errs.push(e.index_context(keys[i]).into());
                            } else {
                                let mut sm = self.sm.lock().unwrap();
                                sm.inc(b.loc, 1).unwrap();
                            }
                        }
                    }
                }
            }
        }

        match errs.len() {
            0 => Ok(()),
            1 => Err(errs[0].clone()),
            _ => Err(BTreeError::Aggregate(errs)),
        }
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> btree::Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> btree::Result<()> {
        Ok(())
    }
}

//------------------------------------------

impl ArrayWalker {
    pub fn new(engine: Arc<dyn IoEngine + Send + Sync>, ignore_non_fatal: bool) -> ArrayWalker {
        let nr_blocks = engine.get_nr_blocks() as u64;
        let r: ArrayWalker = ArrayWalker {
            engine,
            sm: Arc::new(Mutex::new(RestrictedSpaceMap::new(nr_blocks))),
            ignore_non_fatal,
        };
        r
    }

    pub fn new_with_sm(
        engine: Arc<dyn IoEngine + Send + Sync>,
        sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
        ignore_non_fatal: bool,
    ) -> array::Result<ArrayWalker> {
        {
            let sm = sm.lock().unwrap();
            assert_eq!(sm.get_nr_blocks().unwrap(), engine.get_nr_blocks());
        }

        Ok(ArrayWalker {
            engine,
            sm,
            ignore_non_fatal,
        })
    }

    pub fn walk<V>(&self, visitor: &dyn ArrayVisitor<V>, root: u64) -> btree::Result<()>
    where
        V: Unpack,
    {
        let w =
            BTreeWalker::new_with_sm(self.engine.clone(), self.sm.clone(), self.ignore_non_fatal)?;
        let mut path = vec![0];
        let v = BlockValueVisitor::<V>::new(self.engine.clone(), self.sm.clone(), visitor);
        w.walk(&mut path, &v, root)
    }
}

//------------------------------------------

struct BlockValueVisitorThreaded<V> {
    engine: Arc<dyn IoEngine + Send + Sync>,
    array_visitor: Arc<dyn ArrayVisitor<V> + Send + Sync>,
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
}

impl<V: Unpack> BlockValueVisitorThreaded<V> {
    pub fn new(
        e: Arc<dyn IoEngine + Send + Sync>,
        sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
        v: Arc<dyn ArrayVisitor<V> + Send + Sync>,
    ) -> BlockValueVisitorThreaded<V> {
        BlockValueVisitorThreaded {
            engine: e,
            array_visitor: v,
            sm,
        }
    }

    fn visit_array_block(&self, path: &[u64], index: u64, b: &Block) -> array::Result<()> {
        let mut array_path = path.to_vec();
        array_path.push(b.loc);

        let bt = checksum::metadata_block_type(b.get_data());
        if bt != checksum::BT::ARRAY {
            return Err(array_block_err(
                &array_path,
                &format!("checksum failed for array block {}, {:?}", b.loc, bt),
            ));
        }

        let array_block = unpack_array_block::<V>(&array_path, b.get_data())?;
        self.array_visitor.visit(index, array_block)
    }
}

impl<V: Unpack> NodeVisitor<u64> for BlockValueVisitorThreaded<V> {
    fn visit(
        &self,
        path: &[u64],
        kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[u64],
    ) -> btree::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        // Verify key's continuity.
        // The ordering of keys had been verified in unpack_node(),
        // so comparing the keys against its context is sufficient.
        if *keys.first().unwrap() + keys.len() as u64 != *keys.last().unwrap() + 1 {
            return Err(btree::value_err("gaps in array indicies".to_string()));
        }
        if let Some(end) = kr.end {
            if *keys.last().unwrap() + 1 != end {
                return Err(btree::value_err(
                    "non-contiguous array indicies".to_string(),
                ));
            }
        }

        // TODO: visit array blocks within a leaf node using multiple threads
        let mut errs = Vec::<btree::BTreeError>::new();
        match self.engine.read_many(values) {
            Err(_) => {
                // IO completely failed on all the child blocks
                for (i, b) in values.iter().enumerate() {
                    // TODO: report the affected range of entries in the array?
                    errs.push(array::io_err(path, *b).index_context(keys[i]).into());
                }
            }
            Ok(rblocks) => {
                for (i, rb) in rblocks.into_iter().enumerate() {
                    match rb {
                        Err(_) => {
                            errs.push(array::io_err(path, values[i]).index_context(keys[i]).into());
                        }
                        Ok(b) => {
                            if let Err(e) = self.visit_array_block(path, keys[i], &b) {
                                errs.push(e.index_context(keys[i]).into());
                            } else {
                                let mut sm = self.sm.lock().unwrap();
                                sm.inc(b.loc, 1).unwrap();
                            }
                        }
                    }
                }
            }
        }

        match errs.len() {
            0 => Ok(()),
            1 => Err(errs[0].clone()),
            _ => Err(BTreeError::Aggregate(errs)),
        }
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> btree::Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> btree::Result<()> {
        Ok(())
    }
}

pub fn walk_array_threaded<V>(
    engine: Arc<dyn IoEngine + Send + Sync>,
    visitor: Arc<dyn ArrayVisitor<V> + Send + Sync>,
    pool: &ThreadPool,
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    root: u64,
    ignore_non_fatal: bool,
) -> btree::Result<()>
where
    V: Unpack + 'static,
{
    let mut path = vec![0];
    let w = Arc::new(BTreeWalker::new_with_sm(
        engine.clone(),
        sm.clone(),
        ignore_non_fatal,
    )?);
    let v = Arc::new(BlockValueVisitorThreaded::<V>::new(engine, sm, visitor));
    walk_threaded(&mut path, w, pool, v, root)
}

//------------------------------------------

struct BlockPathCollector {
    ablocks: Mutex<BTreeMap<u64, (Vec<u64>, u64)>>,
}

impl BlockPathCollector {
    fn new() -> BlockPathCollector {
        BlockPathCollector {
            ablocks: Mutex::new(BTreeMap::new()),
        }
    }
}

impl NodeVisitor<u64> for BlockPathCollector {
    fn visit(
        &self,
        path: &[u64],
        kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[u64],
    ) -> btree::Result<()> {
        // Verify key's continuity.
        // The ordering of keys had been verified in unpack_node(),
        // so comparing the keys against the key range is sufficient.
        if *keys.first().unwrap() + keys.len() as u64 != *keys.last().unwrap() + 1 {
            return Err(btree::value_err("gaps in array indicies".to_string()));
        }
        if let Some(end) = kr.end {
            if *keys.last().unwrap() + 1 != end {
                return Err(btree::value_err(
                    "non-contiguous array indicies".to_string(),
                ));
            }
        }

        let mut ablocks = self.ablocks.lock().unwrap();
        for (k, v) in keys.iter().zip(values) {
            ablocks.insert(*k, (path.to_vec(), *v));
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

pub fn collect_array_blocks_with_path(
    engine: Arc<dyn IoEngine + Send + Sync>,
    ignore_non_fatal: bool,
    root: u64,
) -> btree::Result<BTreeMap<u64, (Vec<u64>, u64)>> {
    let walker = BTreeWalker::new(engine, ignore_non_fatal);
    let visitor = BlockPathCollector::new();
    walker.walk(&mut vec![0], &visitor, root)?;
    Ok(visitor.ablocks.into_inner().unwrap())
}

//------------------------------------------
