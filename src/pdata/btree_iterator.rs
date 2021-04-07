use anyhow::{anyhow, Result};
use std::collections::VecDeque;
use std::sync::Arc;
use std::vec::Vec;

use crate::io_engine::*;
use crate::pdata::btree::*;
use crate::pdata::unpack::*;

struct Frame<V: Unpack> {
    node: Node<V>,
    value_index: u32,
}

impl<V: Unpack> Frame<V> {
    pub fn new(node: Node<V>) -> Frame<V> {
        Frame {
            node,
            value_index: 0,
        }
    }
}

pub struct BTreeIterator<V: Unpack> {
    engine: Arc<dyn IoEngine + Send + Sync>,
    path: Vec<u64>,
    spine: VecDeque<Frame<V>>,
    ignore_non_fatal: bool,
}

impl<V: Unpack + Copy> BTreeIterator<V> {
    // TODO: do not return Err in new()?
    pub fn new(
        engine: Arc<dyn IoEngine + Send + Sync>,
        root: u64,
        ignore_non_fatal: bool
    ) -> Result<BTreeIterator<V>> {
        let mut iter = BTreeIterator::<V> {
            engine: engine.clone(),
            path: vec!(),
            spine: VecDeque::<Frame<V>>::new(),
            ignore_non_fatal,
        };
        iter.push_node(root)?;
        iter.find_leaf()?;
        Ok(iter)
    }

    fn push_node(&mut self, blocknr: u64) -> Result<()> {
        let b = self.engine.read(blocknr).map_err(|_| anyhow!(""))?;
        self.path.push(blocknr);
        let n = unpack_node::<V>(
            &self.path,
            &b.get_data(),
            self.ignore_non_fatal,
            self.spine.is_empty(),
        )?;
        self.spine.push_back(Frame::new(n));
        Ok(())
    }

    // FIXME: refine the return value?
    // - Ok: returns reference to the top frame?
    // - Err: returns unreachable key range
    fn find_leaf(&mut self) -> Result<()> {
        while let Some(frame) = self.spine.back() {
            match &frame.node {
                Node::Internal { values, .. } => {
                    let blocknr = values[frame.value_index as usize];
                    self.push_node(blocknr)?;
                },
                Node::Leaf { .. } => {
                    break;
                },
            }
        }
        Ok(())
    }

    fn inc_or_backtrack(&mut self) -> Option<()> {
        loop {
            let backtrack;
            if let Some(frame) = self.spine.back_mut() {
                match frame.node {
                    Node::Internal { header, .. } => {
                        // FIXME: return error representing a broken key range, and inc value_index
                        //return Err(anyhow!("unexpected top frame"));

                        // FIXME: duplicate
                        if frame.value_index + 1 < header.nr_entries {
                            frame.value_index += 1;
                            backtrack = false;
                        } else {
                            backtrack = true;
                        }
                    },
                    Node::Leaf { header, .. } => {
                        if frame.value_index + 1 < header.nr_entries {
                            frame.value_index += 1;
                            backtrack = false;
                        } else {
                            backtrack = true;
                        }
                    },
                }
            } else {
                return None;
            }

            if backtrack {
                self.spine.pop_back();
            } else {
                break;
            }
        }
        Some(())
    }

    pub fn next(&mut self) -> Option<Result<V>>{
        if Some(()) == self.inc_or_backtrack() {
            if let Err(e) = self.find_leaf() { // FIXME: do not unwrap Err
                return Some(Err(e));
            }
        }

        if let Some(frame) = self.spine.back() {
            match &frame.node { // FIXME: ??
                Node::Internal { .. } => return Some(Err(anyhow!("unexpected frame top"))), 
                Node::Leaf { values, .. } => return Some(Ok(values[frame.value_index as usize])),
            }
        } else {
            None
        }
    }
}
