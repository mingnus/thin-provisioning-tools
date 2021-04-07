pub struct BitsetIterator {
    iter: array_iterator,
    array_index: u32, // index within an array_block
    bit_index: u32, // index within u64
    current_bits: u64;
}

impl BitsetIterator {
    pub fn new(engine: Arc<dyn IoEngine + Send + Sync>, root: u64) -> BitsetIterator {
        let iter = ArrayIterator::new(engine, root);
        let bits = iter.next();

        bitset_iterator {
            iter: iter,
            array_index: 0,
            bit_index: 0,
            current_bits: bits,
        }
    }
}

impl Iterator for BitsetIterator {
    type Item = bool; // FIXME: use anyhow::Result<bool>

    // TODO: error handling
    fn next(&self) -> Option<Self::Item> {
        if !self.entries_remaining {
            return None;
        }

        self.entries_remaining -= 1;
        self.bit_index += 1;
        if self.bit_index > 63 {
            self.array_index += 1;
            self.bit_index = 0;
            self.current_bits = self.iter.next().expect("cannot iterate to the next array entry");
        }

        self.current_bits & (1 << self.bit_index)
    }
}
