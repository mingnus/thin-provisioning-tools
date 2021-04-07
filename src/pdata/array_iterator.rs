struct ArrayIterator<V> {
    engine: Arc<dyn IoEngine + Send + Sync>,
    btree_iter: BTreeIterator<u64>,
    value_index: usize, // FIXME: use Vec::Iter
    array_block: ArrayBlock<V>,
}

impl<V: Unpack + Copy> ArrayIterator<V> {
    fn new(engine: Arc<dyn IoEngine + Send + Sync>, root: u64) -> ArrayIterator<V> {
        let btree_iter = BtreeIterator::new(engine.clone(), root);

        // FIXME: duplicated
        let entry = self.btree_iter.next().expect("unexpected end of btree"); // FIXME: error handling

        let b = self.engine
            .read(entry.1)
            .map_err(|_| anyhow!("read error on array block"))
            .expect("read error on array block");

        let array_block = unpack_array_block::<V>(b.get_data())
            .map_err(|_| anyhow!("unpack error"))
            .expect("unpack error");

        ArrayIterator {
            engine: engine,
            btree_iter,
            value_index: 0,
            array_block,
        }
    }

    fn load_array_block(&self) -> Result<()> {
        let entry = self.btree_iter.next().expect("unexpected end of btree"); // FIXME: error handling

        let b = self.engine
            .read(entry.1)
            .map_err(|_| anyhow!("read error on array block"))
            .expect("read error on array block");

        let array_block = unpack_array_block::<V>(b.get_data())
            .map_err(|_| anyhow!("unpack error"))
            .expect("unpack error");

        Ok(())
    }
}

impl<V> Iterator for ArrayIterator<V> {
    type Item = V;

    fn next(&mut self) -> Self::Item {
        self.value_index += 1;
        if self.value_index >= self.array_block.header.nr_entries {
            self.load_array_block();
            self.value_index = 0;
        }

        self.array_block.values[self.value_index]
    }
}
