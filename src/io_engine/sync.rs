use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Result};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;

use crate::io_engine::base::*;
use crate::io_engine::buffer_pool::*;
use crate::io_engine::gaps::*;
use crate::io_engine::utils::*;

#[cfg(test)]
mod tests;

//------------------------------------------

pub struct SyncIoEngine {
    nr_blocks: u64,
    file: File,
}

impl SyncIoEngine {
    fn open_file<P: AsRef<Path>>(path: P, writable: bool, excl: bool) -> Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(writable)
            .custom_flags(if excl {
                libc::O_EXCL | libc::O_DIRECT
            } else {
                libc::O_DIRECT
            })
            .open(path)?;

        Ok(file)
    }

    pub fn new<P: AsRef<Path>>(path: P, writable: bool) -> Result<Self> {
        SyncIoEngine::new_with(path, writable, true)
    }

    pub fn new_with<P: AsRef<Path>>(path: P, writable: bool, excl: bool) -> Result<Self> {
        let nr_blocks = get_nr_blocks(path.as_ref())?; // check file mode before opening it
        let file = SyncIoEngine::open_file(path.as_ref(), writable, excl)?;

        Ok(SyncIoEngine { nr_blocks, file })
    }

    fn bad_read<T>() -> Result<T> {
        Err(io::Error::new(io::ErrorKind::Other, "read failed"))
    }

    fn bad_write() -> io::Error {
        io::Error::new(io::ErrorKind::Other, "write failed")
    }

    fn read_many_<T: VectoredIo>(
        vio: VectoredBlockIo<T>,
        io_block_pool: &mut BufferPool,
        blocks: &[u64],
        handler: &mut dyn ReadHandler,
    ) -> Result<()> {
        const GAP_THRESHOLD: u64 = 8;

        if blocks.is_empty() {
            return Ok(());
        }

        // Split into runs of adjacent blocks
        let batches = generate_runs(blocks, GAP_THRESHOLD, libc::UIO_MAXIOV as u64);

        // Issue ios
        // FIXME: put those IOBlocks on get error
        let mut bs = blocks
            .iter()
            .map(|loc| io_block_pool.get(*loc).ok_or_else(|| io::Error::new(io::ErrorKind::Other, "out of pool space")))
            .collect::<Result<Vec<IOBlock>>>()?;

        let mut issued_minus_gaps = 0;

        let mut bs_index = 0;

        let mut gaps: Vec<IOBlock> = Vec::with_capacity(16);

        for batch in batches {
            let mut first = None;

            // build io
            let mut buffers = Vec::with_capacity(16);
            for op in &batch {
                match op {
                    RunOp::Run(b, e) => {
                        if first.is_none() {
                            first = Some(*b);
                        }
                        for b in *b..*e {
                            assert_eq!(b, bs[issued_minus_gaps].loc);

                            let data = unsafe {
                                std::slice::from_raw_parts_mut(bs[issued_minus_gaps].data, 4096)
                            };
                            buffers.push(data);
                            issued_minus_gaps += 1;
                        }
                    }
                    RunOp::Gap(b, e) => {
                        // Initially I reused the same junk buffer for the gaps, since I don't intend
                        // using what is read.  But this causes issues with dm-integrity.
                        if first.is_none() {
                            first = Some(*b);
                        }
                        for loc in *b..*e {
                            let gap_buffer = io_block_pool.get(loc).ok_or_else(|| io::Error::new(io::ErrorKind::Other, "out of pool space"))?;
                            let data = unsafe {
                                std::slice::from_raw_parts_mut(gap_buffer.data, 4096)
                            };
                            buffers.push(data);
                            gaps.push(gap_buffer);
                        }
                    }
                }
            }

            assert!(first.is_some());

            // Issue io
            let run_results = vio.read_blocks(&mut buffers[..], first.unwrap() * BLOCK_SIZE as u64);

            let mut rindex = 0;
            if let Ok(run_results) = run_results {
                // select results
                for op in batch {
                    match op {
                        RunOp::Run(b, e) => {
                            for i in b..e {
                                if run_results[rindex].is_err() {
                                    handler.handle(blocks[bs_index], Self::bad_read());
                                } else {
                                    let b = &bs[bs_index];
                                    assert_eq!(i, b.loc);
                                    let data = unsafe {
                                        // FIXME: do not hardcode 4k block size once we know
                                        // why BufferPool::get_block_size() returns 64k.
                                        std::slice::from_raw_parts_mut(b.data, 4096)
                                    };
                                    handler.handle(b.loc, Ok(data));
                                }
                                bs_index += 1;
                                rindex += 1;
                            }
                        }
                        RunOp::Gap(b, e) => {
                            rindex += (e - b) as usize;
                        }
                    }
                }
            } else {
                // Error everything
                for op in batch {
                    match op {
                        RunOp::Run(b, e) => {
                            for _ in b..e {
                                handler.handle(blocks[bs_index], Self::bad_read());
                                bs_index += 1;
                            }
                            rindex += (e - b) as usize;
                        }
                        RunOp::Gap(b, e) => {
                            rindex += (e - b) as usize;
                        }
                    }
                }
            }
        }
        //assert_eq!(rindex, run_results.len());
        assert_eq!(bs_index, blocks.len());
        for b in bs {
            io_block_pool.put(b);
        }
        for b in gaps {
            io_block_pool.put(b);
        }

        Ok(())
    }

    fn write_many_<T: VectoredIo>(
        vio: VectoredBlockIo<T>,
        blocks: &[Block],
    ) -> Result<Vec<Result<()>>> {
        if blocks.is_empty() {
            return Ok(vec![]);
        }

        let batches = find_runs_nogap(blocks, libc::UIO_MAXIOV as usize);
        let mut issued: usize = 0;
        let mut results: Vec<Result<()>> = Vec::with_capacity(blocks.len());

        for (batch_start, batch_size) in batches {
            assert!(batch_size > 0);

            // Issue io
            let buffers: Vec<&[u8]> = blocks[issued..(issued + batch_size)]
                .iter()
                .map(|b| b.as_ref())
                .collect();
            let run_results = vio.write_blocks(&buffers, batch_start * BLOCK_SIZE as u64);
            issued += batch_size;

            if let Ok(run_results) = run_results {
                for r in run_results {
                    results.push(r.map_err(|_| Self::bad_write()));
                }
            } else {
                // Error everything
                for _ in 0..batch_size {
                    results.push(Err(Self::bad_write()));
                }
            }
        }

        Ok(results)
    }

    fn read_blocks_sync(&self, io_block_pool: &mut BufferPool, blocks: &[u64], handler: &mut dyn ReadHandler) {
        Self::read_many_((&self.file).into(), io_block_pool, blocks, handler);
        /*if let Ok(rblocks) = Self::read_many_((&self.file).into(), blocks)
            for (rb, bn) in rblocks.into_iter().zip(blocks) {
                if let Ok(rb) = rb {
                    handler.handle(rb.loc, Ok(rb.get_data()));
                } else {
                    handler.handle(*bn, Err(rb.unwrap_err()));
                }
            }
        } else {
            for bn in blocks {
                handler.handle(*bn, Err(std::io::Error::from_raw_os_error(-5)));
            }
        }*/
    }
}

impl IoEngine for SyncIoEngine {
    fn get_nr_blocks(&self) -> u64 {
        self.nr_blocks
    }

    fn get_batch_size(&self) -> usize {
        32 // could be up to libc::UIO_MAXIOV
    }

    fn read(&self, loc: u64) -> Result<Block> {
        let b = Block::new(loc);
        self.file
            .read_exact_at(b.get_data(), b.loc * BLOCK_SIZE as u64)?;
        Ok(b)
    }

    fn read_many(&self, blocks: &[u64]) -> Result<Vec<Result<Block>>> {
        let mut bs = Vec::with_capacity(blocks.len());
        for b in blocks {
            bs.push(self.read(*b));
        }
        Ok(bs)
    }

    fn write(&self, b: &Block) -> Result<()> {
        self.file
            .write_all_at(b.get_data(), b.loc * BLOCK_SIZE as u64)?;
        Ok(())
    }

    fn write_many(&self, blocks: &[Block]) -> Result<Vec<Result<()>>> {
        Self::write_many_((&self.file).into(), blocks)
    }

    fn read_blocks(
        &self,
        io_block_pool: &mut BufferPool,
        blocks: &mut dyn Iterator<Item = u64>,
        handler: &mut dyn ReadHandler,
    ) -> io::Result<()> {
        let mut chunk = Vec::new();
        for b in blocks {
            chunk.push(b);
            if chunk.len() == 512 {
                self.read_blocks_sync(io_block_pool, &chunk, handler);
                chunk.clear();
            }
        }
        self.read_blocks_sync(io_block_pool, &chunk, handler);
        handler.complete();
        Ok(())
    }
}

// Simplified version of generate_runs() without gaps. It should be a bit faster
// since multiple iterations are not required.
fn find_runs_nogap(blocks: &[Block], max_len: usize) -> Vec<(u64, usize)> {
    if blocks.is_empty() {
        return Vec::new();
    }

    let mut begin = blocks[0].loc;
    let mut len = 1usize;

    if blocks.len() == 1 {
        return vec![(begin, len)];
    }

    let mut runs = Vec::new();

    for b in &blocks[1..] {
        if b.loc == begin + len as u64 && len < max_len {
            len += 1;
        } else {
            runs.push((begin, len));
            begin = b.loc;
            len = 1;
        }
    }

    runs.push((begin, len));

    runs
}

//------------------------------------------
