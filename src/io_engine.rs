use io_uring::opcode;
use io_uring::types;
use io_uring::IoUring;
use safemem::write_bytes;
use std::alloc::{alloc, dealloc, Layout};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Result};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::file_utils;

//------------------------------------------

pub const BLOCK_SIZE: usize = 4096;
pub const SECTOR_SHIFT: usize = 9;
const ALIGN: usize = 4096;

#[derive(Debug)]
pub struct Block {
    pub loc: u64,
    data: *mut u8,
}

impl Block {
    // Creates a new block that corresponds to the given location.  The
    // memory is not initialised.
    pub fn new(loc: u64) -> Block {
        let layout = Layout::from_size_align(BLOCK_SIZE, ALIGN).unwrap();
        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null(), "out of memory");
        Block { loc, data: ptr }
    }

    pub fn zeroed(loc: u64) -> Block {
        let r = Self::new(loc);
        write_bytes(r.get_data(), 0);
        r
    }

    pub fn get_data<'a>(&self) -> &'a mut [u8] {
        unsafe { std::slice::from_raw_parts_mut::<'a>(self.data, BLOCK_SIZE) }
    }

    pub fn zero(&mut self) {
        unsafe {
            std::ptr::write_bytes(self.data, 0, BLOCK_SIZE);
        }
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(BLOCK_SIZE, ALIGN).unwrap();
        unsafe {
            dealloc(self.data, layout);
        }
    }
}

unsafe impl Send for Block {}

//------------------------------------------

pub trait IoEngine {
    fn get_nr_blocks(&self) -> u64;
    fn get_batch_size(&self) -> usize;
    fn get_read_counts(&self) -> u64;

    fn read(&self, b: u64) -> Result<Block>;
    // The whole io could fail, or individual blocks
    fn read_many(&self, blocks: &[u64]) -> Result<Vec<Result<Block>>>;

    fn write(&self, block: &Block) -> Result<()>;
    // The whole io could fail, or individual blocks
    fn write_many(&self, blocks: &[Block]) -> Result<Vec<Result<()>>>;
}

fn get_nr_blocks(path: &Path) -> io::Result<u64> {
    Ok(file_utils::file_size(path)? / (BLOCK_SIZE as u64))
}

//------------------------------------------

pub struct SyncIoEngine {
    nr_blocks: u64,
    file: File,
    nr_reads: AtomicU64,
}

impl SyncIoEngine {
    fn open_file(path: &Path, writable: bool, excl: bool) -> Result<File> {
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

    pub fn new(path: &Path, writable: bool) -> Result<SyncIoEngine> {
        SyncIoEngine::new_with(path, writable, true)
    }

    pub fn new_with(path: &Path, writable: bool, excl: bool) -> Result<SyncIoEngine> {
        let nr_blocks = get_nr_blocks(path)?; // check file mode before opening it
        let file = SyncIoEngine::open_file(path, writable, excl)?;

        Ok(SyncIoEngine {
            nr_blocks,
            file,
            nr_reads: AtomicU64::new(0),
        })
    }
}

impl IoEngine for SyncIoEngine {
    fn get_nr_blocks(&self) -> u64 {
        self.nr_blocks
    }

    fn get_batch_size(&self) -> usize {
        1
    }

    fn get_read_counts(&self) -> u64 {
        self.nr_reads.load(Ordering::Relaxed)
    }

    fn read(&self, loc: u64) -> Result<Block> {
        let b = Block::new(loc);
        self.file
            .read_exact_at(b.get_data(), b.loc * BLOCK_SIZE as u64)?;
        self.nr_reads.fetch_add(1, Ordering::Relaxed);
        Ok(b)
    }

    fn read_many(&self, blocks: &[u64]) -> Result<Vec<Result<Block>>> {
        let mut bs = Vec::new();
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
        let mut bs = Vec::new();
        for b in blocks {
            bs.push(self.write(b));
        }
        Ok(bs)
    }
}

//------------------------------------------

pub struct AsyncIoEngine_ {
    queue_len: u32,
    ring: IoUring,
    nr_blocks: u64,
    fd: RawFd,
    input: Arc<File>,
}

pub struct AsyncIoEngine {
    inner: Mutex<AsyncIoEngine_>,
}

impl AsyncIoEngine {
    pub fn new(path: &Path, queue_len: u32, writable: bool) -> Result<AsyncIoEngine> {
        AsyncIoEngine::new_with(path, queue_len, writable, true)
    }

    pub fn new_with(
        path: &Path,
        queue_len: u32,
        writable: bool,
        excl: bool,
    ) -> Result<AsyncIoEngine> {
        let nr_blocks = get_nr_blocks(path)?; // check file mode earlier
        let mut flags = libc::O_DIRECT;
        if excl {
            flags |= libc::O_EXCL;
        }
        let input = OpenOptions::new()
            .read(true)
            .write(writable)
            .custom_flags(flags)
            .open(path)?;

        Ok(AsyncIoEngine {
            inner: Mutex::new(AsyncIoEngine_ {
                queue_len,
                ring: IoUring::new(queue_len)?,
                nr_blocks,
                fd: input.as_raw_fd(),
                input: Arc::new(input),
            }),
        })
    }

    // FIXME: refactor next two fns
    fn read_many_(&self, blocks: Vec<Block>) -> Result<Vec<Result<Block>>> {
        use std::io::*;

        let mut inner = self.inner.lock().unwrap();
        let count = blocks.len();
        let fd_inner = inner.input.as_raw_fd();

        for (i, b) in blocks.iter().enumerate() {
            let read_e = opcode::Read::new(types::Fd(fd_inner), b.data, BLOCK_SIZE as u32)
                .offset(b.loc as i64 * BLOCK_SIZE as i64);

            unsafe {
                inner
                    .ring
                    .submission()
                    .push(&read_e.build().user_data(i as u64))
                    .expect("queue is full");
            }
        }

        inner.ring.submit_and_wait(count)?;

        let mut cqes = inner.ring.completion().collect::<Vec<_>>();

        if cqes.len() != count {
            return Err(Error::new(
                ErrorKind::Other,
                "insufficient io_uring completions",
            ));
        }

        // reorder cqes
        cqes.sort_by(|a, b| a.user_data().partial_cmp(&b.user_data()).unwrap());

        let mut rs = Vec::new();
        for (i, b) in blocks.into_iter().enumerate() {
            let c = &cqes[i];

            let r = c.result();
            if r < 0 {
                let error = Error::from_raw_os_error(-r);
                rs.push(Err(error));
            } else if c.result() != BLOCK_SIZE as i32 {
                rs.push(Err(Error::new(ErrorKind::UnexpectedEof, "short read")));
            } else {
                rs.push(Ok(b));
            }
        }
        Ok(rs)
    }

    fn write_many_(&self, blocks: &[Block]) -> Result<Vec<Result<()>>> {
        use std::io::*;

        let mut inner = self.inner.lock().unwrap();
        let count = blocks.len();
        let fd_inner = inner.input.as_raw_fd();

        for (i, b) in blocks.iter().enumerate() {
            let write_e = opcode::Write::new(types::Fd(fd_inner), b.data, BLOCK_SIZE as u32)
                .offset(b.loc as i64 * BLOCK_SIZE as i64);

            unsafe {
                inner
                    .ring
                    .submission()
                    .push(&write_e.build().user_data(i as u64))
                    .expect("queue is full");
            }
        }

        inner.ring.submit_and_wait(count)?;

        let mut cqes = inner.ring.completion().collect::<Vec<_>>();

        // reorder cqes
        cqes.sort_by(|a, b| a.user_data().partial_cmp(&b.user_data()).unwrap());

        let mut rs = Vec::new();
        for c in cqes {
            let r = c.result();
            if r < 0 {
                let error = Error::from_raw_os_error(-r);
                rs.push(Err(error));
            } else if r != BLOCK_SIZE as i32 {
                rs.push(Err(Error::new(ErrorKind::UnexpectedEof, "short write")));
            } else {
                rs.push(Ok(()));
            }
        }
        Ok(rs)
    }
}

impl Clone for AsyncIoEngine {
    fn clone(&self) -> AsyncIoEngine {
        let inner = self.inner.lock().unwrap();
        eprintln!("in clone, queue_len = {}", inner.queue_len);
        AsyncIoEngine {
            inner: Mutex::new(AsyncIoEngine_ {
                queue_len: inner.queue_len,
                ring: IoUring::new(inner.queue_len).expect("couldn't create uring"),
                nr_blocks: inner.nr_blocks,
                fd: inner.fd,
                input: inner.input.clone(),
            }),
        }
    }
}

impl IoEngine for AsyncIoEngine {
    fn get_nr_blocks(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.nr_blocks
    }

    fn get_batch_size(&self) -> usize {
        self.inner.lock().unwrap().queue_len as usize
    }

    fn get_read_counts(&self) -> u64 {
        0 // stub
    }

    fn read(&self, b: u64) -> Result<Block> {
        let mut inner = self.inner.lock().unwrap();
        let fd = types::Fd(inner.input.as_raw_fd());
        let b = Block::new(b);
        let read_e = opcode::Read::new(fd, b.data, BLOCK_SIZE as u32)
            .offset(b.loc as i64 * BLOCK_SIZE as i64);

        unsafe {
            inner
                .ring
                .submission()
                .push(&read_e.build().user_data(0))
                .expect("queue is full");
        }

        inner.ring.submit_and_wait(1)?;

        let cqes = inner.ring.completion().collect::<Vec<_>>();

        let r = cqes[0].result();
        use std::io::*;
        if r < 0 {
            let error = Error::from_raw_os_error(-r);
            Err(error)
        } else if r != BLOCK_SIZE as i32 {
            Err(Error::new(ErrorKind::UnexpectedEof, "short write"))
        } else {
            Ok(b)
        }
    }

    fn read_many(&self, blocks: &[u64]) -> Result<Vec<Result<Block>>> {
        let inner = self.inner.lock().unwrap();
        let queue_len = inner.queue_len as usize;
        drop(inner);

        let mut results = Vec::new();
        for cs in blocks.chunks(queue_len) {
            let mut bs = Vec::new();
            for b in cs {
                bs.push(Block::new(*b));
            }

            results.append(&mut self.read_many_(bs)?);
        }

        Ok(results)
    }

    fn write(&self, b: &Block) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let fd = types::Fd(inner.input.as_raw_fd());
        let write_e = opcode::Write::new(fd, b.data, BLOCK_SIZE as u32)
            .offset(b.loc as i64 * BLOCK_SIZE as i64);

        unsafe {
            inner
                .ring
                .submission()
                .push(&write_e.build().user_data(0))
                .expect("queue is full");
        }

        inner.ring.submit_and_wait(1)?;

        let cqes = inner.ring.completion().collect::<Vec<_>>();

        let r = cqes[0].result();
        use std::io::*;
        if r < 0 {
            let error = Error::from_raw_os_error(-r);
            Err(error)
        } else if r != BLOCK_SIZE as i32 {
            Err(Error::new(ErrorKind::UnexpectedEof, "short write"))
        } else {
            Ok(())
        }
    }

    fn write_many(&self, blocks: &[Block]) -> Result<Vec<Result<()>>> {
        let inner = self.inner.lock().unwrap();
        let queue_len = inner.queue_len as usize;
        drop(inner);

        let mut results = Vec::new();
        let mut done = 0;
        while done != blocks.len() {
            let len = usize::min(blocks.len() - done, queue_len);
            results.append(&mut self.write_many_(&blocks[done..(done + len)])?);
            done += len;
        }

        Ok(results)
    }
}

//------------------------------------------
