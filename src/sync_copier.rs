use std::io;
use std::path::Path;
use std::sync::Arc;

use crate::copier::CopyOp;
use crate::io_engine::*;

#[derive(Clone)]
pub struct SyncCopier {
    input: Arc<SyncIoEngine>,
    output: Arc<SyncIoEngine>,
}

// FIXME: used byte-based units
impl SyncCopier {
    pub fn new(
        src: &Path,
        dest: &Path,
        block_size: u32,
        src_offset: u64,
        dest_offset: u64,
    ) -> io::Result<SyncCopier> {
        if block_size > (u32::MAX >> SECTOR_SHIFT) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "block size out of bounds",
            ));
        }

        let bs = (block_size << SECTOR_SHIFT) as usize;
        let input = Arc::new(SyncIoEngine::with_offset(
            src,
            bs,
            src_offset << SECTOR_SHIFT,
            16,
            false,
            true,
        )?);
        let output = Arc::new(SyncIoEngine::with_offset(
            dest,
            bs,
            dest_offset << SECTOR_SHIFT,
            16,
            true,
            true,
        )?);

        Ok(SyncCopier { input, output })
    }

    pub fn copy(&self, op: CopyOp) -> io::Result<()> {
        let _b = self.input.read(op.src_b)?;
        //self.output.write(&b)
        Ok(())
    }
}
