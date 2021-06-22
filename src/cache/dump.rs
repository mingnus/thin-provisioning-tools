use anyhow::anyhow;
use fixedbitset::FixedBitSet;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::cache::hint::Hint;
use crate::cache::mapping::Mapping;
use crate::cache::superblock::*;
use crate::cache::xml::{self, MetadataVisitor};
use crate::io_engine::{AsyncIoEngine, IoEngine, SyncIoEngine};
use crate::pdata::array::{self, ArrayBlock};
use crate::pdata::array_walker::*;
use crate::pdata::bitset::{read_bitset, CheckedBitSet};

//------------------------------------------

const MAX_CONCURRENT_IO: u32 = 1024;

//------------------------------------------

mod format1 {
    use super::*;

    struct Inner<'a> {
        visitor: &'a mut dyn MetadataVisitor,
        valid_mappings: FixedBitSet,
    }

    pub struct MappingEmitter<'a> {
        inner: Mutex<Inner<'a>>,
    }

    impl<'a> MappingEmitter<'a> {
        pub fn new(nr_entries: usize, visitor: &'a mut dyn MetadataVisitor) -> MappingEmitter<'a> {
            MappingEmitter {
                inner: Mutex::new(Inner {
                    visitor,
                    valid_mappings: FixedBitSet::with_capacity(nr_entries),
                }),
            }
        }

        pub fn get_valid(self) -> FixedBitSet {
            let inner = self.inner.into_inner().unwrap();
            inner.valid_mappings
        }
    }

    impl<'a> ArrayVisitor<Mapping> for MappingEmitter<'a> {
        fn visit(&self, index: u64, b: ArrayBlock<Mapping>) -> array::Result<()> {
            let cbegin = index as u32 * b.header.max_entries;
            let cend = cbegin + b.header.nr_entries;
            for (map, cblock) in b.values.iter().zip(cbegin..cend) {
                if !map.is_valid() {
                    continue;
                }

                let m = xml::Map {
                    cblock,
                    oblock: map.oblock,
                    dirty: map.is_dirty(),
                };

                let mut inner = self.inner.lock().unwrap();
                inner.valid_mappings.set(cblock as usize, true);
                inner
                    .visitor
                    .mapping(&m)
                    .map_err(|e| array::value_err(format!("{}", e)))?;
            }

            Ok(())
        }
    }
}

//------------------------------------------

mod format2 {
    use super::*;

    //-------------------
    // Mapping visitor

    struct Inner<'a> {
        visitor: &'a mut dyn MetadataVisitor,
        dirty_bits: CheckedBitSet,
        valid_mappings: FixedBitSet,
    }

    pub struct MappingEmitter<'a> {
        inner: Mutex<Inner<'a>>,
    }

    impl<'a> MappingEmitter<'a> {
        pub fn new(
            nr_entries: usize,
            dirty_bits: CheckedBitSet,
            visitor: &'a mut dyn MetadataVisitor,
        ) -> MappingEmitter<'a> {
            MappingEmitter {
                inner: Mutex::new(Inner {
                    visitor,
                    dirty_bits,
                    valid_mappings: FixedBitSet::with_capacity(nr_entries),
                }),
            }
        }

        pub fn get_valid(self) -> FixedBitSet {
            let inner = self.inner.into_inner().unwrap();
            inner.valid_mappings
        }
    }

    impl<'a> ArrayVisitor<Mapping> for MappingEmitter<'a> {
        fn visit(&self, index: u64, b: ArrayBlock<Mapping>) -> array::Result<()> {
            let cbegin = index as u32 * b.header.max_entries;
            let cend = cbegin + b.header.nr_entries;
            for (map, cblock) in b.values.iter().zip(cbegin..cend) {
                if !map.is_valid() {
                    continue;
                }

                let mut inner = self.inner.lock().unwrap();
                let dirty;
                if let Some(bit) = inner.dirty_bits.contains(cblock as usize) {
                    dirty = bit;
                } else {
                    // default to dirty if the bitset is damaged
                    dirty = true;
                }
                let m = xml::Map {
                    cblock,
                    oblock: map.oblock,
                    dirty,
                };

                inner.valid_mappings.set(cblock as usize, true);
                inner
                    .visitor
                    .mapping(&m)
                    .map_err(|e| array::value_err(format!("{}", e)))?;
            }
            Ok(())
        }
    }
}

//-----------------------------------------

struct HintEmitter<'a> {
    emitter: Mutex<&'a mut dyn MetadataVisitor>,
    valid_mappings: FixedBitSet,
}

impl<'a> HintEmitter<'a> {
    pub fn new(emitter: &'a mut dyn MetadataVisitor, valid_mappings: FixedBitSet) -> HintEmitter {
        HintEmitter {
            emitter: Mutex::new(emitter),
            valid_mappings,
        }
    }
}

impl<'a> ArrayVisitor<Hint> for HintEmitter<'a> {
    fn visit(&self, index: u64, b: ArrayBlock<Hint>) -> array::Result<()> {
        let cbegin = index as u32 * b.header.max_entries;
        let cend = cbegin + b.header.nr_entries;
        for (hint, cblock) in b.values.iter().zip(cbegin..cend) {
            if !self.valid_mappings.contains(cblock as usize) {
                continue;
            }

            let h = xml::Hint {
                cblock,
                data: hint.hint.to_vec(),
            };

            self.emitter
                .lock()
                .unwrap()
                .hint(&h)
                .map_err(|e| array::value_err(format!("{}", e)))?;
        }

        Ok(())
    }
}

//------------------------------------------

pub struct CacheDumpOptions<'a> {
    pub input: &'a Path,
    pub output: Option<&'a Path>,
    pub async_io: bool,
    pub repair: bool,
}

struct Context {
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &CacheDumpOptions) -> anyhow::Result<Context> {
    let engine: Arc<dyn IoEngine + Send + Sync>;

    if opts.async_io {
        engine = Arc::new(AsyncIoEngine::new(opts.input, MAX_CONCURRENT_IO, false)?);
    } else {
        let nr_threads = std::cmp::max(8, num_cpus::get() * 2);
        engine = Arc::new(SyncIoEngine::new(opts.input, nr_threads, false)?);
    }

    Ok(Context { engine })
}

fn dump_metadata(
    ctx: &Context,
    w: &mut dyn Write,
    sb: &Superblock,
    _repair: bool,
) -> anyhow::Result<()> {
    let engine = &ctx.engine;

    let mut out = xml::XmlWriter::new(w);
    let xml_sb = xml::Superblock {
        uuid: "".to_string(),
        block_size: sb.data_block_size,
        nr_cache_blocks: sb.cache_blocks,
        policy: std::str::from_utf8(&sb.policy_name)?.to_string(),
        hint_width: sb.policy_hint_size,
    };
    out.superblock_b(&xml_sb)?;

    out.mappings_b()?;
    let valid_mappings = match sb.version {
        1 => {
            let w = ArrayWalker::new(engine.clone(), false);
            let mut emitter = format1::MappingEmitter::new(sb.cache_blocks as usize, &mut out);
            w.walk(&mut emitter, sb.mapping_root)?;
            emitter.get_valid()
        }
        2 => {
            // We need to walk the dirty bitset first.
            let dirty_bits;
            if let Some(root) = sb.dirty_root {
                let (bits, errs) =
                    read_bitset(engine.clone(), root, sb.cache_blocks as usize, false);
                // TODO: allow errors in repair mode
                if errs.is_some() {
                    return Err(anyhow!("errors in bitset {}", errs.unwrap()));
                }
                dirty_bits = bits;
            } else {
                // FIXME: is there a way this can legally happen?  eg,
                // a crash of a freshly created cache?
                return Err(anyhow!("format 2 selected, but no dirty bitset present"));
            }

            let w = ArrayWalker::new(engine.clone(), false);
            let mut emitter =
                format2::MappingEmitter::new(sb.cache_blocks as usize, dirty_bits, &mut out);
            w.walk(&mut emitter, sb.mapping_root)?;
            emitter.get_valid()
        }
        v => {
            return Err(anyhow!("unsupported metadata version: {}", v));
        }
    };
    out.mappings_e()?;

    out.hints_b()?;
    {
        let w = ArrayWalker::new(engine.clone(), false);
        let mut emitter = HintEmitter::new(&mut out, valid_mappings);
        w.walk(&mut emitter, sb.hint_root)?;
    }
    out.hints_e()?;

    out.superblock_e()?;
    out.eof()?;

    Ok(())
}

pub fn dump(opts: CacheDumpOptions) -> anyhow::Result<()> {
    let ctx = mk_context(&opts)?;
    let engine = &ctx.engine;
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;

    let mut writer: Box<dyn Write>;
    if opts.output.is_some() {
        writer = Box::new(BufWriter::new(File::create(opts.output.unwrap())?));
    } else {
        writer = Box::new(BufWriter::new(std::io::stdout()));
    }

    dump_metadata(&ctx, &mut writer, &sb, opts.repair)
}

//------------------------------------------
