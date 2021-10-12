use anyhow::Result;

mod common;

use common::common_args::*;
use common::process::*;
use common::program::*;
use common::target::*;

//------------------------------------------

const USAGE: &str = "Usage: cache_metadata_size [options]\n\
                     Options:\n  \
                       {-h|--help}\n  \
                       {-V|--version}\n  \
                       {--block-size <sectors>}\n  \
                       {--device-size <sectors>}\n  \
                       {--nr-blocks <natural>}\n  \
                       {--max-hint-width <nr bytes>}\n\
                     \n\
                     These all relate to the size of the fast device (eg, SSD), rather\n\
                     than the whole cached device.";

//------------------------------------------

struct CacheMetadataSize;

impl<'a> Program<'a> for CacheMetadataSize {
    fn name() -> &'a str {
        "cache_metadata_size"
    }

    fn path() -> &'a std::ffi::OsStr {
        CACHE_METADATA_SIZE.as_ref()
    }

    fn usage() -> &'a str {
        USAGE
    }

    fn arg_type() -> ArgType {
        ArgType::InputArg
    }

    fn bad_option_hint(option: &str) -> String {
        msg::bad_option_hint(option)
    }
}

//------------------------------------------

test_accepts_help!(CacheMetadataSize);
test_accepts_version!(CacheMetadataSize);
test_rejects_bad_option!(CacheMetadataSize);

//------------------------------------------

#[test]
fn no_args() -> Result<()> {
    let _stderr = run_fail(CACHE_METADATA_SIZE, [""; 0])?;
    #[cfg(not(feature = "rust_tests"))]
    assert_eq!(
        _stderr,
        "Please specify either --device-size and --block-size, or --nr-blocks."
    );
    Ok(())
}

#[test]
fn device_size_only() -> Result<()> {
    let _stderr = run_fail(CACHE_METADATA_SIZE, args!["--device-size", "204800"])?;
    #[cfg(not(feature = "rust_tests"))]
    assert_eq!(
        _stderr,
        "If you specify --device-size you must also give --block-size."
    );
    Ok(())
}

#[test]
fn block_size_only() -> Result<()> {
    let _stderr = run_fail(CACHE_METADATA_SIZE, args!["--block-size", "128"])?;
    #[cfg(not(feature = "rust_tests"))]
    assert_eq!(
        _stderr,
        "If you specify --block-size you must also give --device-size."
    );
    Ok(())
}

#[test]
#[cfg(not(feature = "rust_tests"))]
fn conradictory_info_fails() -> Result<()> {
    let stderr = run_fail(
        CACHE_METADATA_SIZE,
        args![
            "--device-size",
            "102400",
            "--block-size",
            "1000",
            "--nr-blocks",
            "6"
        ],
    )?;
    assert_eq!(stderr, "Contradictory arguments given, --nr-blocks doesn't match the --device-size and --block-size.");
    Ok(())
}

#[test]
#[cfg(not(feature = "rust_tests"))]
fn all_args_agree() -> Result<()> {
    let out = run_ok_raw(
        CACHE_METADATA_SIZE,
        args![
            "--device-size",
            "102400",
            "--block-size",
            "100",
            "--nr-blocks",
            "1024"
        ],
    )?;
    let stdout = std::str::from_utf8(&out.stdout[..])
        .unwrap()
        .trim_end_matches(|c| c == '\n' || c == '\r')
        .to_string();
    assert_eq!(stdout, "8248 sectors");
    assert_eq!(out.stderr.len(), 0);
    Ok(())
}

#[test]
#[cfg(feature = "rust_tests")]
fn dev_size_and_nr_blocks_conflicts() -> Result<()> {
    run_fail(
        CACHE_METADATA_SIZE,
        args!["--device-size", "102400", "--nr-blocks", "1024"],
    )?;
    Ok(())
}

#[test]
#[cfg(feature = "rust_tests")]
fn block_size_and_nr_blocks_conflicts() -> Result<()> {
    run_fail(
        CACHE_METADATA_SIZE,
        args!["--block-size", "100", "--nr-blocks", "1024"],
    )?;
    Ok(())
}

#[test]
fn nr_blocks_alone() -> Result<()> {
    let out = run_ok_raw(CACHE_METADATA_SIZE, args!["--nr-blocks", "1024"])?;
    let stdout = std::str::from_utf8(&out.stdout[..])
        .unwrap()
        .trim_end_matches(|c| c == '\n' || c == '\r')
        .to_string();
    assert_eq!(stdout, "8248 sectors");
    assert_eq!(out.stderr.len(), 0);
    Ok(())
}

#[test]
fn dev_size_and_block_size_succeeds() -> Result<()> {
    let out = run_ok_raw(
        CACHE_METADATA_SIZE,
        args!["--device-size", "102400", "--block-size", "100"],
    )?;
    let stdout = std::str::from_utf8(&out.stdout[..])
        .unwrap()
        .trim_end_matches(|c| c == '\n' || c == '\r')
        .to_string();
    assert_eq!(stdout, "8248 sectors");
    assert_eq!(out.stderr.len(), 0);
    Ok(())
}

#[test]
fn large_nr_blocks() -> Result<()> {
    let out = run_ok_raw(CACHE_METADATA_SIZE, args!["--nr-blocks", "67108864"])?;
    let stdout = std::str::from_utf8(&out.stdout[..])
        .unwrap()
        .trim_end_matches(|c| c == '\n' || c == '\r')
        .to_string();
    assert_eq!(stdout, "3678208 sectors");
    assert_eq!(out.stderr.len(), 0);
    Ok(())
}

//------------------------------------------
