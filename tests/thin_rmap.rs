use anyhow::Result;

mod common;

use common::common_args::*;
use common::test_dir::*;
use common::*;

//------------------------------------------

const USAGE: &str = "Usage: thin_rmap [options] {device|file}\n\
                     Options:\n  \
                       {-h|--help}\n  \
                       {-V|--version}\n  \
                       {--region <block range>}*\n\
                     Where:\n  \
                       <block range> is of the form <begin>..<one-past-the-end>\n  \
                       for example 5..45 denotes blocks 5 to 44 inclusive, but not block 45";

//------------------------------------------

struct ThinRmap;

impl<'a> Program<'a> for ThinRmap {
    fn name() -> &'a str {
        "thin_rmap"
    }

    fn path() -> &'a str {
        THIN_RMAP
    }

    fn usage() -> &'a str {
        USAGE
    }

    fn arg_type() -> ArgType {
        ArgType::InputArg
    }

    fn bad_option_hint(option: &str) -> String {
        cpp_msg::bad_option_hint(option)
    }
}

//------------------------------------------

test_accepts_help!(ThinRmap);
test_accepts_version!(ThinRmap);
test_rejects_bad_option!(ThinRmap);

//------------------------------------------

#[test]
fn valid_region_format_should_pass() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let md_path = md.to_str().unwrap();
    run_ok(THIN_RMAP, &["--region", "23..7890", md_path])?;
    Ok(())
}

#[test]
fn invalid_regions_should_fail() -> Result<()> {
    let invalid_regions = [
        "23,7890",
        "23..six",
        "found..7890",
        "89..88",
        "89..89",
        "89..",
        "",
        "89...99",
    ];
    for r in &invalid_regions {
        let mut td = TestDir::new()?;
        let md = mk_valid_md(&mut td)?;
        run_fail(THIN_RMAP, &[&r.to_string(), md.to_str().unwrap()])?;
    }
    Ok(())
}

#[test]
fn multiple_regions_should_pass() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    run_ok(
        THIN_RMAP,
        &[
            "--region",
            "1..23",
            "--region",
            "45..78",
            md.to_str().unwrap(),
        ],
    )?;
    Ok(())
}

#[test]
fn junk_input() -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = mk_valid_xml(&mut td)?;
    run_fail(THIN_RMAP, &["--region", "0..-1", xml.to_str().unwrap()])?;
    Ok(())
}

//------------------------------------------
