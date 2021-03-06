use crate::common::thin_xml_generator::{write_xml, FragmentedS};
use crate::common::*;

//------------------------------------------
// wrappers

type ArgsBuilder = fn(&mut TestDir, &str, &dyn Fn(&[&str]) -> Result<()>) -> Result<()>;

fn with_output_md_untouched(
    td: &mut TestDir,
    input: &str,
    thunk: &dyn Fn(&[&str]) -> Result<()>,
) -> Result<()> {
    let output = mk_zeroed_md(td)?;
    ensure_untouched(&output, || {
        let args = ["-i", input, "-o", output.to_str().unwrap()];
        thunk(&args)
    })
}

fn with_output_superblock_zeroed(
    td: &mut TestDir,
    input: &str,
    thunk: &dyn Fn(&[&str]) -> Result<()>,
) -> Result<()> {
    let output = mk_zeroed_md(td)?;
    ensure_superblock_zeroed(&output, || {
        let args = ["-i", input, "-o", output.to_str().unwrap()];
        thunk(&args)
    })
}

fn input_arg_only(
    _td: &mut TestDir,
    input: &str,
    thunk: &dyn Fn(&[&str]) -> Result<()>,
) -> Result<()> {
    let args = [input];
    thunk(&args)
}

fn build_args_fn(t: ArgType) -> Result<ArgsBuilder> {
    match t {
        ArgType::InputArg => Ok(input_arg_only),
        ArgType::IoOptions => Ok(with_output_md_untouched),
    }
}

//------------------------------------------
// test invalid arguments

pub fn test_missing_input_arg<'a, P>() -> Result<()>
where
    P: InputProgram<'a>,
{
    let stderr = run_fail(P::path(), &[])?;
    assert!(stderr.contains(P::missing_input_arg()));
    Ok(())
}

#[macro_export]
macro_rules! test_missing_input_arg {
    ($program: ident) => {
        #[test]
        fn missing_input_arg() -> Result<()> {
            test_missing_input_arg::<$program>()
        }
    };
}

pub fn test_missing_input_option<'a, P>() -> Result<()>
where
    P: InputProgram<'a>,
{
    let mut td = TestDir::new()?;
    let output = mk_zeroed_md(&mut td)?;
    ensure_untouched(&output, || {
        let args = ["-o", output.to_str().unwrap()];
        let stderr = run_fail(P::path(), &args)?;
        assert!(stderr.contains(P::missing_input_arg()));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_missing_input_option {
    ($program: ident) => {
        #[test]
        fn missing_input_option() -> Result<()> {
            test_missing_input_option::<$program>()
        }
    };
}

pub fn test_input_file_not_found<'a, P>() -> Result<()>
where
    P: InputProgram<'a>,
{
    let mut td = TestDir::new()?;

    let wrapper = build_args_fn(P::arg_type())?;
    wrapper(&mut td, "no-such-file", &|args: &[&str]| {
        let stderr = run_fail(P::path(), args)?;
        assert!(stderr.contains(P::file_not_found()));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_input_file_not_found {
    ($program: ident) => {
        #[test]
        fn input_file_not_found() -> Result<()> {
            test_input_file_not_found::<$program>()
        }
    };
}

pub fn test_input_cannot_be_a_directory<'a, P>() -> Result<()>
where
    P: InputProgram<'a>,
{
    let mut td = TestDir::new()?;

    let wrapper = build_args_fn(P::arg_type())?;
    wrapper(&mut td, "/tmp", &|args: &[&str]| {
        let stderr = run_fail(P::path(), args)?;
        assert!(stderr.contains("Not a block device or regular file"));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_input_cannot_be_a_directory {
    ($program: ident) => {
        #[test]
        fn input_cannot_be_a_directory() -> Result<()> {
            test_input_cannot_be_a_directory::<$program>()
        }
    };
}

pub fn test_unreadable_input_file<'a, P>() -> Result<()>
where
    P: InputProgram<'a>,
{
    let mut td = TestDir::new()?;

    // input an unreadable file
    let input = mk_valid_md(&mut td)?;
    duct::cmd!("chmod", "-r", &input).run()?;

    let wrapper = build_args_fn(P::arg_type())?;
    wrapper(&mut td, input.to_str().unwrap(), &|args: &[&str]| {
        let stderr = run_fail(P::path(), args)?;
        assert!(stderr.contains("Permission denied"));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_unreadable_input_file {
    ($program: ident) => {
        #[test]
        fn unreadable_input_file() -> Result<()> {
            test_unreadable_input_file::<$program>()
        }
    };
}

//------------------------------------------
// test invalid content

pub fn test_help_message_for_tiny_input_file<'a, P>() -> Result<()>
where
    P: BinaryInputProgram<'a>,
{
    let mut td = TestDir::new()?;

    let input = td.mk_path("meta.bin");
    file_utils::create_sized_file(&input, 1024)?;

    let wrapper = build_args_fn(P::arg_type())?;
    wrapper(&mut td, input.to_str().unwrap(), &|args: &[&str]| {
        let stderr = run_fail(P::path(), args)?;
        assert!(stderr.contains("Metadata device/file too small.  Is this binary metadata?"));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_help_message_for_tiny_input_file {
    ($program: ident) => {
        #[test]
        fn prints_help_message_for_tiny_input_file() -> Result<()> {
            test_help_message_for_tiny_input_file::<$program>()
        }
    };
}

pub fn test_spot_xml_data<'a, P>() -> Result<()>
where
    P: BinaryInputProgram<'a>,
{
    let mut td = TestDir::new()?;

    // input a large xml file
    let input = td.mk_path("meta.xml");
    let mut gen = FragmentedS::new(4, 10240);
    write_xml(&input, &mut gen)?;

    let wrapper = build_args_fn(P::arg_type())?;
    wrapper(&mut td, input.to_str().unwrap(), &|args: &[&str]| {
        let stderr = run_fail(P::path(), args)?;
        eprintln!("{}", stderr);
        let msg = format!(
            "This looks like XML.  {} only checks the binary metadata format.",
            P::name()
        );
        assert!(stderr.contains(&msg));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_spot_xml_data {
    ($program: ident) => {
        #[test]
        fn spot_xml_data() -> Result<()> {
            test_spot_xml_data::<$program>()
        }
    };
}

pub fn test_corrupted_input_data<'a, P>() -> Result<()>
where
    P: InputProgram<'a>,
{
    let mut td = TestDir::new()?;
    let input = mk_zeroed_md(&mut td)?;

    let wrapper = match P::arg_type() {
        ArgType::InputArg => input_arg_only,
        ArgType::IoOptions => with_output_superblock_zeroed,
    };
    wrapper(&mut td, input.to_str().unwrap(), &|args: &[&str]| {
        let stderr = run_fail(P::path(), args)?;
        assert!(stderr.contains(P::corrupted_input()));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_corrupted_input_data {
    ($program: ident) => {
        #[test]
        fn corrupted_input_data() -> Result<()> {
            test_corrupted_input_data::<$program>()
        }
    };
}

//------------------------------------------
