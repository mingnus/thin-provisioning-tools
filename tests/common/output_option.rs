use crate::common::*;

//-----------------------------------------
// test invalid arguments

pub fn test_missing_output_option<'a, P>() -> Result<()>
where
    P: OutputProgram<'a>,
{
    let mut td = TestDir::new()?;
    let input = P::mk_valid_input(&mut td)?;
    let stderr = run_fail(P::path(), &["-i", input.to_str().unwrap()])?;
    assert!(stderr.contains(P::missing_output_arg()));
    Ok(())
}

#[macro_export]
macro_rules! test_missing_output_option {
    ($program: ident) => {
        #[test]
        fn missing_output_option() -> Result<()> {
            test_missing_output_option::<$program>()
        }
    };
}

pub fn test_output_file_not_found<'a, P>() -> Result<()>
where
    P: OutputProgram<'a>,
{
    let mut td = TestDir::new()?;
    let input = P::mk_valid_input(&mut td)?;
    let stderr = run_fail(
        P::path(),
        &["-i", input.to_str().unwrap(), "-o", "no-such-file"],
    )?;
    assert!(stderr.contains(<P as OutputProgram>::file_not_found()));
    Ok(())
}

#[macro_export]
macro_rules! test_output_file_not_found {
    ($program: ident) => {
        #[test]
        fn output_file_not_found() -> Result<()> {
            test_output_file_not_found::<$program>()
        }
    };
}

pub fn test_output_cannot_be_a_directory<'a, P>() -> Result<()>
where
    P: OutputProgram<'a>,
{
    let mut td = TestDir::new()?;
    let input = P::mk_valid_input(&mut td)?;
    let stderr = run_fail(P::path(), &["-i", input.to_str().unwrap(), "-o", "/tmp"])?;
    assert!(stderr.contains("Not a block device or regular file"));
    Ok(())
}

#[macro_export]
macro_rules! test_output_cannot_be_a_directory {
    ($program: ident) => {
        #[test]
        fn output_cannot_be_a_directory() -> Result<()> {
            test_output_cannot_be_a_directory::<$program>()
        }
    };
}

pub fn test_unwritable_output_file<'a, P>() -> Result<()>
where
    P: OutputProgram<'a>,
{
    let mut td = TestDir::new()?;
    let input = P::mk_valid_input(&mut td)?;

    let output = td.mk_path("meta.bin");
    let _file = file_utils::create_sized_file(&output, 4096);
    duct::cmd!("chmod", "-w", &output).run()?;

    let stderr = run_fail(
        P::path(),
        &[
            "-i",
            input.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
        ],
    )?;
    assert!(stderr.contains("Permission denied"));
    Ok(())
}

#[macro_export]
macro_rules! test_unwritable_output_file {
    ($program: ident) => {
        #[test]
        fn unwritable_output_file() -> Result<()> {
            test_unwritable_output_file::<$program>()
        }
    };
}

//----------------------------------------
// test invalid content

// currently thin/cache_restore only
pub fn test_tiny_output_file<'a, P>() -> Result<()>
where
    P: BinaryOutputProgram<'a>,
{
    let mut td = TestDir::new()?;
    let input = P::mk_valid_input(&mut td)?;

    let output = td.mk_path("meta.bin");
    let _file = file_utils::create_sized_file(&output, 4096);

    let stderr = run_fail(
        P::path(),
        &[
            "-i",
            input.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
        ],
    )?;
    assert!(stderr.contains("Output file too small"));
    Ok(())
}

#[macro_export]
macro_rules! test_tiny_output_file {
    ($program: ident) => {
        #[test]
        fn tiny_output_file() -> Result<()> {
            test_tiny_output_file::<$program>()
        }
    };
}

//-----------------------------------------
