extern crate clap;
extern crate thinp;

use atty::Stream;
use clap::{App, Arg};
use std::path::Path;
use std::process;
use std::sync::Arc;

use thinp::era::check::{check, EraCheckOptions};
use thinp::file_utils;
use thinp::report::*;

//------------------------------------------

fn main() {
    let parser = App::new("era_check")
        .version(thinp::version::tools_version())
        // flags
        .arg(
            Arg::with_name("ASYNC_IO")
                .help("Force use of io_uring for synchronous io")
                .long("async-io")
                .hidden(true),
        )
        .arg(
            Arg::with_name("AUTO_REPAIR")
                .help("Auto repair trivial issues.")
                .long("auto-repair"),
        )
        .arg(
            Arg::with_name("IGNORE_NON_FATAL")
                .help("Only return a non-zero exit code if a fatal error is found.")
                .long("ignore-non-fatal-errors"),
        )
        .arg(
            Arg::with_name("QUIET")
                .help("Suppress output messages, return only exit code.")
                .short("q")
                .long("quiet"),
        )
        .arg(
            Arg::with_name("SB_ONLY")
                .help("Only check the superblock.")
                .long("super-block-only"),
        )
        // arguments
        .arg(
            Arg::with_name("INPUT")
                .help("Specify the input device to check")
                .required(true)
                .index(1),
        );

    let matches = parser.get_matches();
    let input_file = Path::new(matches.value_of("INPUT").unwrap());

    if let Err(e) = file_utils::is_file_or_blk(input_file) {
        eprintln!("Invalid input file '{}': {}.", input_file.display(), e);
        process::exit(1);
    }

    let report;
    if matches.is_present("QUIET") {
        report = std::sync::Arc::new(mk_quiet_report());
    } else if atty::is(Stream::Stdout) {
        report = std::sync::Arc::new(mk_progress_bar_report());
    } else {
        report = Arc::new(mk_simple_report());
    }

    let opts = EraCheckOptions {
        dev: &input_file,
        async_io: matches.is_present("ASYNC_IO"),
        sb_only: matches.is_present("SB_ONLY"),
        ignore_non_fatal: matches.is_present("IGNORE_NON_FATAL"),
        auto_repair: matches.is_present("AUTO_REPAIR"),
        report: report.clone(),
    };

    if let Err(reason) = check(&opts) {
        report.fatal(&format!("{}", reason));
        process::exit(1);
    }
}

//------------------------------------------
