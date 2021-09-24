extern crate clap;
extern crate thinp;

use clap::{App, Arg};
use std::path::Path;
use std::process;
use thinp::era::invalidate::{invalidate, EraInvalidateOptions};
use thinp::file_utils;

//------------------------------------------

fn main() {
    let parser = App::new("era_invalidate")
        .version(thinp::version::tools_version())
        .about("List blocks that may have changed since a given era")
        // flags
        .arg(
            Arg::with_name("ASYNC_IO")
                .help("Force use of io_uring for synchronous io")
                .long("async-io")
                .hidden(true),
        )
        // options
        .arg(
            Arg::with_name("OUTPUT")
                .help("Specify the output file rather than stdout")
                .short("o")
                .long("output")
                .value_name("FILE"),
        )
        // arguments
        .arg(
            Arg::with_name("INPUT")
                .help("Specify the input device to dump")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("WRITTEN_SINCE")
                .help("Blocks written since the given era will be listed")
                .long("written-since")
                .required(true)
                .value_name("ERA"),
        );

    let matches = parser.get_matches();
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let output_file = if matches.is_present("OUTPUT") {
        Some(Path::new(matches.value_of("OUTPUT").unwrap()))
    } else {
        None
    };

    if let Err(e) = file_utils::is_file_or_blk(input_file) {
        eprintln!("Invalid input file '{}': {}.", input_file.display(), e);
        process::exit(1);
    }

    let threshold = matches
        .value_of("WRITTEN_SINCE")
        .map(|s| {
            s.parse::<u32>().unwrap_or_else(|_| {
                eprintln!("Couldn't parse written_since");
                process::exit(1);
            })
        })
        .unwrap_or(0);

    let opts = EraInvalidateOptions {
        input: input_file,
        output: output_file,
        async_io: matches.is_present("ASYNC_IO"),
        threshold,
        metadata_snap: matches.is_present("METADATA_SNAP"),
    };

    if let Err(reason) = invalidate(&opts) {
        eprintln!("{}", reason);
        process::exit(1);
    }
}

//------------------------------------------
