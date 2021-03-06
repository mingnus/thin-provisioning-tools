extern crate clap;
extern crate thinp;

use clap::{App, Arg};
use std::path::Path;
use thinp::cache::dump::{dump, CacheDumpOptions};

//------------------------------------------

fn main() {
    let parser = App::new("cache_dump")
        .version(thinp::version::tools_version())
        .about("Dump the cache metadata to stdout in XML format")
        // flags
        .arg(
            Arg::with_name("ASYNC_IO")
                .help("Force use of io_uring for synchronous io")
                .long("async-io")
                .hidden(true),
        )
        .arg(
            Arg::with_name("REPAIR")
                .help("Repair the metadata whilst dumping it")
                .short("r")
                .long("repair"),
        )
        // options
        .arg(
            Arg::with_name("OUTPUT")
                .help("Specify the output file rather than stdout")
                .short("o")
                .long("output")
                .value_name("OUTPUT"),
        )
        // arguments
        .arg(
            Arg::with_name("INPUT")
                .help("Specify the input device to dump")
                .required(true)
                .index(1),
        );

    let matches = parser.get_matches();
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let output_file = if matches.is_present("OUTPUT") {
        Some(Path::new(matches.value_of("OUTPUT").unwrap()))
    } else {
        None
    };

    let opts = CacheDumpOptions {
        input: input_file,
        output: output_file,
        async_io: matches.is_present("ASYNC_IO"),
        repair: matches.is_present("REPAIR"),
    };

    if let Err(reason) = dump(opts) {
        eprintln!("{}", reason);
        std::process::exit(1);
    }
}

//------------------------------------------
