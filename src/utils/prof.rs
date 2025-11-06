use std::fs::File;
use std::io::Read;

use crate::report::Report;

//------------------------------------------

pub fn get_memory_usage() -> Result<usize, std::io::Error> {
    let mut s = String::new();
    File::open("/proc/self/statm")?.read_to_string(&mut s)?;
    let pages = s
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse::<usize>()
        .unwrap();
    Ok((pages * 4096) / (1024 * 1024))
}

pub fn print_mem(report: &Report, msg: &str) {
    report.debug(&format!("{}: {} meg", msg, get_memory_usage().unwrap()));
}

//------------------------------------------
