use clap::{value_parser, Arg, ArgAction};
use std::path::Path;
use std::process::exit;
use thinp::commands::engine::*;
use thinp::commands::utils::*;
use thinp::commands::Command;

mod merge;
use merge::*;

//------------------------------------------

pub struct ThinMergeCommand;

impl ThinMergeCommand {
    fn cli(&self) -> clap::Command {
        let cmd = clap::Command::new(self.name())
            .next_display_order(None)
            .version(thinp::tools_version!())
            .about("Merge an external snapshot and the in-pool origin into one device")
            .arg(
                Arg::new("METADATA_SNAPSHOT")
                    .help("Use metadata snapshot")
                    .short('m')
                    .long("metadata-snap")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("ORIGIN")
                    .help("The numeric identifier for the external origin")
                    .long("origin")
                    .value_name("DEV_ID")
                    .value_parser(value_parser!(u64))
                    .required(true),
            )
            .arg(
                Arg::new("SNAPSHOT")
                    .help("The numeric identifier for the external snapshot")
                    .long("snapshot")
                    .value_name("DEV_ID")
                    .value_parser(value_parser!(u64)),
            )
            // arguments
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input metadata")
                    .short('i')
                    .long("input")
                    .value_name("FILE")
                    .required(true),
            )
            .arg(
                Arg::new("OUTPUT")
                    .help("Specify the output metadata")
                    .short('o')
                    .long("output")
                    .value_name("FILE")
                    .required(true),
            );

        engine_args(cmd)
    }
}

impl<'a> Command<'a> for ThinMergeCommand {
    fn name(&self) -> &'a str {
        "thin_merge"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.get_one::<String>("INPUT").unwrap());
        let output_file = Path::new(matches.get_one::<String>("OUTPUT").unwrap());

        let report = mk_report(false);

        if let Err(e) = check_input_file(input_file).and_then(check_file_not_tiny) {
            return to_exit_code::<()>(&report, Err(e));
        }

        let engine_opts = parse_engine_opts(ToolType::Thin, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let origin = *matches.get_one::<u64>("ORIGIN").unwrap();
        let snapshot = *matches.get_one::<u64>("SNAPSHOT").unwrap();

        let opts = ThinMergeOptions {
            input: input_file,
            output: output_file,
            engine_opts: engine_opts.unwrap(),
            report: report.clone(),
            origin,
            snapshot,
        };

        to_exit_code(&report, merge_thins(opts))
    }
}

fn main() {
    let mut args = std::env::args_os();
    let cmd = ThinMergeCommand;
    exit(cmd.run(&mut args))
}

//------------------------------------------
