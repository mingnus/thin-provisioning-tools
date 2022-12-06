use clap::Arg;
use std::path::Path;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::{parse_log_level, verbose_args};
use crate::thin::check::{check, ThinCheckOptions};

pub struct ThinCheckCommand;

impl ThinCheckCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        let cmd = clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::version::tools_version())
            .about("Validates thin provisioning metadata on a device or file.")
            // flags
            .arg(
                Arg::new("AUTO_REPAIR")
                    .help("Auto repair trivial issues.")
                    .long("auto-repair")
                    .conflicts_with_all(&[
                        "IGNORE_NON_FATAL",
                        "METADATA_SNAPSHOT",
                        "OVERRIDE_MAPPING_ROOT",
                        "SB_ONLY",
                        "SKIP_MAPPINGS",
                    ]),
            )
            .arg(
                // Using --clear-needs-check along with --skip-mappings is allowed
                // (but not recommended) for backward compatibility (commit 1fe8a0d)
                Arg::new("CLEAR_NEEDS_CHECK")
                    .help("Clears the 'needs_check' flag in the superblock")
                    .long("clear-needs-check-flag")
                    .conflicts_with_all(&["METADATA_SNAPSHOT", "OVERRIDE_MAPPING_ROOT"]),
            )
            .arg(
                Arg::new("IGNORE_NON_FATAL")
                    .help("Only return a non-zero exit code if a fatal error is found.")
                    .long("ignore-non-fatal-errors"),
            )
            .arg(
                Arg::new("METADATA_SNAPSHOT")
                    .help("Check the metadata snapshot on a live pool")
                    .short('m')
                    .long("metadata-snap"),
            )
            .arg(
                Arg::new("QUIET")
                    .help("Suppress output messages, return only exit code.")
                    .short('q')
                    .long("quiet"),
            )
            .arg(
                Arg::new("SB_ONLY")
                    .help("Only check the superblock.")
                    .long("super-block-only"),
            )
            .arg(
                Arg::new("SKIP_MAPPINGS")
                    .help("Don't check the mapping tree")
                    .long("skip-mappings"),
            )
            // options
            .arg(
                Arg::new("OVERRIDE_MAPPING_ROOT")
                    .help("Specify a mapping root to use")
                    .long("override-mapping-root")
                    .value_name("BLOCKNR")
                    .takes_value(true),
            )
            // arguments
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input device to check")
                    .required(true)
                    .index(1),
            );
        verbose_args(engine_args(cmd))
    }
}

impl<'a> Command<'a> for ThinCheckCommand {
    fn name(&self) -> &'a str {
        "thin_check"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.value_of("INPUT").unwrap());

        let report = mk_report(matches.is_present("QUIET"));
        let log_level = match parse_log_level(&matches) {
            Ok(level) => level,
            Err(e) => return to_exit_code::<()>(&report, Err(anyhow::Error::msg(e))),
        };
        report.set_level(log_level);

        check_input_file(input_file, &report);
        check_file_not_tiny(input_file, &report);
        check_not_xml(input_file, &report);

        let engine_opts = parse_engine_opts(ToolType::Thin, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts.map(|_| ()));
        }
        let engine_opts = engine_opts.unwrap();

        let opts = ThinCheckOptions {
            input: input_file,
            engine_opts,
            sb_only: matches.is_present("SB_ONLY"),
            skip_mappings: matches.is_present("SKIP_MAPPINGS"),
            ignore_non_fatal: matches.is_present("IGNORE_NON_FATAL"),
            auto_repair: matches.is_present("AUTO_REPAIR"),
            clear_needs_check: matches.is_present("CLEAR_NEEDS_CHECK"),
            override_mapping_root: optional_value_or_exit::<u64>(&matches, "OVERRIDE_MAPPING_ROOT"),
            report: report.clone(),
        };

        to_exit_code(&report, check(opts))
    }
}
