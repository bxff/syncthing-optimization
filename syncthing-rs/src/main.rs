mod bep;
mod bep_compat;
mod bep_core;
mod config;
mod db;
mod folder_core;
mod folder_modes;
mod index_engine;
mod model_core;
mod planner;
mod protocol;
mod runtime;
mod scenarios;
mod store;
mod walker;

use runtime::{parse_daemon_args, run_daemon};
use scenarios::{run_scenario_snapshot, scenario_ids};
use std::process;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        usage();
        process::exit(2);
    }

    match args[1].as_str() {
        "scenario" => {
            if args.len() != 3 {
                eprintln!("scenario requires exactly one id");
                usage();
                process::exit(2);
            }
            match run_scenario_snapshot(&args[2]) {
                Ok(out) => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&out).expect("serialize scenario output")
                    );
                }
                Err(err) => {
                    eprintln!("scenario failed: {err}");
                    process::exit(1);
                }
            }
        }
        "scenario-list" => {
            for id in scenario_ids() {
                println!("{id}");
            }
        }
        "daemon" => match parse_daemon_args(&args[2..]) {
            Ok(cfg) => {
                if let Err(err) = run_daemon(cfg) {
                    eprintln!("daemon failed: {err}");
                    process::exit(1);
                }
            }
            Err(err) => {
                eprintln!("invalid daemon arguments: {err}");
                usage();
                process::exit(2);
            }
        },
        _ => {
            eprintln!("unknown command: {}", args[1]);
            usage();
            process::exit(2);
        }
    }
}

fn usage() {
    eprintln!("usage:");
    eprintln!("  syncthing-rs scenario <scenario-id>");
    eprintln!("  syncthing-rs scenario-list");
    eprintln!(
        "  syncthing-rs daemon (--folder <id>:<path> ... | --folder-path <path> [--folder-id <id>] | --config <path.json>) [--listen <addr>] [--api-listen <addr>] [--db-root <path>] [--memory-max-mb <n>] [--max-peers <n>] [--once]"
    );
}
