mod bep;
mod bep_compat;
mod bep_core;
mod bep_proto;
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
use scenarios::{
    run_daemon_scenario_snapshot, run_peer_interop_scenario_snapshot, run_scenario_snapshot,
    scenario_ids,
};
use std::process;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 1 {
        run_daemon_command(&[]);
        return;
    }

    if matches!(args[1].as_str(), "-h" | "--help" | "help") {
        print_usage(false);
        return;
    }

    if matches!(args[1].as_str(), "--version" | "version") {
        // R8: Match Go's `syncthing vX.Y.Z "Codename" (os arch) user@date` format
        let build_user = std::env::var("USER").unwrap_or_else(|_| "unknown".to_string());
        println!(
            "syncthing v{} \"Fermium Flea\" ({} {}) {}@{}",
            env!("CARGO_PKG_VERSION"),
            std::env::consts::OS,
            std::env::consts::ARCH,
            build_user,
            option_env!("BUILD_DATE").unwrap_or("unknown"),
        );
        return;
    }

    if args[1].starts_with('-') {
        run_daemon_command(&args[1..]);
        return;
    }

    match args[1].as_str() {
        "scenario" => {
            if args.len() != 3 {
                eprintln!("scenario requires exactly one id");
                print_usage(true);
                process::exit(1);
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
        "daemon-scenario" => {
            if args.len() != 3 {
                eprintln!("daemon-scenario requires exactly one id");
                print_usage(true);
                process::exit(1);
            }
            match run_daemon_scenario_snapshot(&args[2]) {
                Ok(out) => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&out).expect("serialize scenario output")
                    );
                }
                Err(err) => {
                    eprintln!("daemon-scenario failed: {err}");
                    process::exit(1);
                }
            }
        }
        "interop-scenario" => {
            if args.len() != 3 {
                eprintln!("interop-scenario requires exactly one id");
                print_usage(true);
                process::exit(1);
            }
            match run_peer_interop_scenario_snapshot(&args[2]) {
                Ok(out) => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&out).expect("serialize scenario output")
                    );
                }
                Err(err) => {
                    eprintln!("interop-scenario failed: {err}");
                    process::exit(1);
                }
            }
        }
        "scenario-list" => {
            for id in scenario_ids() {
                println!("{id}");
            }
        }
        "serve" => run_daemon_command(&args[2..]),
        _ => {
            eprintln!("unknown command: {}", args[1]);
            print_usage(true);
            process::exit(1);
        }
    }
}

fn run_daemon_command(args: &[String]) {
    if args.len() == 1 && matches!(args[0].as_str(), "-h" | "--help" | "help") {
        print_usage(false);
        return;
    }
    match parse_daemon_args(args) {
        Ok(cfg) => {
            if let Err(err) = run_daemon(cfg) {
                eprintln!("daemon failed: {err}");
                process::exit(1);
            }
        }
        Err(err) => {
            eprintln!("invalid daemon arguments: {err}");
            print_usage(true);
            process::exit(1);
        }
    }
}

fn print_usage(stderr: bool) {
    if stderr {
        eprintln!("usage:");
        eprintln!("  syncthing-rs scenario <scenario-id>");
        eprintln!("  syncthing-rs daemon-scenario <scenario-id>");
        eprintln!("  syncthing-rs interop-scenario <scenario-id>");
        eprintln!("  syncthing-rs scenario-list");
        eprintln!(
            "  syncthing-rs serve (--folder <id>:<path> ... | --folder-path <path> [--folder-id <id>] | --config <path.json>) [--listen <addr>] [--api-listen <addr>] [--db-root <path>] [--memory-max-mb <n>] [--max-peers <n>] [--once]"
        );
    } else {
        println!("usage:");
        println!("  syncthing-rs scenario <scenario-id>");
        println!("  syncthing-rs daemon-scenario <scenario-id>");
        println!("  syncthing-rs interop-scenario <scenario-id>");
        println!("  syncthing-rs scenario-list");
        println!(
            "  syncthing-rs serve (--folder <id>:<path> ... | --folder-path <path> [--folder-id <id>] | --config <path.json>) [--listen <addr>] [--api-listen <addr>] [--db-root <path>] [--memory-max-mb <n>] [--max-peers <n>] [--once]"
        );
    }
}
