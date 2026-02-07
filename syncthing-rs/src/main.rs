mod folder_modes;
mod planner;
mod protocol;
mod scenarios;
mod store;

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
}
