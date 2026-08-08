use rivet_client::{Client, LocalClient};
use rivet_core::TaskPayload;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(String::as_str) {
        Some("submit") => cmd_submit(&args[2..]),
        Some("status") => cmd_status(&args[2..]),
        Some("workers") => cmd_workers(),
        Some(unknown) => {
            eprintln!("error: unknown command '{unknown}'");
            eprintln!();
            print_usage();
            std::process::exit(1);
        }
        None => print_usage(),
    }
}

fn cmd_submit(args: &[String]) {
    let name = args.first().map(String::as_str).unwrap_or("unnamed");

    // TODO (Milestone 1): Remove the note below once LocalScheduler::submit works.
    println!("Submitting task '{name}'...");

    let mut client = LocalClient::new();
    match client.submit(TaskPayload::new(name)) {
        Ok(id) => println!("Submitted: {id}"),
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}

fn cmd_status(args: &[String]) {
    let id_str = args.first().map(String::as_str).unwrap_or("?");
    // TODO (Milestone 1): Parse the task ID and call client.get_result(id).
    println!("Status of task '{id_str}': (not yet implemented)");
}

fn cmd_workers() {
    // TODO (Milestone 2): List registered workers from the scheduler.
    println!("Workers: (not yet implemented)");
}

fn print_usage() {
    println!("Rivet — distributed task execution");
    println!();
    println!("USAGE:");
    println!("    rivet <COMMAND> [ARGS]");
    println!();
    println!("COMMANDS:");
    println!("    submit <name>    Submit a named task for execution");
    println!("    status <id>      Check the status of a submitted task");
    println!("    workers          List all registered workers");
}
