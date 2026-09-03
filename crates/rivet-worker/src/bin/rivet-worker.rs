//! The Rivet worker process.
//!
//! Deliberately thin. Everything worth testing lives in `rivet_worker::serve`.
//!
//!     cargo run --bin rivet-worker -- 127.0.0.1:7001 2
//!
//! Cargo finds this file because it sits in `src/bin/`; no `[[bin]]` entry is
//! needed. It also exports the built path to tests as
//! `env!("CARGO_BIN_EXE_rivet-worker")`, which is how the integration tests
//! launch a real worker without guessing where `target/` is.

use rivet_worker::serve::{self, Config};
use std::net::TcpListener;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();

    let Config { addr, capacity } = match serve::parse_args(&args) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("rivet-worker: {message}");
            eprintln!("usage: rivet-worker <addr> [capacity]");
            return ExitCode::from(2);
        }
    };

    let listener = match TcpListener::bind(addr) {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("rivet-worker: could not bind {addr}: {e}");
            return ExitCode::FAILURE;
        }
    };

    // Bind with port 0 and the OS picks a free port; print the real one so a
    // parent process can read it back.
    match listener.local_addr() {
        Ok(bound) => println!("listening on {bound}"),
        Err(e) => eprintln!("rivet-worker: bound, but could not read the address: {e}"),
    }

    match serve::serve(&listener, capacity) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("rivet-worker: {e}");
            ExitCode::FAILURE
        }
    }
}
