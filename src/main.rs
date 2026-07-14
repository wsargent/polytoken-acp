//! polytoken-acp: ACP server shim for the Polytoken daemon.
//!
//! Bridges the Agent Client Protocol (JSON-RPC over stdio) to the polytoken
//! daemon's HTTP/SSE API, allowing Paseo or any ACP-compatible editor to drive
//! Polytoken as an agent without the TUI.

mod agent;
mod daemon;
mod events;

use tracing::info;

fn main() {
    // Handle --version / -V before anything else — Paseo calls this in its
    // diagnostic check and expects a quick exit, not an ACP server startup.
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--version" || a == "-V") {
        println!("polytoken-acp {}", env!("CARGO_PKG_VERSION"));
        return;
    }

    // Initialize tracing — output goes to stderr ONLY (stdout is JSON-RPC)
    // All logs are structured JSON (one object per line) for machine consumption.
    //
    // The conversation target (polytoken_acp::conv) provides turn-by-turn
    // conversation monitoring with structured fields like event_type, summary,
    // prompt_id, etc.
    //
    // Override with RUST_LOG, e.g.:
    //   RUST_LOG=info                       — default conversation monitoring
    //   RUST_LOG=debug                      — per-event daemon/ACP detail
    //   RUST_LOG=polytoken_acp::conv=debug  — verbose conversation only
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("info,polytoken_acp::conv=info")
            }),
        )
        .init();

    info!(
        "polytoken-acp starting up (version {})",
        env!("CARGO_PKG_VERSION")
    );

    // Build the tokio runtime
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    runtime.block_on(async {
        agent::run().await;
    });
}
