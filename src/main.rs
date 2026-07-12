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
    // Initialize tracing — output goes to stderr ONLY (stdout is JSON-RPC)
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
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
