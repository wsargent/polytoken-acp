//! polytoken-acp: ACP server shim for the Polytoken daemon.
//!
//! Bridges the Agent Client Protocol (JSON-RPC over stdio) to the polytoken
//! daemon's HTTP/SSE API, allowing Paseo or any ACP-compatible editor to drive
//! Polytoken as an agent without the TUI.

mod agent;
mod daemon;
mod events;

use std::rc::Rc;

use agent_client_protocol::{self as acp};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::{error, info};

fn main() {
    // Initialize tracing — output goes to stderr ONLY (stdout is JSON-RPC)
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("polytoken-acp starting up (version {})", env!("CARGO_PKG_VERSION"));

    // Build the tokio runtime
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    runtime.block_on(async {
        let local_set = tokio::task::LocalSet::new();
        local_set
            .run_until(async move {
                let agent = Rc::new(agent::PolytokenAgent::new());

                // Wire stdin/stdout to the ACP connection
                // stdout = outgoing (to client), stdin = incoming (from client)
                let outgoing = tokio::io::stdout().compat_write();
                let incoming = tokio::io::stdin().compat();

                // The spawn closure MUST use spawn_local (ACP SDK requires ?Send futures)
                let (conn, io_future) = acp::AgentSideConnection::new(
                    Rc::clone(&agent),
                    outgoing,
                    incoming,
                    |fut| {
                        tokio::task::spawn_local(fut);
                    },
                );

                // Inject the connection into the agent for sending notifications
                agent.set_connection(Rc::new(conn));

                // Spawn the I/O future on the LocalSet
                let io_handle = tokio::task::spawn_local(io_future);

                // Wait for the I/O loop to finish (stdin closes, client disconnects)
                match io_handle.await {
                    Ok(Ok(())) => info!("ACP connection closed gracefully"),
                    Ok(Err(e)) => info!(error = %e, "ACP connection ended with error"),
                    Err(e) => error!("ACP I/O task panicked: {}", e),
                }

                // Clean up: terminate all daemon processes
                agent.shutdown().await;
                info!("polytoken-acp shutting down");
            })
            .await;
    });
}
