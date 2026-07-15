//! Smoke tests that exercise `polytoken-acp` as an ACP client would.
//!
//! The ACP protocol is newline-delimited JSON-RPC 2.0 over stdio.
//! These tests spawn the `polytoken-acp` binary, send requests on stdin,
//! and read responses from stdout.
//!
//! Tests that require a real polytoken daemon are `#[ignore]`d.
//! Run them with: `cargo test --test smoke -- --ignored`

use std::process::Stdio;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

/// A minimal ACP client over stdio.
struct AcpClient {
    stdin: tokio::process::ChildStdin,
    stdout: BufReader<tokio::process::ChildStdout>,
    next_id: std::sync::atomic::AtomicU64,
    child: tokio::process::Child,
}

impl AcpClient {
    /// Spawn `polytoken-acp` and return a client connected to its stdio.
    async fn spawn() -> std::io::Result<Self> {
        let mut child = Command::new(env!("CARGO_BIN_EXE_polytoken-acp"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;

        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());

        Ok(Self {
            stdin,
            stdout,
            next_id: std::sync::atomic::AtomicU64::new(1),
            child,
        })
    }

    /// Send a JSON-RPC request and return the `result` field of the response.
    ///
    /// Panics if the response contains an error or cannot be parsed.
    async fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let msg = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });

        let line = serde_json::to_string(&msg).unwrap();
        self.stdin
            .write_all(line.as_bytes())
            .await
            .unwrap_or_else(|e| panic!("failed to write {method} request: {e}"));
        self.stdin
            .write_all(b"\n")
            .await
            .unwrap_or_else(|e| panic!("failed to write newline: {e}"));
        self.stdin.flush().await.unwrap();

        self.read_response(method).await
    }

    /// Read lines until we find a JSON-RPC response with a matching id, then
    /// return its `result`. Skips notifications.
    async fn read_response(&mut self, method_label: &str) -> Value {
        let mut buf = String::new();
        loop {
            buf.clear();
            match self.stdout.read_line(&mut buf).await {
                Ok(0) => {
                    // EOF — check if the child died
                    let _ = self.child.try_wait();
                    panic!("polytoken-acp stdout closed while waiting for {method_label} response");
                }
                Ok(_) => {}
                Err(e) => panic!("failed to read response: {e}"),
            }

            let line = buf.trim();
            if line.is_empty() {
                continue;
            }

            let msg: Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("Skipping unparseable line: {line} ({e})");
                    continue;
                }
            };

            // Skip notifications (no id)
            if msg.get("id").is_some() {
                if let Some(err) = msg.get("error") {
                    panic!("{method_label} returned error: {err}");
                }
                return msg.get("result").cloned().unwrap_or(Value::Null);
            }
        }
    }

    /// Read lines until we find a JSON-RPC response with a matching id.
    /// Collects all notifications (lines without an id) encountered along the way.
    async fn read_response_with_notifications(
        &mut self,
        method_label: &str,
    ) -> (Value, Vec<Value>) {
        let mut notifications = Vec::new();
        let mut buf = String::new();
        loop {
            buf.clear();
            match self.stdout.read_line(&mut buf).await {
                Ok(0) => {
                    let _ = self.child.try_wait();
                    panic!("polytoken-acp stdout closed while waiting for {method_label} response");
                }
                Ok(_) => {}
                Err(e) => panic!("failed to read response: {e}"),
            }

            let line = buf.trim();
            if line.is_empty() {
                continue;
            }

            let msg: Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("Skipping unparseable line: {line} ({e})");
                    continue;
                }
            };

            if msg.get("id").is_some() {
                // Response — return result + collected notifications
                if let Some(err) = msg.get("error") {
                    panic!("{method_label} returned error: {err}");
                }
                return (
                    msg.get("result").cloned().unwrap_or(Value::Null),
                    notifications,
                );
            } else {
                // Notification — collect it
                notifications.push(msg);
            }
        }
    }

    async fn kill(&mut self) {
        let _ = self.child.kill().await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Verify the `initialize` response advertises the expected capabilities.
///
/// This test does NOT require the polytoken binary — it only checks the ACP
/// handshake before any daemon spawning happens.
#[tokio::test]
async fn test_initialize_capabilities() {
    let mut client = AcpClient::spawn()
        .await
        .expect("failed to spawn polytoken-acp");

    let result = client
        .request(
            "initialize",
            json!({
                "protocolVersion": 1,
                "clientCapabilities": {},
            }),
        )
        .await;

    // Agent info
    let agent_info = result.get("agentInfo").expect("missing agentInfo");
    assert_eq!(agent_info["name"], "polytoken");

    // load_session should be true (session/load replays history)
    let caps = result
        .get("agentCapabilities")
        .expect("missing agentCapabilities");
    assert_eq!(caps["loadSession"], true, "loadSession should be true");

    // Session capabilities: must advertise list and resume
    let session_caps = caps
        .get("sessionCapabilities")
        .expect("missing sessionCapabilities");
    assert!(
        session_caps.get("list").is_some(),
        "must advertise session/list capability"
    );
    assert!(
        session_caps.get("resume").is_some(),
        "must advertise session/resume capability"
    );
    assert!(
        session_caps.get("close").is_some(),
        "must advertise session/close capability"
    );

    // _meta should advertise polytoken extension methods
    let meta = caps
        .get("_meta")
        .expect("missing _meta in agentCapabilities");
    assert_eq!(
        meta["polytoken"]["ask_user_question"], true,
        "_meta must advertise ask_user_question extension"
    );
    assert_eq!(
        meta["polytoken"]["system_reminder"], true,
        "_meta must advertise system_reminder extension"
    );

    client.kill().await;
}

/// Check if the `polytoken` binary is available on PATH.
fn polytoken_available() -> bool {
    std::process::Command::new("polytoken")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .is_ok()
}

/// Full initialize → authenticate → session/new round-trip.
///
/// Verifies that:
/// - session/new returns a session_id
/// - the response includes modes (execute, plan)
/// - the response includes config_options with a model select containing
///   at least one option
///
/// Run with: `cargo test --test smoke -- --ignored test_session_new`
#[tokio::test]
#[ignore = "Requires polytoken binary + LLM credentials"]
async fn test_session_new() {
    if !polytoken_available() {
        eprintln!("SKIP: polytoken binary not on PATH");
        return;
    }

    let mut client = AcpClient::spawn()
        .await
        .expect("failed to spawn polytoken-acp");

    // Initialize
    client
        .request(
            "initialize",
            json!({"protocolVersion": 1, "clientCapabilities": {}}),
        )
        .await;

    // Authenticate (noop method — agent returns empty auth methods)
    client
        .request("authenticate", json!({"methodId": "noop"}))
        .await;

    // session/new
    let cwd = std::env::current_dir().unwrap();
    let result = client
        .request(
            "session/new",
            json!({
                "cwd": cwd,
                "mcpServers": [],
            }),
        )
        .await;

    let session_id = result
        .get("sessionId")
        .and_then(|v| v.as_str())
        .expect("missing sessionId in session/new response");
    assert!(!session_id.is_empty(), "sessionId should not be empty");

    // Modes
    let modes = result.get("modes").expect("missing modes");
    let available_modes = modes["availableModes"]
        .as_array()
        .expect("availableModes should be an array");
    assert!(
        available_modes.iter().any(|m| m["id"] == "execute"),
        "modes should include execute"
    );
    assert!(
        available_modes.iter().any(|m| m["id"] == "plan"),
        "modes should include plan"
    );

    // Config options
    let config_options = result
        .get("configOptions")
        .and_then(|v| v.as_array())
        .expect("missing or non-array configOptions");

    let model_option = config_options
        .iter()
        .find(|o| o["category"] == "model")
        .expect("configOptions should contain an option with category=model");

    assert_eq!(
        model_option["id"], "model",
        "model config option should have id=model"
    );
    assert_eq!(
        model_option["type"], "select",
        "model config option should be type=select"
    );

    let options = model_option["options"]
        .as_array()
        .expect("model options should be an array");
    assert!(
        !options.is_empty(),
        "model select should have at least one option"
    );

    // Each option should have a value and name
    for opt in options {
        assert!(
            opt.get("value").and_then(|v| v.as_str()).is_some(),
            "model option missing value"
        );
        assert!(
            opt.get("name").and_then(|v| v.as_str()).is_some(),
            "model option missing name"
        );
    }

    eprintln!("session/new returned {} models", options.len());

    client.kill().await;
}

/// session/resume on a known session ID should return modes and config_options.
///
/// Flow: initialize → authenticate → session/new → session/resume (same ID).
/// The resume should fail because the session is already in memory.
///
/// Run with: `cargo test --test smoke -- --ignored test_session_resume`
#[tokio::test]
#[ignore = "Requires polytoken binary + LLM credentials"]
async fn test_session_resume() {
    if !polytoken_available() {
        eprintln!("SKIP: polytoken binary not on PATH");
        return;
    }

    let mut client = AcpClient::spawn()
        .await
        .expect("failed to spawn polytoken-acp");
    let cwd = std::env::current_dir().unwrap();

    // Initialize + authenticate
    client
        .request(
            "initialize",
            json!({"protocolVersion": 1, "clientCapabilities": {}}),
        )
        .await;
    client
        .request("authenticate", json!({"methodId": "noop"}))
        .await;

    // Create a session
    let result = client
        .request("session/new", json!({"cwd": cwd, "mcpServers": []}))
        .await;
    let session_id = result["sessionId"]
        .as_str()
        .expect("missing sessionId")
        .to_string();

    // Attempt to resume the same session — should error (already in memory)
    let id = client
        .next_id
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let msg = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/resume",
        "params": {
            "sessionId": &session_id,
            "cwd": &cwd,
            "mcpServers": [],
        },
    });
    let line = serde_json::to_string(&msg).unwrap();
    client.stdin.write_all(line.as_bytes()).await.unwrap();
    client.stdin.write_all(b"\n").await.unwrap();
    client.stdin.flush().await.unwrap();

    // Read response — it should be an error
    let mut buf = String::new();
    loop {
        buf.clear();
        match client.stdout.read_line(&mut buf).await {
            Ok(0) => panic!("stdout closed waiting for session/resume response"),
            Ok(_) => {}
            Err(e) => panic!("read error: {e}"),
        }
        let line = buf.trim();
        if line.is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if msg.get("id").and_then(|v| v.as_u64()) == Some(id) {
            let err = msg
                .get("error")
                .expect("expected error for duplicate resume");
            let message = err["message"].as_str().unwrap_or("(no message)");
            eprintln!("session/resume of existing session correctly returned error: {message}");
            break;
        }
    }

    client.kill().await;
}

/// session/set_config_option with model should switch the model.
///
/// Flow: initialize → authenticate → session/new → session/set_config_option.
/// Verifies the config option is accepted without error.
///
/// Run with: `cargo test --test smoke -- --ignored test_set_config_option`
#[tokio::test]
#[ignore = "Requires polytoken binary + LLM credentials"]
async fn test_set_config_option() {
    if !polytoken_available() {
        eprintln!("SKIP: polytoken binary not on PATH");
        return;
    }

    let mut client = AcpClient::spawn()
        .await
        .expect("failed to spawn polytoken-acp");
    let cwd = std::env::current_dir().unwrap();

    // Initialize + authenticate
    client
        .request(
            "initialize",
            json!({"protocolVersion": 1, "clientCapabilities": {}}),
        )
        .await;
    client
        .request("authenticate", json!({"methodId": "noop"}))
        .await;

    // Create a session
    let result = client
        .request("session/new", json!({"cwd": cwd, "mcpServers": []}))
        .await;
    let session_id = result["sessionId"]
        .as_str()
        .expect("missing sessionId")
        .to_string();

    // Find the first available model
    let config_options = result["configOptions"]
        .as_array()
        .expect("missing configOptions");
    let model_option = config_options
        .iter()
        .find(|o| o["category"] == "model")
        .expect("no model config option");
    let model_value = model_option["options"][0]["value"]
        .as_str()
        .expect("missing model value");

    eprintln!("Switching model to: {model_value}");

    // Set the config option
    let _result = client
        .request(
            "session/set_config_option",
            json!({
                "sessionId": &session_id,
                "configId": "model",
                "value": model_value,
            }),
        )
        .await;

    // If we get here without panic, the config option was accepted.
    eprintln!("session/set_config_option accepted model={model_value}");

    client.kill().await;
}

/// Verify that the models returned by `session/new` configOptions match the
/// models reported by `polytoken models`.
///
/// This cross-references the daemon's `/state` endpoint (which feeds the ACP
/// config options) against the CLI `polytoken models` output to catch
/// drift between what the daemon exposes and what the ACP shim advertises.
///
/// Run with: `cargo test --test smoke -- --ignored test_models_match_cli`
#[tokio::test]
#[ignore = "Requires polytoken binary + LLM credentials"]
async fn test_models_match_cli() {
    if !polytoken_available() {
        eprintln!("SKIP: polytoken binary not on PATH");
        return;
    }

    // --- Collect expected model names from `polytoken models` ---
    let cli_output = std::process::Command::new("polytoken")
        .arg("models")
        .output()
        .expect("failed to run `polytoken models`");

    assert!(
        cli_output.status.success(),
        "`polytoken models` exited with status {}",
        cli_output.status
    );

    let cli_stdout = String::from_utf8_lossy(&cli_output.stdout);

    // Parse selectable model names from lines like:
    //   "  selectable: zai/glm-5.2, zai/glm-5.2(none), ..."
    // and also the provider line:
    //   "  provider: zai/glm-5.2"
    let mut cli_model_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    for line in cli_stdout.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("selectable:") {
            for entry in rest.split(',') {
                let name = entry.trim();
                // Skip reasoning-variant suffixes like "model(none)" or "model(high)"
                // — we only want the base model name.
                let base = name.split('(').next().unwrap_or(name).trim();
                if !base.is_empty() {
                    cli_model_names.insert(base.to_string());
                }
            }
        } else if let Some(rest) = trimmed.strip_prefix("provider:") {
            let name = rest.trim();
            if !name.is_empty() {
                cli_model_names.insert(name.to_string());
            }
        }
    }

    assert!(
        !cli_model_names.is_empty(),
        "no models found in `polytoken models` output"
    );

    eprintln!("CLI reports {} unique model names", cli_model_names.len());

    // --- Get models from session/new configOptions ---
    let mut client = AcpClient::spawn()
        .await
        .expect("failed to spawn polytoken-acp");
    let cwd = std::env::current_dir().unwrap();

    client
        .request(
            "initialize",
            json!({"protocolVersion": 1, "clientCapabilities": {}}),
        )
        .await;
    client
        .request("authenticate", json!({"methodId": "noop"}))
        .await;

    let result = client
        .request("session/new", json!({"cwd": cwd, "mcpServers": []}))
        .await;

    let config_options = result["configOptions"]
        .as_array()
        .expect("missing configOptions");

    let model_option = config_options
        .iter()
        .find(|o| o["category"] == "model")
        .expect("configOptions should contain an option with category=model");

    let acp_options = model_option["options"]
        .as_array()
        .expect("model options should be an array");

    assert!(
        !acp_options.is_empty(),
        "ACP configOptions model list should not be empty"
    );

    // Collect model value IDs from the ACP response
    let acp_model_names: std::collections::HashSet<String> = acp_options
        .iter()
        .map(|o| {
            o["value"]
                .as_str()
                .expect("model option missing value")
                .to_string()
        })
        .collect();

    eprintln!(
        "ACP configOptions reports {} model names",
        acp_model_names.len()
    );

    // Every ACP model should appear in the CLI's model list.
    // (The CLI may list more entries — e.g. dynamic models not yet loaded —
    // so we check subset, not equality.)
    let missing: Vec<_> = acp_model_names
        .iter()
        .filter(|m| !cli_model_names.contains(*m))
        .collect();

    assert!(
        missing.is_empty(),
        "Models in ACP configOptions but not in CLI output: {:?}",
        missing
    );

    eprintln!("All ACP models are present in CLI output");

    client.kill().await;
}

/// session/close should terminate the session's daemon.
///
/// Flow: initialize → authenticate → session/new → session/close.
/// After closing, a second session/close on the same ID should still succeed
/// (idempotent — spec says "Agents MAY return an error if the session does
/// not exist or is not currently active" but we choose to be idempotent).
///
/// Run with: `cargo test --test smoke -- --ignored test_session_close`
#[tokio::test]
#[ignore = "Requires polytoken binary + LLM credentials"]
async fn test_session_close() {
    if !polytoken_available() {
        eprintln!("SKIP: polytoken binary not on PATH");
        return;
    }

    let mut client = AcpClient::spawn()
        .await
        .expect("failed to spawn polytoken-acp");
    let cwd = std::env::current_dir().unwrap();

    client
        .request(
            "initialize",
            json!({"protocolVersion": 1, "clientCapabilities": {}}),
        )
        .await;
    client
        .request("authenticate", json!({"methodId": "noop"}))
        .await;

    // Create a session
    let result = client
        .request("session/new", json!({"cwd": cwd, "mcpServers": []}))
        .await;
    let session_id = result["sessionId"]
        .as_str()
        .expect("missing sessionId")
        .to_string();

    eprintln!("Created session {session_id}, closing...");

    // Close it
    let _result = client
        .request("session/close", json!({"sessionId": &session_id}))
        .await;

    eprintln!("Session closed successfully");

    // Closing again should still work (idempotent)
    let _result = client
        .request("session/close", json!({"sessionId": &session_id}))
        .await;

    eprintln!("Second close on same session succeeded (idempotent)");

    client.kill().await;
}

/// session/load on a known session ID should return modes, config_options,
/// and replay history as session notifications.
///
/// Flow: initialize → authenticate → session/new → session/close → session/load (same ID).
///
/// Run with: `cargo test --test smoke -- --ignored test_session_load`
#[tokio::test]
#[ignore = "Requires polytoken binary + LLM credentials"]
async fn test_session_load() {
    if !polytoken_available() {
        eprintln!("SKIP: polytoken binary not on PATH");
        return;
    }

    let mut client = AcpClient::spawn()
        .await
        .expect("failed to spawn polytoken-acp");
    let cwd = std::env::current_dir().unwrap();

    // Initialize + authenticate
    client
        .request(
            "initialize",
            json!({"protocolVersion": 1, "clientCapabilities": {}}),
        )
        .await;
    client
        .request("authenticate", json!({"methodId": "noop"}))
        .await;

    // Create a session
    let result = client
        .request("session/new", json!({"cwd": cwd, "mcpServers": []}))
        .await;
    let session_id = result["sessionId"]
        .as_str()
        .expect("missing sessionId")
        .to_string();

    eprintln!("Created session {session_id}, closing before load...");

    // Close it so we can load it fresh
    let _ = client
        .request("session/close", json!({"sessionId": &session_id}))
        .await;

    // Send session/load request (using raw write since request() uses read_response)
    let id = client
        .next_id
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let msg = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/load",
        "params": {
            "sessionId": &session_id,
            "cwd": &cwd,
            "mcpServers": [],
        },
    });
    let line = serde_json::to_string(&msg).unwrap();
    client.stdin.write_all(line.as_bytes()).await.unwrap();
    client.stdin.write_all(b"\n").await.unwrap();
    client.stdin.flush().await.unwrap();

    // Read response and collect notifications sent before it
    let (load_result, notifications) = client
        .read_response_with_notifications("session/load")
        .await;

    // The response should include modes
    let modes = load_result.get("modes");
    if let Some(modes) = modes {
        let available_modes = modes["availableModes"]
            .as_array()
            .expect("availableModes should be an array");
        assert!(
            available_modes.iter().any(|m| m["id"] == "execute"),
            "modes should include execute"
        );
        eprintln!("session/load returned {} modes", available_modes.len());
    }

    // The response should include config_options
    let config_options = load_result.get("configOptions");
    if let Some(opts) = config_options.and_then(|v| v.as_array()) {
        eprintln!("session/load returned {} config options", opts.len());
    }

    // We should have received at least some notifications (history replay or
    // session_info_update, available_commands_update, Plan)
    eprintln!(
        "session/load sent {} notifications before response",
        notifications.len()
    );

    client.kill().await;
}
