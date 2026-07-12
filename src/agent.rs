//! ACP Agent implementation for the polytoken daemon shim.
//!
//! In ACP 1.x, the agent is built using a builder pattern with
//! `on_receive_request` / `on_receive_notification` handlers rather than
//! implementing a trait. The shared session state (daemon handles) lives in
//! an `Arc<Mutex<>>` captured by each handler closure.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use agent_client_protocol::schema::v1 as acp;
use agent_client_protocol::{
    Agent, Client, ConnectionTo, Dispatch, Stdio, on_receive_dispatch, on_receive_notification,
    on_receive_request,
};
use tracing::{debug, error, info, warn};

use crate::daemon::DaemonHandle;
use crate::events::{self, AskUserQuestionPayload, EventTranslation};

/// Shared state across all ACP handlers for one connection.
struct AgentState {
    sessions: HashMap<String, DaemonHandle>,
}

impl AgentState {
    fn new() -> Self {
        Self {
            sessions: HashMap::new(),
        }
    }

    /// Take all daemons out of the map and terminate them.
    #[allow(dead_code)]
    async fn shutdown(&mut self) {
        let daemons: Vec<(String, DaemonHandle)> = self.sessions.drain().collect();
        for (_, mut daemon) in daemons {
            daemon.terminate().await;
        }
    }
}

/// Entry point: build the agent, wire up handlers, and run over stdio.
pub async fn run() {
    let state = Arc::new(Mutex::new(AgentState::new()));

    let result = Agent
        .builder()
        .name("polytoken")
        // initialize
        .on_receive_request(
            async |_req: acp::InitializeRequest, responder, _cx| {
                info!("ACP initialize from client");
                let caps = acp::AgentCapabilities::new()
                    .load_session(false)
                    .prompt_capabilities(acp::PromptCapabilities::new().embedded_context(true))
                    .mcp_capabilities(acp::McpCapabilities::new())
                    .session_capabilities(
                        acp::SessionCapabilities::new()
                            .list(acp::SessionListCapabilities::new())
                            .resume(acp::SessionResumeCapabilities::new())
                            .close(acp::SessionCloseCapabilities::new()),
                    );
                let resp = acp::InitializeResponse::new(_req.protocol_version)
                    .agent_capabilities(caps)
                    .agent_info(
                        acp::Implementation::new("polytoken", env!("CARGO_PKG_VERSION"))
                            .title("Polytoken"),
                    );
                responder.respond(resp)
            },
            on_receive_request!(),
        )
        // authenticate
        .on_receive_request(
            async |_req: acp::AuthenticateRequest, responder, _cx| {
                responder.respond(acp::AuthenticateResponse::new())
            },
            on_receive_request!(),
        )
        // new_session
        .on_receive_request(
            {
                let state = Arc::clone(&state);
                async move |req: acp::NewSessionRequest, responder, cx| {
                    handle_new_session(&state, req, responder, cx).await
                }
            },
            on_receive_request!(),
        )
        // session/resume
        .on_receive_request(
            {
                let state = Arc::clone(&state);
                async move |req: acp::ResumeSessionRequest, responder, cx| {
                    handle_resume_session(&state, req, responder, cx).await
                }
            },
            on_receive_request!(),
        )
        // session/close
        .on_receive_request(
            {
                let state = Arc::clone(&state);
                async move |req: acp::CloseSessionRequest, responder, cx| {
                    handle_close_session(&state, req, responder, cx).await
                }
            },
            on_receive_request!(),
        )
        // prompt
        .on_receive_request(
            {
                let state = Arc::clone(&state);
                async move |req: acp::PromptRequest, responder, cx| {
                    handle_prompt(&state, req, responder, cx).await
                }
            },
            on_receive_request!(),
        )
        // list_sessions
        .on_receive_request(
            {
                let state = Arc::clone(&state);
                async move |req: acp::ListSessionsRequest, responder, cx| {
                    handle_list_sessions(&state, req, responder, cx).await
                }
            },
            on_receive_request!(),
        )
        // set_session_mode
        .on_receive_request(
            {
                let state = Arc::clone(&state);
                async move |req: acp::SetSessionModeRequest, responder, cx| {
                    handle_set_session_mode(&state, req, responder, cx).await
                }
            },
            on_receive_request!(),
        )
        // set_session_config_option
        .on_receive_request(
            {
                let state = Arc::clone(&state);
                async move |req: acp::SetSessionConfigOptionRequest, responder, cx| {
                    handle_set_session_config_option(&state, req, responder, cx).await
                }
            },
            on_receive_request!(),
        )
        // cancel (notification)
        .on_receive_notification(
            {
                let state = Arc::clone(&state);
                async move |notif: acp::CancelNotification, _cx| {
                    handle_cancel(&state, &notif).await;
                    Ok(())
                }
            },
            on_receive_notification!(),
        )
        // Fallback: respond to any unhandled message with an error
        .on_receive_dispatch(
            async move |message: Dispatch, cx| {
                message.respond_with_error(
                    agent_client_protocol::util::internal_error("unhandled message"),
                    cx,
                )
            },
            on_receive_dispatch!(),
        )
        .connect_to(Stdio::new())
        .await;

    match &result {
        Ok(()) => info!("ACP connection closed gracefully"),
        Err(e) => error!(error = %e, "ACP connection ended with error"),
    }

    // Clean up: terminate all daemon processes
    let daemons: Vec<(String, DaemonHandle)> = state.lock().unwrap().sessions.drain().collect();
    for (_, mut daemon) in daemons {
        daemon.terminate().await;
    }
    info!("polytoken-acp shutting down");
}

// ---------------------------------------------------------------------------
// Request handlers
// ---------------------------------------------------------------------------

async fn handle_new_session(
    state: &Arc<Mutex<AgentState>>,
    req: acp::NewSessionRequest,
    responder: agent_client_protocol::Responder<acp::NewSessionResponse>,
    cx: ConnectionTo<Client>,
) -> Result<(), agent_client_protocol::Error> {
    info!(cwd = ?req.cwd, "ACP new_session");

    if !req.mcp_servers.is_empty() {
        warn!(
            count = req.mcp_servers.len(),
            "MCP servers passed by client are acknowledged but not forwarded (v1)"
        );
    }

    match DaemonHandle::spawn(&req.cwd).await {
        Ok(daemon) => {
            let session_id = daemon.session_id().to_string();

            // Fetch daemon state once, then build modes and config options from it.
            let daemon_state = daemon.fetch_daemon_state().await;
            let mode_state = build_session_mode_state_from_value(&daemon_state);
            let config_options = build_config_options(&daemon_state, &mode_state);

            state
                .lock()
                .unwrap()
                .sessions
                .insert(session_id.clone(), daemon);

            info!(session_id = %session_id, "New session created");

            // Send session_info_update with the title from the daemon state.
            if let Ok(ref ds) = daemon_state {
                if let Some(title) = ds.get("session_title").and_then(|v| v.as_str()) {
                    if !title.is_empty() {
                        let info_update = acp::SessionInfoUpdate::new().title(title.to_string());
                        let sid = acp::SessionId::new(session_id.clone());
                        let notification = acp::SessionNotification::new(
                            sid,
                            acp::SessionUpdate::SessionInfoUpdate(info_update),
                        );
                        if let Err(e) = cx.send_notification(notification) {
                            warn!(error = %e, "Failed to send session_info_update notification");
                        }
                    }
                }
            }

            let mut response = acp::NewSessionResponse::new(session_id);
            if let Some(ms) = &mode_state {
                response = response.modes(ms.clone());
            }
            if !config_options.is_empty() {
                response = response.config_options(config_options);
            }
            responder.respond(response)
        }
        Err(e) => {
            error!(error = %e, "Failed to spawn daemon");
            responder.respond_with_error(agent_client_protocol::Error::internal_error().data(
                serde_json::json!({
                    "error": "Failed to start polytoken daemon",
                    "detail": e.to_string(),
                }),
            ))
        }
    }
}

async fn handle_resume_session(
    state: &Arc<Mutex<AgentState>>,
    req: acp::ResumeSessionRequest,
    responder: agent_client_protocol::Responder<acp::ResumeSessionResponse>,
    _cx: ConnectionTo<Client>,
) -> Result<(), agent_client_protocol::Error> {
    let session_id = req.session_id.0.to_string();

    info!(
        session_id = %session_id,
        cwd = ?req.cwd,
        "ACP session/resume"
    );

    // Check if we already have this session in memory (e.g. resumed twice).
    {
        let sessions = state.lock().unwrap();
        if sessions.sessions.contains_key(&session_id) {
            return responder.respond_with_error(
                agent_client_protocol::Error::internal_error().data(serde_json::json!({
                    "error": "Session already exists"
                })),
            );
        }
    }

    match DaemonHandle::spawn_with_session_id(&req.cwd, Some(&session_id)).await {
        Ok(daemon) => {
            let daemon_state = daemon.fetch_daemon_state().await;
            let mode_state = build_session_mode_state_from_value(&daemon_state);
            let config_options = build_config_options(&daemon_state, &mode_state);

            state
                .lock()
                .unwrap()
                .sessions
                .insert(session_id.clone(), daemon);

            info!(session_id = %session_id, "Session resumed");
            let mut response = acp::ResumeSessionResponse::new();
            if let Some(ms) = &mode_state {
                response = response.modes(ms.clone());
            }
            if !config_options.is_empty() {
                response = response.config_options(config_options);
            }
            responder.respond(response)
        }
        Err(e) => {
            error!(error = %e, session_id = %session_id, "Failed to resume session");
            responder.respond_with_error(agent_client_protocol::Error::internal_error().data(
                serde_json::json!({
                    "error": "Failed to resume polytoken daemon session",
                    "detail": e.to_string(),
                }),
            ))
        }
    }
}

async fn handle_close_session(
    state: &Arc<Mutex<AgentState>>,
    req: acp::CloseSessionRequest,
    responder: agent_client_protocol::Responder<acp::CloseSessionResponse>,
    _cx: ConnectionTo<Client>,
) -> Result<(), agent_client_protocol::Error> {
    let session_id = req.session_id.0.to_string();
    info!(session_id = %session_id, "ACP session/close");

    // Remove the daemon from the map and terminate it.
    let daemon = {
        let mut sessions = state.lock().unwrap();
        sessions.sessions.remove(&session_id)
    };

    match daemon {
        Some(mut daemon) => {
            daemon.terminate().await;
            info!(session_id = %session_id, "Session closed");
        }
        None => {
            warn!(session_id = %session_id, "session/close: session not found (already closed?)");
        }
    }

    responder.respond(acp::CloseSessionResponse::new())
}

async fn handle_prompt(
    state: &Arc<Mutex<AgentState>>,
    req: acp::PromptRequest,
    responder: agent_client_protocol::Responder<acp::PromptResponse>,
    cx: ConnectionTo<Client>,
) -> Result<(), agent_client_protocol::Error> {
    let session_id = req.session_id.0.to_string();
    let prompt_text = events::extract_text(&req.prompt);

    info!(session_id = %session_id, "ACP prompt");

    // Collect connection info without holding lock across await
    let (events_url, bearer_token, base_url) = {
        let sessions = state.lock().unwrap();
        let daemon = match sessions.sessions.get(&session_id) {
            Some(d) => d,
            None => {
                error!(session_id = %session_id, "Session not found");
                return responder.respond_with_error(
                    agent_client_protocol::Error::internal_error().data(serde_json::json!({
                        "error": "Session not found"
                    })),
                );
            }
        };
        (
            daemon.events_url(),
            daemon.bearer_token().to_string(),
            daemon.base_url().to_string(),
        )
    };

    // Send prompt to daemon
    let prompt_id = match DaemonHandle::prompt_with(&base_url, &bearer_token, &prompt_text).await {
        Ok(id) => id,
        Err(e) => {
            error!(error = %e, "Failed to send prompt to daemon");
            return responder.respond_with_error(
                agent_client_protocol::Error::internal_error().data(serde_json::json!({
                    "error": "Failed to forward prompt to daemon",
                    "detail": e.to_string(),
                })),
            );
        }
    };

    info!(session_id = %session_id, prompt_id = %prompt_id, "Prompt forwarded to daemon");

    // Spawn the SSE consumer as a background task.
    // The responder is moved into the task; when the turn completes,
    // the task responds with the stop reason.
    let consumer = SseConsumer {
        conn: cx.clone(),
        session_id: session_id.clone(),
        prompt_id: prompt_id.clone(),
        events_url,
        bearer_token,
        base_url,
        responder,
    };

    cx.spawn(consumer.run())?;

    Ok(())
}

async fn handle_cancel(state: &Arc<Mutex<AgentState>>, notif: &acp::CancelNotification) {
    let session_id = notif.session_id.0.to_string();
    info!(session_id = %session_id, "ACP cancel");

    // We need to take the daemon out briefly to call cancel (which is async).
    // Since we can't hold the lock across await, we clone the necessary bits.
    let (base_url, bearer) = {
        let sessions = state.lock().unwrap();
        match sessions.sessions.get(&session_id) {
            Some(daemon) => (
                daemon.base_url().to_string(),
                daemon.bearer_token().to_string(),
            ),
            None => return,
        }
    };

    // Cancel via HTTP directly (no lock needed)
    let client = reqwest::Client::new();
    let url = format!("{}/turn/cancel", base_url);
    let _ = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", bearer))
        .send()
        .await;
}

/// Build the ACP SessionModeState from the daemon's current facet and known facets.
/// Polytoken has two facets: "execute" (default) and "plan".
fn build_session_mode_state_from_value(
    state: &Result<serde_json::Value, anyhow::Error>,
) -> Option<acp::SessionModeState> {
    let active_facet = match state {
        Ok(state) => state
            .get("active_facet")
            .and_then(|v| v.as_str())
            .unwrap_or("execute")
            .to_string(),
        Err(e) => {
            warn!(error = %e, "Failed to fetch daemon state for modes; defaulting to execute");
            "execute".to_string()
        }
    };

    let modes = vec![
        acp::SessionMode::new("execute", "Execute")
            .description("Agent executes tasks, writes files, and runs commands"),
        acp::SessionMode::new("plan", "Plan")
            .description("Agent plans and reviews before making changes"),
    ];

    Some(acp::SessionModeState::new(active_facet, modes))
}

/// Build the model `SessionConfigOption` from the daemon's `available_models` list.
///
/// ACP clients like Paseo look for a select-type config option with
/// `category: "model"` to populate their model picker. Each option value is
/// the daemon model name (`AvailableModelEntry.name`), and the label is the
/// display label (`AvailableModelEntry.label`).
fn build_config_options(
    state: &Result<serde_json::Value, anyhow::Error>,
    mode_state: &Option<acp::SessionModeState>,
) -> Vec<acp::SessionConfigOption> {
    let mut options = Vec::new();

    // Mode config option (category=mode)
    if let Some(ms) = mode_state {
        let mode_options: Vec<acp::SessionConfigSelectOption> = ms
            .available_modes
            .iter()
            .map(|m| {
                acp::SessionConfigSelectOption::new(m.id.0.as_ref().to_string(), m.name.clone())
            })
            .collect();

        if !mode_options.is_empty() {
            let current_mode = ms.current_mode_id.0.as_ref().to_string();
            let mode_option =
                acp::SessionConfigOption::select("mode", "Mode", current_mode, mode_options)
                    .category(acp::SessionConfigOptionCategory::Mode);
            options.push(mode_option);
        }
    }

    // Model config option (category=model)
    if let Some(opt) = build_model_config_option(state) {
        options.push(opt);
    }

    options
}

/// Build the model `SessionConfigOption` from the daemon's `available_models` list.
fn build_model_config_option(
    state: &Result<serde_json::Value, anyhow::Error>,
) -> Option<acp::SessionConfigOption> {
    let state = match state {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "Failed to fetch daemon state for models; skipping model config");
            return None;
        }
    };

    let active_model = state
        .get("active_model")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let available_models = state.get("available_models").and_then(|v| v.as_array())?;

    if available_models.is_empty() {
        return None;
    }

    let options: Vec<acp::SessionConfigSelectOption> = available_models
        .iter()
        .filter_map(|m| {
            let name = m.get("name").and_then(|v| v.as_str())?.to_string();
            let label = m
                .get("label")
                .and_then(|v| v.as_str())
                .unwrap_or(&name)
                .to_string();
            Some(acp::SessionConfigSelectOption::new(name, label))
        })
        .collect();

    if options.is_empty() {
        return None;
    }

    // Use the first model name if active_model is empty.
    let current_value = if active_model.is_empty() {
        options[0].value.0.to_string()
    } else {
        active_model.to_string()
    };

    Some(
        acp::SessionConfigOption::select("model", "Model", current_value, options)
            .category(acp::SessionConfigOptionCategory::Model),
    )
}

async fn handle_list_sessions(
    state: &Arc<Mutex<AgentState>>,
    _req: acp::ListSessionsRequest,
    responder: agent_client_protocol::Responder<acp::ListSessionsResponse>,
    _cx: ConnectionTo<Client>,
) -> Result<(), agent_client_protocol::Error> {
    let sessions = state.lock().unwrap();
    let session_infos: Vec<acp::SessionInfo> = sessions
        .sessions
        .iter()
        .map(|(id, daemon)| acp::SessionInfo::new(id.clone(), daemon.cwd().to_path_buf()))
        .collect();
    responder.respond(acp::ListSessionsResponse::new(session_infos))
}

async fn handle_set_session_mode(
    state: &Arc<Mutex<AgentState>>,
    req: acp::SetSessionModeRequest,
    responder: agent_client_protocol::Responder<acp::SetSessionModeResponse>,
    _cx: ConnectionTo<Client>,
) -> Result<(), agent_client_protocol::Error> {
    let session_id = req.session_id.0.to_string();
    let mode_id = req.mode_id.0.to_string();

    info!(session_id = %session_id, mode_id = %mode_id, "ACP set_session_mode");

    let result = {
        let sessions = state.lock().unwrap();
        let daemon = match sessions.sessions.get(&session_id) {
            Some(d) => d,
            None => {
                drop(sessions);
                return responder.respond_with_error(
                    agent_client_protocol::Error::internal_error()
                        .data(serde_json::json!({"error": "Session not found"})),
                );
            }
        };
        // Clone the connection info before releasing the lock
        (
            daemon.base_url().to_string(),
            daemon.bearer_token().to_string(),
        )
    }; // lock released here

    let (base_url, bearer) = result;
    let client = reqwest::Client::new();
    let url = format!("{}/facet", base_url);
    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", bearer))
        .json(&serde_json::json!({ "facet": mode_id }))
        .send()
        .await;
    let result = match resp {
        Ok(r) if r.status().is_success() => Ok(()),
        Ok(r) => Err(anyhow::anyhow!(
            "POST /facet returned status {}",
            r.status()
        )),
        Err(e) => Err(anyhow::anyhow!("Failed to POST /facet: {}", e)),
    };

    match result {
        Ok(()) => {
            info!(session_id = %session_id, mode_id = %mode_id, "Facet switched");
            responder.respond(acp::SetSessionModeResponse::new())
        }
        Err(e) => {
            error!(error = %e, "Failed to switch facet");
            responder.respond_with_error(agent_client_protocol::Error::internal_error().data(
                serde_json::json!({
                    "error": "Failed to switch facet",
                    "detail": e.to_string(),
                }),
            ))
        }
    }
}

async fn handle_set_session_config_option(
    state: &Arc<Mutex<AgentState>>,
    req: acp::SetSessionConfigOptionRequest,
    responder: agent_client_protocol::Responder<acp::SetSessionConfigOptionResponse>,
    _cx: ConnectionTo<Client>,
) -> Result<(), agent_client_protocol::Error> {
    let session_id = req.session_id.0.to_string();
    let config_id = req.config_id.0.to_string();
    let value = req.value.as_value_id().map(|v| v.0.as_ref().to_string());

    info!(
        session_id = %session_id,
        config_id = %config_id,
        value = ?value,
        "ACP set_session_config_option"
    );

    let Some(value) = value else {
        warn!("Expected value-id for config option; ignoring");
        return responder.respond(acp::SetSessionConfigOptionResponse::new(vec![]));
    };

    let (base_url, bearer) = {
        let sessions = state.lock().unwrap();
        match sessions.sessions.get(&session_id) {
            Some(d) => (d.base_url().to_string(), d.bearer_token().to_string()),
            None => {
                return responder.respond_with_error(
                    agent_client_protocol::Error::internal_error()
                        .data(serde_json::json!({"error": "Session not found"})),
                );
            }
        }
    };

    // Route to the appropriate daemon endpoint based on config_id.
    let (endpoint, payload, label) = match config_id.as_str() {
        "model" => (
            format!("{}/model", base_url),
            serde_json::json!({ "model": value }),
            "model",
        ),
        "mode" => (
            format!("{}/facet", base_url),
            serde_json::json!({ "facet": value }),
            "facet/mode",
        ),
        _ => {
            warn!(config_id = %config_id, "Unsupported config option; ignoring");
            return responder.respond(acp::SetSessionConfigOptionResponse::new(vec![]));
        }
    };

    let client = reqwest::Client::new();
    let resp = client
        .post(&endpoint)
        .header("Authorization", format!("Bearer {}", bearer))
        .json(&payload)
        .send()
        .await;

    match resp {
        Ok(r) if r.status().is_success() => {
            info!(session_id = %session_id, label = %label, value = %value, "Config option set");
            responder.respond(acp::SetSessionConfigOptionResponse::new(vec![]))
        }
        Ok(r) => {
            let status = r.status();
            let body = r.text().await.unwrap_or_default();
            error!(status = %status, body = %body, label = %label, "Failed to set config option");
            responder.respond_with_error(agent_client_protocol::Error::internal_error().data(
                serde_json::json!({
                    "error": format!("Failed to set {}", label),
                    "detail": format!("POST {} returned status {}: {}", endpoint, status, body),
                }),
            ))
        }
        Err(e) => {
            error!(error = %e, label = %label, "Failed to POST");
            responder.respond_with_error(agent_client_protocol::Error::internal_error().data(
                serde_json::json!({
                    "error": format!("Failed to set {}", label),
                    "detail": e.to_string(),
                }),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// SSE Consumer
// ---------------------------------------------------------------------------

/// The SSE consumer task that bridges daemon events to ACP notifications.
struct SseConsumer {
    conn: ConnectionTo<Client>,
    session_id: String,
    prompt_id: String,
    events_url: String,
    bearer_token: String,
    base_url: String,
    responder: agent_client_protocol::Responder<acp::PromptResponse>,
}

impl SseConsumer {
    async fn run(self) -> Result<(), agent_client_protocol::Error> {
        let max_retries = 3;
        let mut retry_count = 0;

        let conn = self.conn;
        let session_id = self.session_id;
        let prompt_id = self.prompt_id;
        let events_url = self.events_url;
        let bearer_token = self.bearer_token;
        let base_url = self.base_url;
        let mut responder = Some(self.responder);

        loop {
            match connect_and_consume(
                &events_url,
                &bearer_token,
                &base_url,
                &prompt_id,
                &session_id,
                &conn,
            )
            .await
            {
                ConsumeOutcome::Done(stop_reason) => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    info!(
                        prompt_id = %prompt_id,
                        stop_reason = ?stop_reason,
                        "Prompt turn complete"
                    );
                    return responder
                        .take()
                        .expect("responder already consumed")
                        .respond(acp::PromptResponse::new(stop_reason));
                }
                ConsumeOutcome::Error => {
                    retry_count += 1;
                    if retry_count >= max_retries {
                        error!(
                            prompt_id = %prompt_id,
                            retries = retry_count,
                            "SSE consumer exhausted retries; ending turn"
                        );
                        return responder
                            .take()
                            .expect("responder already consumed")
                            .respond(acp::PromptResponse::new(acp::StopReason::EndTurn));
                    }
                    warn!(
                        prompt_id = %prompt_id,
                        retry = retry_count,
                        "SSE connection error; retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                ConsumeOutcome::Continue => {}
            }
        }
    }
}

enum ConsumeOutcome {
    Continue,
    Done(acp::StopReason),
    Error,
}

/// Connect to SSE stream and consume events until the turn ends or an error occurs.
async fn connect_and_consume(
    events_url: &str,
    bearer_token: &str,
    base_url: &str,
    prompt_id: &str,
    session_id: &str,
    conn: &ConnectionTo<Client>,
) -> ConsumeOutcome {
    let client = reqwest::Client::new();
    let response = client
        .get(events_url)
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Accept", "text/event-stream")
        .send()
        .await;

    let response = match response {
        Ok(r) if r.status().is_success() => r,
        Ok(r) => {
            error!(status = %r.status(), "SSE connection returned non-200");
            return ConsumeOutcome::Error;
        }
        Err(e) => {
            error!(error = %e, "SSE connection failed");
            return ConsumeOutcome::Error;
        }
    };

    debug!("SSE stream connected");

    use futures::StreamExt;
    let mut stream = response.bytes_stream();
    let mut parser = SseParser::new();

    while let Some(chunk_result) = stream.next().await {
        let chunk = match chunk_result {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "SSE stream error");
                return ConsumeOutcome::Error;
            }
        };

        let data_lines = parser.feed(&chunk);
        for data in data_lines {
            match process_sse_event(&data, prompt_id, session_id, base_url, bearer_token, conn)
                .await
            {
                ConsumeOutcome::Done(reason) => return ConsumeOutcome::Done(reason),
                ConsumeOutcome::Error => return ConsumeOutcome::Error,
                ConsumeOutcome::Continue => {}
            }
        }
    }

    // Stream ended without message_complete
    warn!(prompt_id = %prompt_id, "SSE stream ended without explicit turn end");
    ConsumeOutcome::Done(acp::StopReason::EndTurn)
}

/// Process a single SSE event and translate it into ACP actions.
async fn process_sse_event(
    data: &str,
    prompt_id: &str,
    session_id: &str,
    base_url: &str,
    bearer_token: &str,
    conn: &ConnectionTo<Client>,
) -> ConsumeOutcome {
    let event = match events::parse_sse_event(data) {
        Some(e) => e,
        None => {
            debug!(data = %data.chars().take(200).collect::<String>(), "Failed to parse SSE event; skipping");
            return ConsumeOutcome::Continue;
        }
    };

    debug!(event_type = ?std::mem::discriminant(&event), "SSE event received");

    // Filter by prompt_id if the event has one
    if let Some(epid) = events::event_prompt_id(&event)
        && epid != prompt_id
    {
        return ConsumeOutcome::Continue;
    }

    match events::translate_event(&event) {
        EventTranslation::Update(update) => {
            let sid = acp::SessionId::new(session_id.to_string());
            let notification = acp::SessionNotification::new(sid, update.clone());
            if let Err(e) = conn.send_notification(notification) {
                error!(error = %e, "Failed to send session notification");
            }
            ConsumeOutcome::Continue
        }
        EventTranslation::TurnEnd => ConsumeOutcome::Done(acp::StopReason::EndTurn),
        EventTranslation::TurnCancelled => ConsumeOutcome::Done(acp::StopReason::Cancelled),
        EventTranslation::PermissionRequest {
            interrogative_id,
            question,
        } => {
            handle_permission(
                conn,
                &interrogative_id,
                &question,
                session_id,
                base_url,
                bearer_token,
            )
            .await;
            ConsumeOutcome::Continue
        }
        EventTranslation::AskUserQuestion {
            interrogative_id,
            payload,
        } => {
            handle_ask_user_question(conn, &interrogative_id, &payload, base_url, bearer_token)
                .await;
            ConsumeOutcome::Continue
        }
        EventTranslation::Ignore => ConsumeOutcome::Continue,
    }
}

/// Forward a permission request to the ACP client and relay the response to the daemon.
async fn handle_permission(
    conn: &ConnectionTo<Client>,
    interrogative_id: &str,
    question: &str,
    session_id: &str,
    base_url: &str,
    bearer_token: &str,
) {
    info!(interrogative_id = %interrogative_id, "Forwarding permission request to ACP client");

    let options = events::build_permission_options();
    let tool_call_update = acp::ToolCallUpdate::new(
        interrogative_id.to_string(),
        acp::ToolCallUpdateFields::new()
            .title(question.to_string())
            .status(acp::ToolCallStatus::Pending),
    );

    let sid = acp::SessionId::new(session_id.to_string());
    let request = acp::RequestPermissionRequest::new(sid, tool_call_update, options);

    let base_url = base_url.to_string();
    let bearer_token = bearer_token.to_string();
    let interrogative_id = interrogative_id.to_string();

    let sent = conn.send_request(request);
    sent.on_receiving_result(async move |result| {
        let granted = match result {
            Ok(response) => events::resolve_permission_outcome(&response.outcome),
            Err(e) => {
                error!(error = %e, "Permission request failed");
                false
            }
        };

        info!(interrogative_id = %interrogative_id, granted, "Permission response from client");

        if let Err(e) =
            respond_interrogative_permission(&base_url, &bearer_token, &interrogative_id, granted)
                .await
        {
            error!(error = %e, "Failed to respond to interrogative on daemon");
        }

        Ok(())
    })
    .expect("on_receiving_result failed");
}

/// Forward an ask_user_question to the ACP client via ext_method and relay answers.
async fn handle_ask_user_question(
    conn: &ConnectionTo<Client>,
    interrogative_id: &str,
    payload: &AskUserQuestionPayload,
    base_url: &str,
    bearer_token: &str,
) {
    info!(
        interrogative_id = %interrogative_id,
        question_count = payload.questions.len(),
        "Forwarding ask_user_question to ACP client"
    );

    let request_json = serde_json::json!({
        "interrogative_id": interrogative_id,
        "questions": payload.questions,
    });

    let params = match serde_json::value::RawValue::from_string(request_json.to_string()) {
        Ok(raw) => std::sync::Arc::from(raw),
        Err(e) => {
            error!(error = %e, "Failed to serialize ask_user_question params");
            let _ = cancel_interrogative(base_url, bearer_token, interrogative_id).await;
            return;
        }
    };

    let ext_request = acp::AgentRequest::ExtMethodRequest(acp::ExtRequest::new(
        "polytoken/ask_user_question",
        params,
    ));

    let base_url = base_url.to_string();
    let bearer_token = bearer_token.to_string();
    let interrogative_id = interrogative_id.to_string();

    let sent = conn.send_request(ext_request);
    sent.on_receiving_result(async move |result| {
        match result {
            Ok(response_value) => {
                let answers = parse_ask_user_question_answers_value(&response_value);

                if answers.is_empty() {
                    warn!(
                        interrogative_id = %interrogative_id,
                        "No answers from ACP client; cancelling interrogative"
                    );
                    let _ = cancel_interrogative(&base_url, &bearer_token, &interrogative_id).await;
                    return Ok(());
                }

                info!(
                    interrogative_id = %interrogative_id,
                    answer_count = answers.len(),
                    "Answers received from ACP client"
                );

                if let Err(e) =
                    respond_ask_user_question(&base_url, &bearer_token, &interrogative_id, &answers)
                        .await
                {
                    error!(error = %e, "Failed to respond to ask_user_question on daemon");
                }
            }
            Err(e) => {
                warn!(
                    error = %e,
                    interrogative_id = %interrogative_id,
                    "ACP client does not support ask_user_question or request failed; cancelling"
                );
                let _ = cancel_interrogative(&base_url, &bearer_token, &interrogative_id).await;
            }
        }

        Ok(())
    })
    .expect("on_receiving_result failed");
}

// ---------------------------------------------------------------------------
// Daemon HTTP helpers (standalone functions for use in spawned tasks)
// ---------------------------------------------------------------------------

async fn respond_interrogative_permission(
    base_url: &str,
    bearer_token: &str,
    interrogative_id: &str,
    granted: bool,
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/interrogative/{}/respond", base_url, interrogative_id);
    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", bearer_token))
        .json(&serde_json::json!({"kind": "permission_answer", "granted": granted}))
        .send()
        .await?;
    if !resp.status().is_success() {
        warn!(status = %resp.status(), "Interrogative response failed");
    }
    Ok(())
}

/// Parse the ext_method response from the ACP client into answer replies.
fn parse_ask_user_question_answers_value(value: &serde_json::Value) -> Vec<serde_json::Value> {
    match value {
        serde_json::Value::Object(obj) => {
            if let Some(serde_json::Value::Array(answers)) = obj.get("answers") {
                answers.clone()
            } else {
                Vec::new()
            }
        }
        _ => Vec::new(),
    }
}

/// POST `{"kind": "ask_user_question_answers", "answers": [...]}` to the daemon.
async fn respond_ask_user_question(
    base_url: &str,
    bearer_token: &str,
    interrogative_id: &str,
    answers: &[serde_json::Value],
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/interrogative/{}/respond", base_url, interrogative_id);
    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", bearer_token))
        .json(&serde_json::json!({
            "kind": "ask_user_question_answers",
            "answers": answers
        }))
        .send()
        .await?;
    if !resp.status().is_success() {
        warn!(status = %resp.status(), "ask_user_question response failed");
    }
    Ok(())
}

/// POST `{"kind": "cancel"}` to the daemon to cancel a pending interrogative.
async fn cancel_interrogative(
    base_url: &str,
    bearer_token: &str,
    interrogative_id: &str,
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/interrogative/{}/respond", base_url, interrogative_id);
    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", bearer_token))
        .json(&serde_json::json!({"kind": "cancel"}))
        .send()
        .await?;
    if !resp.status().is_success() {
        warn!(status = %resp.status(), "Interrogative cancel failed");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// SSE line parser
// ---------------------------------------------------------------------------

/// Simple SSE event parser: feeds bytes, yields complete `data:` payloads.
struct SseParser {
    buffer: String,
}

impl SseParser {
    fn new() -> Self {
        Self {
            buffer: String::new(),
        }
    }

    fn feed(&mut self, bytes: &[u8]) -> Vec<String> {
        let mut results = Vec::new();
        self.buffer.push_str(&String::from_utf8_lossy(bytes));

        while let Some(pos) = self.buffer.find("\n\n") {
            let event_str: String = self.buffer.drain(..pos + 2).collect();

            // Extract data lines from the event
            let data: String = event_str
                .lines()
                .filter_map(|l| {
                    l.strip_prefix("data:")
                        .map(|s| s.strip_prefix(' ').unwrap_or(s))
                })
                .collect::<Vec<_>>()
                .join("\n");

            if !data.is_empty() {
                results.push(data);
            }
        }

        results
    }
}
