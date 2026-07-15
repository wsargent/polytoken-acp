//! ACP Agent implementation for the polytoken daemon shim.
//!
//! In ACP 1.x, the agent is built using a builder pattern with
//! `on_receive_request` / `on_receive_notification` handlers rather than
//! implementing a trait. The shared session state (daemon handles) lives in
//! an `Arc<Mutex<>>` captured by each handler closure.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use agent_client_protocol::schema::v1 as acp;
use agent_client_protocol::{
    Agent, Client, ConnectionTo, Dispatch, Stdio, on_receive_dispatch, on_receive_notification,
    on_receive_request,
};
use anyhow::{Context, bail};
use tracing::{debug, error, info, warn};

use crate::daemon::DaemonHandle;
use crate::events::{self, AskUserQuestionPayload, EventTranslation};
use crate::history;

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
                let mut ext_meta = serde_json::Map::new();
                ext_meta.insert(
                    "polytoken".to_string(),
                    serde_json::json!({
                        "ask_user_question": true,
                        "system_reminder": true,
                        "subagent_started": true,
                        "subagent_completed": true,
                        "job_promoted": true,
                        "job_completed": true,
                        "job_expiring": true,
                        "job_cancelled": true,
                        "job_updated": true,
                    }),
                );
                let caps = acp::AgentCapabilities::new()
                    .load_session(true)
                    .prompt_capabilities(acp::PromptCapabilities::new().embedded_context(true))
                    .mcp_capabilities(acp::McpCapabilities::new())
                    .session_capabilities(
                        acp::SessionCapabilities::new()
                            .list(acp::SessionListCapabilities::new())
                            .resume(acp::SessionResumeCapabilities::new())
                            .close(acp::SessionCloseCapabilities::new()),
                    )
                    .meta(ext_meta);
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
        // session/load
        .on_receive_request(
            {
                let state = Arc::clone(&state);
                async move |req: acp::LoadSessionRequest, responder, cx| {
                    handle_load_session(&state, req, responder, cx).await
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

    // Convert ACP-provided MCP servers into a polytoken config file so the
    // daemon picks them up at startup via --project-config-dir.
    let mcp_config_dir = if !req.mcp_servers.is_empty() {
        let session_temp =
            std::env::temp_dir().join(format!("polytoken-acp-mcp-{:x}", rand::random::<u64>()));
        match write_mcp_servers_config(&req.mcp_servers, &session_temp) {
            Some(dir) => {
                info!(
                    server_count = req.mcp_servers.len(),
                    config_dir = ?dir,
                    "Forwarding MCP servers to daemon"
                );
                Some(dir)
            }
            None => {
                warn!(
                    count = req.mcp_servers.len(),
                    "MCP servers provided but config generation failed; not forwarding"
                );
                None
            }
        }
    } else {
        None
    };

    match DaemonHandle::spawn_with_session_id(&req.cwd, None, mcp_config_dir.as_deref()).await {
        Ok(mut daemon) => {
            let session_id = daemon.session_id().to_string();

            // Fetch daemon state once, then build modes and config options from it.
            let daemon_state = daemon.fetch_daemon_state().await;
            let permission_monitor = match fetch_permission_monitor_raw(
                daemon.base_url(),
                daemon.bearer_token(),
            )
            .await
            {
                Some(v) => Ok(v),
                None => Err(anyhow::anyhow!("permission-monitor fetch failed")),
            };
            let mode_state = build_session_mode_from_permission_monitor(&permission_monitor);
            let config_options =
                build_config_options(&daemon_state, &mode_state, &permission_monitor);

            if let Some(dir) = &mcp_config_dir {
                daemon.set_mcp_config_dir(dir.clone());
            }

            state
                .lock()
                .unwrap()
                .sessions
                .insert(session_id.clone(), daemon);

            info!(session_id = %session_id, "New session created");

            // Build and send the session/new response FIRST, before any
            // notifications. ACP clients (e.g. Paseo) drop session/update
            // notifications whose sessionId doesn't match the session they
            // know about — and they only learn the session ID from the
            // session/new response. If notifications are sent before the
            // response, the client hasn't recorded the session ID yet and
            // discards them (including available_commands_update, which
            // makes slash commands never appear).
            let mut response = acp::NewSessionResponse::new(session_id.clone());
            if let Some(ms) = &mode_state {
                response = response.modes(ms.clone());
            }
            if !config_options.is_empty() {
                response = response.config_options(config_options);
            }
            let respond_result = responder.respond(response);

            // Now send notifications — the client has the session ID from the
            // response above and will accept these.

            // Send session_info_update with the title from the daemon state.
            if let Ok(ref ds) = daemon_state
                && let Some(title) = ds.get("session_title").and_then(|v| v.as_str())
                && !title.is_empty()
            {
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

            // Send available_commands_update with the daemon's slash commands and skills.
            {
                let mut all_commands = build_available_commands(&req.cwd).unwrap_or_default();
                let skill_commands = build_skill_commands(&daemon_state);
                all_commands.extend(skill_commands);
                if !all_commands.is_empty() {
                    let sid = acp::SessionId::new(session_id.clone());
                    let notification = acp::SessionNotification::new(
                        sid,
                        acp::SessionUpdate::AvailableCommandsUpdate(
                            acp::AvailableCommandsUpdate::new(all_commands),
                        ),
                    );
                    if let Err(e) = cx.send_notification(notification) {
                        warn!(error = %e, "Failed to send available_commands_update notification");
                    }
                }
            }

            // Send initial Plan (todos) from daemon state.
            if let Ok(ref ds) = daemon_state
                && let Some(plan) = events::build_plan_from_state(ds)
            {
                let sid = acp::SessionId::new(session_id.clone());
                let notification =
                    acp::SessionNotification::new(sid, acp::SessionUpdate::Plan(plan));
                if let Err(e) = cx.send_notification(notification) {
                    warn!(error = %e, "Failed to send initial Plan notification");
                }
            }

            respond_result
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
    cx: ConnectionTo<Client>,
) -> Result<(), agent_client_protocol::Error> {
    let session_id = req.session_id.0.to_string();

    info!(
        session_id = %session_id,
        cwd = ?req.cwd,
        "ACP session/resume"
    );

    // Convert ACP-provided MCP servers into a polytoken config file.
    let mcp_config_dir = if !req.mcp_servers.is_empty() {
        let session_temp =
            std::env::temp_dir().join(format!("polytoken-acp-mcp-{:x}", rand::random::<u64>()));
        match write_mcp_servers_config(&req.mcp_servers, &session_temp) {
            Some(dir) => {
                info!(
                    server_count = req.mcp_servers.len(),
                    config_dir = ?dir,
                    "Forwarding MCP servers to daemon"
                );
                Some(dir)
            }
            None => {
                warn!(
                    count = req.mcp_servers.len(),
                    "MCP servers provided but config generation failed; not forwarding"
                );
                None
            }
        }
    } else {
        None
    };

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

    match DaemonHandle::spawn_with_session_id(
        &req.cwd,
        Some(&session_id),
        mcp_config_dir.as_deref(),
    )
    .await
    {
        Ok(mut daemon) => {
            let daemon_state = daemon.fetch_daemon_state().await;
            let permission_monitor = match fetch_permission_monitor_raw(
                daemon.base_url(),
                daemon.bearer_token(),
            )
            .await
            {
                Some(v) => Ok(v),
                None => Err(anyhow::anyhow!("permission-monitor fetch failed")),
            };
            let mode_state = build_session_mode_from_permission_monitor(&permission_monitor);
            let config_options =
                build_config_options(&daemon_state, &mode_state, &permission_monitor);

            if let Some(dir) = &mcp_config_dir {
                daemon.set_mcp_config_dir(dir.clone());
            }

            state
                .lock()
                .unwrap()
                .sessions
                .insert(session_id.clone(), daemon);

            // Push session state to the client so resumed sessions behave like
            // new sessions — without these notifications the client sees a blank
            // slate even though the daemon loaded saved history.

            // Send session_info_update with the title from the daemon state.
            if let Ok(ref ds) = daemon_state
                && let Some(title) = ds.get("session_title").and_then(|v| v.as_str())
                && !title.is_empty()
            {
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

            // Send available_commands_update with the daemon's slash commands and skills.
            {
                let mut all_commands = build_available_commands(&req.cwd).unwrap_or_default();
                let skill_commands = build_skill_commands(&daemon_state);
                all_commands.extend(skill_commands);
                if !all_commands.is_empty() {
                    let sid = acp::SessionId::new(session_id.clone());
                    let notification = acp::SessionNotification::new(
                        sid,
                        acp::SessionUpdate::AvailableCommandsUpdate(
                            acp::AvailableCommandsUpdate::new(all_commands),
                        ),
                    );
                    if let Err(e) = cx.send_notification(notification) {
                        warn!(error = %e, "Failed to send available_commands_update notification");
                    }
                }
            }

            // Send initial Plan (todos) from daemon state.
            if let Ok(ref ds) = daemon_state
                && let Some(plan) = events::build_plan_from_state(ds)
            {
                let sid = acp::SessionId::new(session_id.clone());
                let notification =
                    acp::SessionNotification::new(sid, acp::SessionUpdate::Plan(plan));
                if let Err(e) = cx.send_notification(notification) {
                    warn!(error = %e, "Failed to send initial Plan notification");
                }
            }

            info!(session_id = %session_id, "Session resumed");

            // Respond first so the client has the session ID before
            // notifications arrive (same ordering rationale as new_session).
            let mut response = acp::ResumeSessionResponse::new();
            if let Some(ms) = &mode_state {
                response = response.modes(ms.clone());
            }
            if !config_options.is_empty() {
                response = response.config_options(config_options);
            }
            let respond_result = responder.respond(response);

            // Send session_info_update with the title from the daemon state.
            if let Ok(ref ds) = daemon_state
                && let Some(title) = ds.get("session_title").and_then(|v| v.as_str())
                && !title.is_empty()
            {
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

            // Send available_commands_update with the daemon's slash commands and skills.
            {
                let mut all_commands = build_available_commands(&req.cwd).unwrap_or_default();
                let skill_commands = build_skill_commands(&daemon_state);
                all_commands.extend(skill_commands);
                if !all_commands.is_empty() {
                    let sid = acp::SessionId::new(session_id.clone());
                    let notification = acp::SessionNotification::new(
                        sid,
                        acp::SessionUpdate::AvailableCommandsUpdate(
                            acp::AvailableCommandsUpdate::new(all_commands),
                        ),
                    );
                    if let Err(e) = cx.send_notification(notification) {
                        warn!(error = %e, "Failed to send available_commands_update notification");
                    }
                }
            }

            // Send initial Plan (todos) from daemon state.
            if let Ok(ref ds) = daemon_state
                && let Some(plan) = events::build_plan_from_state(ds)
            {
                let sid = acp::SessionId::new(session_id.clone());
                let notification =
                    acp::SessionNotification::new(sid, acp::SessionUpdate::Plan(plan));
                if let Err(e) = cx.send_notification(notification) {
                    warn!(error = %e, "Failed to send initial Plan notification");
                }
            }

            respond_result
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

/// Handle `session/load` — spawn a daemon with `--resume`, fetch and replay
/// history from `GET /history` as ACP session notifications, then return
/// modes and config_options in the `LoadSessionResponse`.
///
/// This handler mirrors `handle_resume_session` but additionally:
/// 1. Fetches conversation history from the daemon's `/history` endpoint.
/// 2. Translates each history item into ACP `SessionUpdate` notifications.
/// 3. Sends those notifications to the client before responding.
///
/// Paseo (`acp-agent.ts:1400-1411`) calls `session/load` when
/// `loadSession: true` is advertised, captures all notifications into
/// `persistedHistory` during `replayingHistory`, then replays them via
/// `streamHistory()` to populate the timeline.
async fn handle_load_session(
    state: &Arc<Mutex<AgentState>>,
    req: acp::LoadSessionRequest,
    responder: agent_client_protocol::Responder<acp::LoadSessionResponse>,
    cx: ConnectionTo<Client>,
) -> Result<(), agent_client_protocol::Error> {
    let session_id = req.session_id.0.to_string();

    info!(session_id = %session_id, cwd = ?req.cwd, "ACP session/load");

    // Convert ACP-provided MCP servers into a polytoken config file.
    let mcp_config_dir = if !req.mcp_servers.is_empty() {
        let session_temp =
            std::env::temp_dir().join(format!("polytoken-acp-mcp-{:x}", rand::random::<u64>()));
        match write_mcp_servers_config(&req.mcp_servers, &session_temp) {
            Some(dir) => {
                info!(
                    server_count = req.mcp_servers.len(),
                    config_dir = ?dir,
                    "Forwarding MCP servers to daemon"
                );
                Some(dir)
            }
            None => {
                warn!(
                    count = req.mcp_servers.len(),
                    "MCP servers provided but config generation failed; not forwarding"
                );
                None
            }
        }
    } else {
        None
    };

    // Check if we already have this session in memory.
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

    match DaemonHandle::spawn_with_session_id(
        &req.cwd,
        Some(&session_id),
        mcp_config_dir.as_deref(),
    )
    .await
    {
        Ok(mut daemon) => {
            let daemon_state = daemon.fetch_daemon_state().await;
            let permission_monitor = match fetch_permission_monitor_raw(
                daemon.base_url(),
                daemon.bearer_token(),
            )
            .await
            {
                Some(v) => Ok(v),
                None => Err(anyhow::anyhow!("permission-monitor fetch failed")),
            };
            let mode_state = build_session_mode_from_permission_monitor(&permission_monitor);
            let config_options =
                build_config_options(&daemon_state, &mode_state, &permission_monitor);

            // Fetch and replay history BEFORE inserting the daemon into the
            // state map. This avoids a re-lock after insertion and matches
            // the pre-insert pattern used by handle_new_session and
            // handle_resume_session, which call fetch_daemon_state on the
            // owned daemon before inserting it.
            let (base_url, bearer_token) = (
                daemon.base_url().to_string(),
                daemon.bearer_token().to_string(),
            );
            match fetch_history_raw(&base_url, &bearer_token).await {
                Ok(history_items) => {
                    let notifications =
                        history::translate_history_to_notifications(&history_items, &session_id);
                    for notification in &notifications {
                        if let Err(e) = cx.send_notification(notification.clone()) {
                            warn!(error = %e, "Failed to send history notification");
                        }
                    }
                    info!(
                        session_id = %session_id,
                        history_count = history_items.len(),
                        sent_notifications = notifications.len(),
                        "History replayed"
                    );
                }
                Err(e) => {
                    warn!(error = %e, "Failed to fetch history; continuing without replay");
                }
            }

            // Insert the daemon into the state map (after history fetch).
            if let Some(dir) = &mcp_config_dir {
                daemon.set_mcp_config_dir(dir.clone());
            }

            state
                .lock()
                .unwrap()
                .sessions
                .insert(session_id.clone(), daemon);

            // Push session state to the client so loaded sessions behave like
            // new/resumed sessions. These blocks are copied verbatim from
            // handle_resume_session to avoid refactoring existing working
            // handlers — a future cleanup can deduplicate into a helper.

            // Send session_info_update with the title from the daemon state.
            if let Ok(ref ds) = daemon_state
                && let Some(title) = ds.get("session_title").and_then(|v| v.as_str())
                && !title.is_empty()
            {
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

            // Send available_commands_update with the daemon's slash commands and skills.
            {
                let mut all_commands = build_available_commands(&req.cwd).unwrap_or_default();
                let skill_commands = build_skill_commands(&daemon_state);
                all_commands.extend(skill_commands);
                if !all_commands.is_empty() {
                    let sid = acp::SessionId::new(session_id.clone());
                    let notification = acp::SessionNotification::new(
                        sid,
                        acp::SessionUpdate::AvailableCommandsUpdate(
                            acp::AvailableCommandsUpdate::new(all_commands),
                        ),
                    );
                    if let Err(e) = cx.send_notification(notification) {
                        warn!(error = %e, "Failed to send available_commands_update notification");
                    }
                }
            }

            // Send initial Plan (todos) from daemon state.
            if let Ok(ref ds) = daemon_state
                && let Some(plan) = events::build_plan_from_state(ds)
            {
                let sid = acp::SessionId::new(session_id.clone());
                let notification =
                    acp::SessionNotification::new(sid, acp::SessionUpdate::Plan(plan));
                if let Err(e) = cx.send_notification(notification) {
                    warn!(error = %e, "Failed to send initial Plan notification");
                }
            }

            info!(session_id = %session_id, "Session loaded");
            let mut response = acp::LoadSessionResponse::new();
            if let Some(ms) = &mode_state {
                response = response.modes(ms.clone());
            }
            if !config_options.is_empty() {
                response = response.config_options(config_options);
            }
            responder.respond(response)
        }
        Err(e) => {
            error!(error = %e, session_id = %session_id, "Failed to load session");
            responder.respond_with_error(agent_client_protocol::Error::internal_error().data(
                serde_json::json!({
                    "error": "Failed to load polytoken daemon session",
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
    tracing::info!(
        target: "polytoken_acp::conv",
        session_id = %session_id,
        prompt_len = prompt_text.len(),
        prompt_preview = %prompt_text.chars().take(200).collect::<String>(),
        "prompt_start"
    );

    // Collect connection info without holding lock across await
    let (events_url, bearer_token, base_url, cwd) = {
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
            daemon.cwd().to_path_buf(),
        )
    };

    // Translate `/skillname` → `@skill:skillname` for known skills.
    let prompt_text = translate_skill_invocations(&prompt_text, &cwd);

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
        cwd,
        responder,
    };

    cx.spawn(consumer.run())?;

    Ok(())
}

async fn handle_cancel(state: &Arc<Mutex<AgentState>>, notif: &acp::CancelNotification) {
    let session_id = notif.session_id.0.to_string();
    info!(session_id = %session_id, "ACP cancel");

    tracing::info!(
        target: "polytoken_acp::conv",
        session_id = %session_id,
        "cancel"
    );

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

/// Build the ACP SessionModeState from the daemon's permission monitor state.
///
/// The daemon exposes 4 permission monitor modes: standard, bypass,
/// bypass_plus, autonomous. ACP "mode" maps to the permission monitor,
/// matching how other ACP providers (e.g. Claude Code) expose permission
/// tiers as modes.
fn build_session_mode_from_permission_monitor(
    permission_monitor: &Result<serde_json::Value, anyhow::Error>,
) -> Option<acp::SessionModeState> {
    let pm = match permission_monitor {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "Failed to fetch /permission-monitor for modes; defaulting to standard");
            return Some(acp::SessionModeState::new(
                "standard",
                permission_monitor_modes(),
            ));
        }
    };

    let current = pm
        .get("monitor")
        .and_then(|m| m.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("standard")
        .to_string();

    Some(acp::SessionModeState::new(
        current,
        permission_monitor_modes(),
    ))
}

/// The four permission monitor modes advertised as ACP SessionModes.
fn permission_monitor_modes() -> Vec<acp::SessionMode> {
    vec![
        acp::SessionMode::new("standard", "Standard")
            .description("Default permission prompts for tool calls"),
        acp::SessionMode::new("bypass", "Bypass").description("Skip permission prompts"),
        acp::SessionMode::new("bypass_plus", "Bypass+").description("Enhanced bypass mode"),
        acp::SessionMode::new("autonomous", "Autonomous")
            .description("Autonomous classifier-based permissions"),
    ]
}

/// Build the model `SessionConfigOption` from the daemon's `available_models` list.
///
/// ACP clients like Paseo look for a select-type config option with
/// `category: "model"` to populate their model picker. Each option value is
/// the daemon model name (`AvailableModelEntry.name`), and the label is the
/// display label (`AvailableModelEntry.label`).
fn build_config_options(
    state: &Result<serde_json::Value, anyhow::Error>,
    _mode_state: &Option<acp::SessionModeState>,
    _permission_monitor: &Result<serde_json::Value, anyhow::Error>,
) -> Vec<acp::SessionConfigOption> {
    let mut options = Vec::new();

    // Model config option (category=model)
    if let Some(opt) = build_model_config_option(state) {
        options.push(opt);
    }

    // Thought level config option (category=thought_level)
    if let Some(opt) = build_thought_level_config_option(state) {
        options.push(opt);
    }

    // MCP server config options (one boolean toggle per server)
    options.extend(build_mcp_config_options(state));

    options
}

/// Scan a directory for `*.md` facet files and insert their names (filename
/// minus `.md`) into the given map. Silently ignores non-existent or
/// unreadable directories.
fn scan_facets_dir(dir: &std::path::Path, facets: &mut std::collections::BTreeMap<String, ()>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().is_none_or(|ext| ext != "md") {
            continue;
        }
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            facets.insert(stem.to_string(), ());
        }
    }
}

/// Discover all available facets by combining three sources:
///
/// | Source | How |
/// |---|---|
/// | Shipped facets | `polytoken vfs ls polytoken://facets` |
/// | Project facets | scan `<cwd>/.polytoken/facets/*.md` |
/// | Global facets | scan `~/.config/polytoken/facets/*.md` |
///
/// The filename minus `.md` is the facet name. All three sources are unioned
/// and deduplicated (custom facets override shipped facets of the same name).
/// Returns a sorted `Vec<String>`.
fn discover_facets(cwd: &std::path::Path) -> Vec<String> {
    let mut facets = std::collections::BTreeMap::new();

    // 1. Shipped facets from VFS.
    if let Ok(output) = std::process::Command::new("polytoken")
        .arg("vfs")
        .arg("ls")
        .arg("polytoken://facets")
        .output()
        && output.status.success()
        && let Ok(text) = std::str::from_utf8(&output.stdout)
    {
        for line in text.lines() {
            let line = line.trim();
            if let Some(name) = line.strip_suffix(".md") {
                facets.insert(name.to_string(), ());
            }
        }
    }

    // 2. Project facets from <cwd>/.polytoken/facets/*.md
    scan_facets_dir(&cwd.join(".polytoken").join("facets"), &mut facets);

    // 3. Global facets from ~/.config/polytoken/facets/*.md
    if let Ok(home) = std::env::var("HOME") {
        let global_dir = std::path::Path::new(&home)
            .join(".config")
            .join("polytoken")
            .join("facets");
        scan_facets_dir(&global_dir, &mut facets);
    }

    facets.into_keys().collect()
}

/// Build the ACP available commands list from `polytoken print-slash-commands`.
///
/// Only commands that the daemon can actually execute when received as a
/// prompt are advertised. TUI-only commands (help, refresh, quit, theme,
/// etc.) are filtered out since they can't work over ACP.
///
/// For the `/facet` command, `_meta.polytoken.choices` is populated with the
/// list of available facet names discovered from the filesystem, so ACP
/// clients (e.g. Paseo) can render autocomplete suggestions.
fn build_available_commands(cwd: &std::path::Path) -> Option<Vec<acp::AvailableCommand>> {
    // Commands that make sense in an ACP context — the daemon can handle
    // these when they appear in a prompt. TUI-only commands are excluded.
    const ACP_SAFE_COMMANDS: &[&str] = &[
        "/clear",
        "/compact",
        "/daemon-reload",
        "/facet",
        "/goal",
        "/mcp",
        "/reset-shell",
        "/title",
    ];

    let output = std::process::Command::new("polytoken")
        .arg("print-slash-commands")
        .output()
        .ok()?;

    if !output.status.success() {
        warn!(status = ?output.status, "polytoken print-slash-commands failed");
        return None;
    }

    let json: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    let commands = json.get("commands")?.as_array()?;

    let acp_commands: Vec<acp::AvailableCommand> = commands
        .iter()
        .filter_map(|cmd| {
            let canonical = cmd.get("canonical")?.as_str()?;

            // Skip TUI-only commands that the daemon can't handle via ACP.
            if !ACP_SAFE_COMMANDS.contains(&canonical) {
                return None;
            }

            let name = canonical.strip_prefix('/').unwrap_or(canonical);
            let description = cmd.get("description")?.as_str()?;
            let category = cmd.get("category")?.as_str()?;

            let mut acp_cmd = acp::AvailableCommand::new(name, description);
            if category == "free-text" || category == "choice" {
                let hint = if category == "choice" {
                    "select an option"
                } else {
                    "enter text"
                };
                acp_cmd = acp_cmd.input(acp::AvailableCommandInput::Unstructured(
                    acp::UnstructuredCommandInput::new(hint),
                ));
            }

            // Attach facet choices to the /facet command so ACP clients can
            // render autocomplete suggestions.
            if canonical == "/facet" {
                let choices = discover_facets(cwd);
                if !choices.is_empty() {
                    let mut meta = serde_json::Map::new();
                    meta.insert(
                        "polytoken".to_string(),
                        serde_json::json!({ "choices": choices }),
                    );
                    acp_cmd = acp_cmd.meta(meta);
                }
            }

            Some(acp_cmd)
        })
        .collect();

    if acp_commands.is_empty() {
        None
    } else {
        Some(acp_commands)
    }
}

/// Build ACP available commands from the daemon's `available_skills` list in `/state`.
///
/// Each skill is advertised as an `AvailableCommand` with `_meta.polytoken.kind = "skill"`
/// so clients can distinguish skills from regular slash commands. Skills are invoked
/// via `@skill:<name>` in prompt text, but we advertise them with just the name so
/// the shim can translate `/name` → `@skill:name` when forwarding the prompt.
fn build_skill_commands(
    state: &Result<serde_json::Value, anyhow::Error>,
) -> Vec<acp::AvailableCommand> {
    let state = match state {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "Failed to fetch daemon state for skills; skipping");
            return Vec::new();
        }
    };

    let skills = match state.get("available_skills").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return Vec::new(),
    };

    let mut meta = serde_json::Map::new();
    meta.insert(
        "polytoken".to_string(),
        serde_json::json!({"kind": "skill"}),
    );

    skills
        .iter()
        .filter_map(|s| s.as_str())
        .map(|name| {
            acp::AvailableCommand::new(name, format!("Invoke the '{}' skill", name))
                .meta(meta.clone())
        })
        .collect()
}

/// Rewrite `/skillname` → `@skill:skillname` in prompt text when `skillname`
/// is a known skill. Regular slash commands (e.g. `/clear`) pass through unchanged.
///
/// Checks the filesystem in the daemon's working directory for skill directories:
/// `<cwd>/.polytoken/skills/<name>/SKILL.md` or `<cwd>/.agents/skills/<name>/SKILL.md`.
fn translate_skill_invocations(prompt: &str, cwd: &std::path::Path) -> String {
    // Only rewrite if the prompt starts with `/word` — skip regular text and
    // known slash commands.
    let trimmed = prompt.trim_start();
    if !trimmed.starts_with('/') {
        return prompt.to_string();
    }
    let rest = &trimmed[1..];
    let end = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());
    let candidate = &rest[..end];

    // Check if this is a skill by looking for SKILL.md in the expected directories.
    let is_skill = [".polytoken/skills", ".agents/skills"]
        .iter()
        .any(|dir| cwd.join(dir).join(candidate).join("SKILL.md").exists());

    if is_skill {
        let slash_pos = prompt.find('/').unwrap();
        let prefix = &prompt[..slash_pos];
        let suffix = &prompt[slash_pos + 1 + candidate.len()..];
        format!("{}@skill:{}{}", prefix, candidate, suffix)
    } else {
        prompt.to_string()
    }
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

/// Build the `thought_level` config option from the active model's reasoning
/// capability.
///
/// The daemon's `/state` response includes:
/// - `active_model`: the current model name (e.g. `zai/glm-5.2`)
/// - `active_reasoning_effort`: the current effort level (e.g. `high`)
/// - `available_models[]`: each has a `reasoning` field describing effort levels
///
/// We find the active model in `available_models`, extract its reasoning levels,
/// and build a select config option. When Paseo sets a thought_level, we translate
/// it to a model switch: `zai/glm-5.2(high)` instead of `zai/glm-5.2`.
fn build_thought_level_config_option(
    state: &Result<serde_json::Value, anyhow::Error>,
) -> Option<acp::SessionConfigOption> {
    let state = match state {
        Ok(s) => s,
        Err(_) => return None,
    };

    let active_model_raw = state.get("active_model")?.as_str()?;
    let available_models = state.get("available_models")?.as_array()?;

    // The daemon may encode effort in the active model name (e.g.
    // "zai/glm-5.2(high)"). The available_models entries use the base name
    // without the suffix, so strip any "(…)" suffix before matching.
    let active_model = active_model_raw
        .split('(')
        .next()
        .unwrap_or(active_model_raw);

    // Find the active model entry to get its reasoning capability.
    let active_entry = available_models
        .iter()
        .find(|m| m.get("name").and_then(|v| v.as_str()) == Some(active_model))?;

    let reasoning = active_entry.get("reasoning")?;

    // Extract effort levels from the reasoning capability.
    // Two shapes: {"type": "effort", "levels": [...], "default_level": "..."}
    //          or {"type": "thinking", "can_disable": true} (thinking on/off)
    let (levels, default_level) =
        if reasoning.get("type").and_then(|v| v.as_str()) == Some("effort") {
            let levels = reasoning
                .get("levels")
                .and_then(|v| v.as_array())?
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect::<Vec<_>>();
            let default_level = reasoning
                .get("default_level")
                .and_then(|v| v.as_str())
                .map(String::from);
            (levels, default_level)
        } else if reasoning.get("type").and_then(|v| v.as_str()) == Some("thinking") {
            // Thinking models have two states: on (default) and off (none).
            // We map them as "thinking" and "none".
            (
                vec!["thinking".to_string(), "none".to_string()],
                Some("thinking".to_string()),
            )
        } else {
            // no_reasoning or unknown type
            return None;
        };

    if levels.is_empty() {
        return None;
    }

    // The current thought_level is from active_reasoning_effort, or the default.
    // For thinking-type models, the daemon uses "t" for thinking-on — normalize to "thinking".
    let mut current = state
        .get("active_reasoning_effort")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or(default_level)
        .unwrap_or_else(|| levels[0].clone());

    // Normalize daemon-specific effort labels to our option values.
    if current == "t" {
        current = "thinking".to_string();
    }

    let options: Vec<acp::SessionConfigSelectOption> = levels
        .iter()
        .map(|level| acp::SessionConfigSelectOption::new(level.clone(), level.clone()))
        .collect();

    Some(
        acp::SessionConfigOption::select("thought_level", "Thinking", current, options)
            .category(acp::SessionConfigOptionCategory::ThoughtLevel),
    )
}

/// Build MCP server config options from the daemon's `/state` response.
///
/// Each configured MCP server becomes a boolean `SessionConfigOption` with
/// id `mcp:<server_name>`. The user can toggle enable/disable in Paseo's
/// config UI, and we route the toggle to `POST /mcp/{name}/enable` or
/// `POST /mcp/{name}/disable`.
fn build_mcp_config_options(
    state: &Result<serde_json::Value, anyhow::Error>,
) -> Vec<acp::SessionConfigOption> {
    let state = match state {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    let servers = match state.get("mcp_servers").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return Vec::new(),
    };

    servers
        .iter()
        .filter_map(|s| {
            let name = s.get("server_name")?.as_str()?;
            let status = s.get("status")?.as_str()?;
            let tool_count = s.get("tool_count").and_then(|v| v.as_i64()).unwrap_or(0);

            let enabled = status != "disabled";
            let description = if tool_count > 0 {
                format!("{} tools", tool_count)
            } else {
                "no tools".to_string()
            };

            Some(
                acp::SessionConfigOption::boolean(
                    format!("mcp:{}", name),
                    name.to_string(),
                    enabled,
                )
                .description(description)
                .category(acp::SessionConfigOptionCategory::Other("mcp".into())),
            )
        })
        .collect()
}

/// Convert ACP `McpServer` variants into a polytoken config YAML file and write
/// it to a temporary directory.
///
/// Returns the directory path (suitable for `--project-config-dir`) if any MCP
/// servers were written, or `None` if the list was empty. The daemon's config
/// layering merges this project-level config on top of the user's global
/// config, so existing servers and model keys remain intact.
///
/// ACP supports three transport variants — `Stdio`, `Http`, and `Sse` — while
/// polytoken's config schema only has `stdio` and `http` (streamable-HTTP).
/// `Stdio` maps directly; `Http` maps to polytoken's `http` transport. The
/// `Sse` variant is **skipped** because SSE and streamable-HTTP are
/// different MCP protocols — polytoken has no SSE transport.
fn write_mcp_servers_config(mcp_servers: &[acp::McpServer], temp_dir: &Path) -> Option<PathBuf> {
    if mcp_servers.is_empty() {
        return None;
    }

    // Build a serde_yaml::Mapping with mcp_servers as the top-level key.
    let mut root = serde_yaml::Mapping::new();
    let mut servers_map = serde_yaml::Mapping::new();

    for server in mcp_servers {
        match server {
            acp::McpServer::Stdio(stdio) => {
                let mut entry = serde_yaml::Mapping::new();
                entry.insert(
                    serde_yaml::Value::String("transport".into()),
                    serde_yaml::Value::String("stdio".into()),
                );
                entry.insert(
                    serde_yaml::Value::String("command".into()),
                    serde_yaml::Value::String(stdio.command.to_string_lossy().to_string()),
                );
                if !stdio.args.is_empty() {
                    let args: Vec<serde_yaml::Value> = stdio
                        .args
                        .iter()
                        .map(|a| serde_yaml::Value::String(a.clone()))
                        .collect();
                    entry.insert(
                        serde_yaml::Value::String("args".into()),
                        serde_yaml::Value::Sequence(args),
                    );
                }
                if !stdio.env.is_empty() {
                    let mut env_map = serde_yaml::Mapping::new();
                    for var in &stdio.env {
                        env_map.insert(
                            serde_yaml::Value::String(var.name.clone()),
                            serde_yaml::Value::String(var.value.clone()),
                        );
                    }
                    entry.insert(
                        serde_yaml::Value::String("env".into()),
                        serde_yaml::Value::Mapping(env_map),
                    );
                }

                // ACP stdio servers may expect the full parent environment
                // (e.g. NODE_PATH, DYLD_*, provider keys). Polytoken's
                // default pass_env is restrictive (HOME, PATH, LANG, TMPDIR),
                // so we explicitly list every env var from the current
                // process. Explicit `env` values above override these.
                let pass_env_names: Vec<serde_yaml::Value> = std::env::vars()
                    .map(|(k, _)| serde_yaml::Value::String(k))
                    .collect();
                entry.insert(
                    serde_yaml::Value::String("pass_env".into()),
                    serde_yaml::Value::Sequence(pass_env_names),
                );

                // Log stdout so connection issues are visible. Polytoken
                // logs stderr by default but stdout is off by default.
                entry.insert(
                    serde_yaml::Value::String("log_stdout".into()),
                    serde_yaml::Value::Bool(true),
                );
                servers_map.insert(
                    serde_yaml::Value::String(stdio.name.clone()),
                    serde_yaml::Value::Mapping(entry),
                );
            }
            acp::McpServer::Http(http) => {
                let (name, url, headers) = (&http.name, &http.url, &http.headers);
                let mut entry = serde_yaml::Mapping::new();
                entry.insert(
                    serde_yaml::Value::String("transport".into()),
                    serde_yaml::Value::String("http".into()),
                );
                entry.insert(
                    serde_yaml::Value::String("url".into()),
                    serde_yaml::Value::String(url.clone()),
                );

                // Separate the Authorization header from the rest.
                // Polytoken has a dedicated `auth` field for credential
                // storage; using it avoids putting tokens in the generic
                // `headers` map and lets polytoken handle auth lifecycle.
                let mut auth_value: Option<String> = None;
                let mut headers_map = serde_yaml::Mapping::new();
                for hdr in headers {
                    if hdr.name.eq_ignore_ascii_case("authorization") {
                        auth_value = Some(hdr.value.clone());
                    } else {
                        headers_map.insert(
                            serde_yaml::Value::String(hdr.name.clone()),
                            serde_yaml::Value::String(hdr.value.clone()),
                        );
                    }
                }

                if let Some(value) = auth_value {
                    let mut auth_map = serde_yaml::Mapping::new();
                    auth_map.insert(
                        serde_yaml::Value::String("type".into()),
                        serde_yaml::Value::String("authorization-header".into()),
                    );
                    auth_map.insert(
                        serde_yaml::Value::String("value".into()),
                        serde_yaml::Value::String(value),
                    );
                    entry.insert(
                        serde_yaml::Value::String("auth".into()),
                        serde_yaml::Value::Mapping(auth_map),
                    );
                }

                if !headers_map.is_empty() {
                    entry.insert(
                        serde_yaml::Value::String("headers".into()),
                        serde_yaml::Value::Mapping(headers_map),
                    );
                }

                servers_map.insert(
                    serde_yaml::Value::String(name.clone()),
                    serde_yaml::Value::Mapping(entry),
                );
            }
            // Polytoken does not support the SSE transport — it only has
            // stdio and streamable-HTTP. SSE and streamable-HTTP are
            // different MCP transports, so we cannot map Sse to http.
            acp::McpServer::Sse(sse) => {
                warn!(
                    server_name = %sse.name,
                    "Skipping SSE MCP server: polytoken does not support the SSE transport"
                );
            }
            // Polytoken only supports stdio and http transports. Unknown or
            // future ACP variants (e.g. Acp) are skipped with a warning.
            _ => {
                warn!("Skipping unsupported MCP server transport variant");
            }
        }
    }

    root.insert(
        serde_yaml::Value::String("mcp_servers".into()),
        serde_yaml::Value::Mapping(servers_map),
    );

    let config_dir = temp_dir.join("mcp-config");
    if let Err(e) = std::fs::create_dir_all(&config_dir) {
        warn!(error = %e, "Failed to create MCP config dir");
        return None;
    }
    // Restrict to owner-only: the config may contain auth tokens.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&config_dir, std::fs::Permissions::from_mode(0o700)).ok();
    }

    let config_path = config_dir.join("config.yaml");
    let yaml_str = match serde_yaml::to_string(&root) {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "Failed to serialize MCP config YAML");
            return None;
        }
    };

    if let Err(e) = std::fs::write(&config_path, yaml_str) {
        warn!(error = %e, "Failed to write MCP config file");
        return None;
    }
    // Restrict to owner-only: the config may contain auth tokens.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&config_path, std::fs::Permissions::from_mode(0o600)).ok();
    }

    info!(
        server_count = mcp_servers.len(),
        config_path = ?config_path,
        "Wrote MCP servers to temp config for daemon"
    );

    Some(config_dir)
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
    let url = format!("{}/permission-monitor", base_url);
    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", bearer))
        .json(&serde_json::json!({ "mode": mode_id }))
        .send()
        .await;
    let result = match resp {
        Ok(r) if r.status().is_success() => Ok(()),
        Ok(r) => Err(anyhow::anyhow!(
            "POST /permission-monitor returned status {}",
            r.status()
        )),
        Err(e) => Err(anyhow::anyhow!("Failed to POST /permission-monitor: {}", e)),
    };

    match result {
        Ok(()) => {
            info!(session_id = %session_id, mode_id = %mode_id, "Permission monitor mode switched");
            responder.respond(acp::SetSessionModeResponse::new())
        }
        Err(e) => {
            error!(error = %e, "Failed to switch permission monitor mode");
            responder.respond_with_error(agent_client_protocol::Error::internal_error().data(
                serde_json::json!({
                    "error": "Failed to switch permission monitor mode",
                    "detail": e.to_string(),
                }),
            ))
        }
    }
}

/// Re-fetch daemon state and rebuild the full set of config options.
///
/// After a config option is set (e.g. model change), the daemon's state
/// changes. ACP clients like Paseo expect the `SetSessionConfigOptionResponse`
/// to include the updated config options so they can refresh their internal
/// state. Returning an empty vec causes the client to clobber its
/// `configOptions`, which makes subsequent config option calls fail with
/// "does not expose ACP thought-level selection".
async fn rebuild_config_options(base_url: &str, bearer: &str) -> Vec<acp::SessionConfigOption> {
    let daemon_state = match fetch_daemon_state_raw(base_url, bearer).await {
        Some(v) => Ok(v),
        None => Err(anyhow::anyhow!(
            "failed to fetch daemon state after config option set"
        )),
    };
    let permission_monitor = match fetch_permission_monitor_raw(base_url, bearer).await {
        Some(v) => Ok(v),
        None => Err(anyhow::anyhow!("permission-monitor fetch failed")),
    };
    let mode_state = build_session_mode_from_permission_monitor(&permission_monitor);
    build_config_options(&daemon_state, &mode_state, &permission_monitor)
}

async fn handle_set_session_config_option(
    state: &Arc<Mutex<AgentState>>,
    req: acp::SetSessionConfigOptionRequest,
    responder: agent_client_protocol::Responder<acp::SetSessionConfigOptionResponse>,
    _cx: ConnectionTo<Client>,
) -> Result<(), agent_client_protocol::Error> {
    let session_id = req.session_id.0.to_string();
    let config_id = req.config_id.0.to_string();
    let value = req
        .value
        .as_value_id()
        .map(|v| v.0.as_ref().to_string())
        .or_else(|| req.value.as_bool().map(|b| b.to_string()));

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
    let (endpoint, payload, label): (String, serde_json::Value, &str) = match config_id.as_str() {
        "model" => (
            format!("{}/model", base_url),
            serde_json::json!({ "model": value }),
            "model",
        ),
        mcp_id if mcp_id.starts_with("mcp:") => {
            let server_name = &mcp_id[4..];
            let action = if value == "true" { "enable" } else { "disable" };
            (
                format!("{}/mcp/{}/{}", base_url, server_name, action),
                serde_json::json!({}),
                "mcp",
            )
        }
        "thought_level" => {
            // Translate thought_level to a model switch.
            // The daemon encodes effort in model names: zai/glm-5.2(high)
            // We need to fetch /state to get the active model name, then
            // append the effort level.
            let client = reqwest::Client::new();
            let state_resp = client
                .get(format!("{}/state", base_url))
                .header("Authorization", format!("Bearer {}", bearer))
                .send()
                .await;

            let model_with_effort = match state_resp {
                Ok(r) if r.status().is_success() => {
                    let state: serde_json::Value = match r.json().await {
                        Ok(v) => v,
                        Err(e) => {
                            error!(error = %e, "Failed to parse /state for thought_level");
                            return responder.respond_with_error(
                                agent_client_protocol::Error::internal_error().data(
                                    serde_json::json!({"error": "Failed to read daemon state for thought_level"}),
                                ),
                            );
                        }
                    };
                    let active_model = state
                        .get("active_model")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");

                    if active_model.is_empty() {
                        warn!("Cannot set thought_level: no active model");
                        return responder.respond(acp::SetSessionConfigOptionResponse::new(vec![]));
                    }

                    // The daemon encodes effort in the model name (e.g.
                    // "zai/glm-5.2(high)"). When a variant is already active,
                    // /state's active_model already includes the suffix, so
                    // strip any existing "(…)" suffix before composing the new
                    // variant to avoid doubling it (e.g.
                    // "zai/glm-5.2(high)(medium)").
                    let base_model = active_model.split('(').next().unwrap_or(active_model);

                    // For effort-type models: append (value) to model name
                    // For thinking-type models: "thinking" → use base model name (no suffix)
                    //                          "none" → append (none)
                    let model_with_effort = if value == "thinking" {
                        // Thinking on = the base model name (default)
                        base_model.to_string()
                    } else {
                        format!("{}({})", base_model, value)
                    };

                    // Verify this model variant exists in available_models
                    let available = state
                        .get("available_models")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();

                    let exists = available.iter().any(|m| {
                        m.get("name").and_then(|v| v.as_str()) == Some(&model_with_effort)
                    });

                    if !exists {
                        warn!(
                            model = %model_with_effort,
                            "Model variant not found in available_models; trying anyway"
                        );
                    }

                    model_with_effort
                }
                _ => {
                    error!("Failed to fetch /state for thought_level");
                    return responder.respond_with_error(
                        agent_client_protocol::Error::internal_error().data(
                            serde_json::json!({"error": "Failed to fetch daemon state for thought_level"}),
                        ),
                    );
                }
            };

            (
                format!("{}/model", base_url),
                serde_json::json!({ "model": model_with_effort }),
                "thought_level",
            )
        }
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

            // Re-fetch daemon state and rebuild config options so the client
            // receives the updated state (e.g. thought_level currentValue after
            // a model change). Returning an empty vec causes Paseo to clobber
            // its configOptions, losing the thought_level option and breaking
            // subsequent setThinkingOption calls.
            let updated_config_options = rebuild_config_options(&base_url, &bearer).await;

            responder.respond(acp::SetSessionConfigOptionResponse::new(
                updated_config_options,
            ))
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
    cwd: std::path::PathBuf,
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
        let cwd = self.cwd;
        let mut responder = Some(self.responder);

        loop {
            match connect_and_consume(
                &events_url,
                &bearer_token,
                &base_url,
                &prompt_id,
                &session_id,
                &cwd,
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

                    // Send usage_update notification with context usage.
                    if let Some(usage) = fetch_context_usage(&base_url, &bearer_token).await {
                        let sid = acp::SessionId::new(session_id.clone());
                        let notification = acp::SessionNotification::new(
                            sid,
                            acp::SessionUpdate::UsageUpdate(usage),
                        );
                        if let Err(e) = conn.send_notification(notification) {
                            warn!(error = %e, "Failed to send usage_update notification");
                        }
                    }

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
    cwd: &std::path::Path,
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
    // Cache the last-known facet list so we only re-send available_commands_update
    // when the list actually changes (guards against spurious session_state_changed).
    let mut cached_facets: Option<Vec<String>> = None;

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
            match process_sse_event(
                &data,
                prompt_id,
                session_id,
                base_url,
                bearer_token,
                cwd,
                &mut cached_facets,
                conn,
            )
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
#[allow(clippy::too_many_arguments)]
async fn process_sse_event(
    data: &str,
    prompt_id: &str,
    session_id: &str,
    base_url: &str,
    bearer_token: &str,
    cwd: &std::path::Path,
    cached_facets: &mut Option<Vec<String>>,
    conn: &ConnectionTo<Client>,
) -> ConsumeOutcome {
    let event = match events::parse_sse_event(data) {
        Some(e) => e,
        None => {
            debug!(data = %data.chars().take(200).collect::<String>(), "Failed to parse SSE event; skipping");
            return ConsumeOutcome::Continue;
        }
    };

    let event_type = events::event_type_name(&event);
    let summary = events::event_summary(&event);

    tracing::debug!(
        target: "polytoken_acp::conv",
        event_type = event_type,
        summary = %summary,
        "daemon_event"
    );

    // Filter by prompt_id if the event has one
    if let Some(epid) = events::event_prompt_id(&event)
        && epid != prompt_id
    {
        return ConsumeOutcome::Continue;
    }

    match events::translate_event(&event) {
        EventTranslation::Update(update) => {
            let update_name = events::session_update_name(&update);
            tracing::debug!(
                target: "polytoken_acp::conv",
                update_type = update_name,
                "acp_notification"
            );
            let sid = acp::SessionId::new(session_id.to_string());
            let notification = acp::SessionNotification::new(sid, update.clone());
            if let Err(e) = conn.send_notification(notification) {
                error!(error = %e, "Failed to send session notification");
            }
            ConsumeOutcome::Continue
        }
        EventTranslation::TurnEnd => {
            tracing::info!(
                target: "polytoken_acp::conv",
                prompt_id = %prompt_id,
                "turn_end"
            );
            ConsumeOutcome::Done(acp::StopReason::EndTurn)
        }
        EventTranslation::TurnCancelled => {
            tracing::info!(
                target: "polytoken_acp::conv",
                prompt_id = %prompt_id,
                "turn_cancelled"
            );
            ConsumeOutcome::Done(acp::StopReason::Cancelled)
        }
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
        EventTranslation::InterrogativeRequest {
            interrogative_id,
            question,
            interrogative_type,
        } => {
            handle_interrogative(
                conn,
                &interrogative_id,
                &question,
                &interrogative_type,
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
        EventTranslation::SubagentStarted {
            handle,
            subagent_type,
            model,
        } => {
            // 1. Send standard ACP ToolCall so Paseo renders it in the timeline.
            let tool_call = acp::ToolCall::new(handle.clone(), subagent_type.clone())
                .kind(acp::ToolKind::Think)
                .status(acp::ToolCallStatus::InProgress);
            let sid = acp::SessionId::new(session_id.to_string());
            let notification =
                acp::SessionNotification::new(sid, acp::SessionUpdate::ToolCall(tool_call));
            if let Err(e) = conn.send_notification(notification) {
                error!(error = %e, "Failed to send subagent_started ToolCall notification");
            }

            // 2. Send extension notification with full metadata (model, type).
            let params = serde_json::json!({
                "handle": handle,
                "subagent_type": subagent_type,
                "model": model,
            });
            send_ext_notification(conn, "_polytoken/subagent_started", &params);

            ConsumeOutcome::Continue
        }
        EventTranslation::SubagentCompleted {
            handle,
            result_summary,
        } => {
            // 1. Send standard ACP ToolCallUpdate so Paseo marks it complete.
            let mut fields =
                acp::ToolCallUpdateFields::new().status(acp::ToolCallStatus::Completed);
            if let Some(summary) = &result_summary {
                let block: acp::ContentBlock = summary.clone().into();
                fields = fields.content(vec![acp::ToolCallContent::from(block)]);
            }
            let update = acp::ToolCallUpdate::new(handle.clone(), fields);
            let sid = acp::SessionId::new(session_id.to_string());
            let notification =
                acp::SessionNotification::new(sid, acp::SessionUpdate::ToolCallUpdate(update));
            if let Err(e) = conn.send_notification(notification) {
                error!(error = %e, "Failed to send subagent_completed ToolCallUpdate notification");
            }

            // 2. Send extension notification with full metadata.
            let params = serde_json::json!({
                "handle": handle,
                "result_summary": result_summary,
            });
            send_ext_notification(conn, "_polytoken/subagent_completed", &params);

            ConsumeOutcome::Continue
        }
        EventTranslation::JobEvent {
            job_id,
            event_type,
            subagent_handle: _,
            exit_code,
        } => {
            // Shell job events (subagent jobs are filtered out in translate_job_event).
            // Send extension notification with the event data.
            let method = format!("_polytoken/{}", event_type);
            let params = match exit_code {
                Some(code) => serde_json::json!({
                    "job_id": job_id,
                    "exit_code": code,
                    "subagent_handle": null,
                }),
                None => serde_json::json!({
                    "job_id": job_id,
                    "subagent_handle": null,
                }),
            };
            send_ext_notification(conn, &method, &params);
            ConsumeOutcome::Continue
        }
        EventTranslation::SystemReminder {
            slug,
            display_name,
            body,
            reason,
        } => {
            let params = serde_json::json!({
                "slug": slug,
                "display_name": display_name,
                "body": body,
                "reason": reason,
            });
            send_ext_notification(conn, "_polytoken/system_reminder", &params);
            ConsumeOutcome::Continue
        }
        EventTranslation::GoalDriverUpdate {
            transition,
            summary,
        } => {
            let entries = if transition == "cleared" {
                vec![]
            } else {
                vec![acp::PlanEntry::new(
                    format!("Goal: {}", summary),
                    acp::PlanEntryPriority::High,
                    acp::PlanEntryStatus::InProgress,
                )]
            };
            let plan = acp::Plan::new(entries);
            let sid = acp::SessionId::new(session_id.to_string());
            let notification = acp::SessionNotification::new(sid, acp::SessionUpdate::Plan(plan));
            if let Err(e) = conn.send_notification(notification) {
                error!(error = %e, "Failed to send goal driver Plan notification");
            }
            ConsumeOutcome::Continue
        }
        EventTranslation::TodoStateChange => {
            // Re-fetch /state and send an updated Plan
            if let Some(plan) = fetch_daemon_state_raw(base_url, bearer_token)
                .await
                .and_then(|state| events::build_plan_from_state(&state))
            {
                let sid = acp::SessionId::new(session_id.to_string());
                let notification =
                    acp::SessionNotification::new(sid, acp::SessionUpdate::Plan(plan));
                if let Err(e) = conn.send_notification(notification) {
                    error!(error = %e, "Failed to send todo Plan notification");
                }
            }
            ConsumeOutcome::Continue
        }
        EventTranslation::FacetChoicesCheck => {
            // Re-discover facets and send updated available_commands_update
            // only when the list has changed since the last check.
            let new_facets = discover_facets(cwd);
            if cached_facets.as_ref() != Some(&new_facets) {
                debug!(facets = ?new_facets, "Facet list changed; sending available_commands_update");
                *cached_facets = Some(new_facets.clone());
                if let Some(mut all_commands) = build_available_commands(cwd) {
                    // Also re-attach skill commands so the update is complete.
                    let skill_commands = match fetch_daemon_state_raw(base_url, bearer_token).await
                    {
                        Some(state) => build_skill_commands(&Ok(state)),
                        None => Vec::new(),
                    };
                    all_commands.extend(skill_commands);
                    let sid = acp::SessionId::new(session_id.to_string());
                    let notification = acp::SessionNotification::new(
                        sid,
                        acp::SessionUpdate::AvailableCommandsUpdate(
                            acp::AvailableCommandsUpdate::new(all_commands),
                        ),
                    );
                    if let Err(e) = conn.send_notification(notification) {
                        warn!(error = %e, "Failed to send available_commands_update for facet change");
                    }
                }
            }
            ConsumeOutcome::Continue
        }
        EventTranslation::PermissionMonitorSwitch { mode } => {
            debug!(mode = %mode, "Permission monitor switched; sending CurrentModeUpdate");
            // Permissions are now mapped to ACP "mode", so a permission
            // monitor switch is forwarded as a CurrentModeUpdate.
            let mode_update = acp::CurrentModeUpdate::new(mode.clone());
            let sid = acp::SessionId::new(session_id.to_string());
            let notification = acp::SessionNotification::new(
                sid,
                acp::SessionUpdate::CurrentModeUpdate(mode_update),
            );
            if let Err(e) = conn.send_notification(notification) {
                error!(error = %e, "Failed to send CurrentModeUpdate for permission monitor switch");
            }
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

    tracing::info!(
        target: "polytoken_acp::conv",
        interrogative_id = %interrogative_id,
        question = %question,
        "permission_request"
    );

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

        tracing::info!(
            target: "polytoken_acp::conv",
            interrogative_id = %interrogative_id,
            granted,
            "permission_response"
        );

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

/// Forward a non-permission interrogative (confirmation, clarification, etc.)
/// to the ACP client via `session/request_permission` and relay the response.
///
/// For non-permission interrogatives, we map "allow" to a positive answer and
/// "reject" to a negative answer. The daemon endpoint accepts different response
/// kinds depending on the interrogative type.
async fn handle_interrogative(
    conn: &ConnectionTo<Client>,
    interrogative_id: &str,
    question: &str,
    interrogative_type: &str,
    session_id: &str,
    base_url: &str,
    bearer_token: &str,
) {
    info!(
        interrogative_id = %interrogative_id,
        interrogative_type = %interrogative_type,
        "Forwarding interrogative to ACP client"
    );

    tracing::info!(
        target: "polytoken_acp::conv",
        interrogative_id = %interrogative_id,
        interrogative_type = %interrogative_type,
        question = %question,
        "interrogative_request"
    );

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
    let interrogative_type = interrogative_type.to_string();

    let sent = conn.send_request(request);
    sent.on_receiving_result(async move |result| {
        let granted = match result {
            Ok(response) => events::resolve_permission_outcome(&response.outcome),
            Err(e) => {
                error!(error = %e, "Interrogative request failed");
                false
            }
        };

        info!(
            interrogative_id = %interrogative_id,
            interrogative_type = %interrogative_type,
            granted,
            "Interrogative response from client"
        );

        tracing::info!(
            target: "polytoken_acp::conv",
            interrogative_id = %interrogative_id,
            interrogative_type = %interrogative_type,
            granted,
            "interrogative_response"
        );

        // Map the ACP permission outcome to the appropriate daemon response kind.
        if let Err(e) = respond_interrogative_generic(
            &base_url,
            &bearer_token,
            &interrogative_id,
            &interrogative_type,
            granted,
        )
        .await
        {
            error!(error = %e, "Failed to respond to interrogative on daemon");
        }

        Ok(())
    })
    .expect("on_receiving_result failed");
}

/// Send a one-way ACP extension notification (method name starts with `_`).
///
/// Extension notifications are best-effort: if the client doesn't recognize
/// the method, it silently ignores it (per the ACP extensibility spec).
fn send_ext_notification(conn: &ConnectionTo<Client>, method: &str, params: &serde_json::Value) {
    let raw = match serde_json::value::RawValue::from_string(params.to_string()) {
        Ok(raw) => std::sync::Arc::from(raw),
        Err(e) => {
            error!(error = %e, method, "Failed to serialize ext notification params");
            return;
        }
    };
    let ext_notif = acp::AgentRequest::ExtMethodRequest(acp::ExtRequest::new(method, raw));
    // Extension notifications are fire-and-forget. If the client doesn't
    // recognize the method it silently ignores it (per the ACP spec).
    let _ = conn.send_request(ext_notif);
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

    tracing::info!(
        target: "polytoken_acp::conv",
        interrogative_id = %interrogative_id,
        question_count = payload.questions.len(),
        "ask_user_question"
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
        "_polytoken/ask_user_question",
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

/// Fetch the daemon's `/state` as raw JSON.
///
/// Used by TodoStateChange to re-fetch todos and build an updated Plan.
async fn fetch_daemon_state_raw(base_url: &str, bearer_token: &str) -> Option<serde_json::Value> {
    let client = reqwest::Client::new();
    let url = format!("{}/state", base_url);
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        warn!(status = %resp.status(), "Failed to fetch /state for todo plan");
        return None;
    }
    resp.json::<serde_json::Value>().await.ok()
}

/// Fetch the daemon's `/permission-monitor` as raw JSON.
async fn fetch_permission_monitor_raw(
    base_url: &str,
    bearer_token: &str,
) -> Option<serde_json::Value> {
    let client = reqwest::Client::new();
    let url = format!("{}/permission-monitor", base_url);
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        warn!(status = %resp.status(), "Failed to fetch /permission-monitor");
        return None;
    }
    resp.json::<serde_json::Value>().await.ok()
}

/// Fetch context usage from the daemon's `/state` endpoint and convert to ACP `UsageUpdate`.
async fn fetch_context_usage(base_url: &str, bearer_token: &str) -> Option<acp::UsageUpdate> {
    let client = reqwest::Client::new();
    let url = format!("{}/state", base_url);
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()
        .await
        .ok()?;

    if !resp.status().is_success() {
        return None;
    }

    let state: serde_json::Value = resp.json().await.ok()?;
    let usage = state.get("context_usage")?;

    let used = usage
        .get("used_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let size = usage
        .get("limit_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    if size == 0 {
        return None;
    }

    Some(acp::UsageUpdate::new(used, size))
}

/// Fetch session history from the daemon's `GET /history` endpoint.
///
/// Returns the `items` array from the `SessionHistorySnapshot` as a list of
/// `serde_json::Value` entries. Each entry has a `type` discriminant that
/// `history::translate_history_item` dispatches on.
async fn fetch_history_raw(
    base_url: &str,
    bearer_token: &str,
) -> anyhow::Result<Vec<serde_json::Value>> {
    let client = reqwest::Client::new();
    let url = format!("{}/history", base_url);
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()
        .await
        .context("Failed to GET /history")?;
    if !resp.status().is_success() {
        bail!("GET /history returned status {}", resp.status());
    }
    let snapshot: serde_json::Value = resp.json().await.context("Failed to parse /history")?;
    let items = snapshot
        .get("items")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    Ok(items)
}

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

/// POST a generic interrogative response to the daemon.
///
/// Maps the ACP permission outcome (allow/reject) to the appropriate daemon
/// response kind based on the interrogative type:
/// - `confirmation` → `{"kind": "confirmation_answer", "confirmed": granted}`
/// - `capability` → `{"kind": "capability_answer", "granted": granted}`
/// - `goal_proposal` → `{"kind": "goal_proposal_answer", "accepted": granted}`
/// - `plan_handoff` → `{"kind": "plan_handoff_answer", "decision": ...}` (best-effort)
/// - `clarification` → `{"kind": "clarification_choice", "choice": ...}` (best-effort)
/// - fallback → `{"kind": "cancel"}` (can't meaningfully answer)
async fn respond_interrogative_generic(
    base_url: &str,
    bearer_token: &str,
    interrogative_id: &str,
    interrogative_type: &str,
    granted: bool,
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/interrogative/{}/respond", base_url, interrogative_id);

    let body = match interrogative_type {
        "confirmation" => serde_json::json!({"kind": "confirmation_answer", "confirmed": granted}),
        "capability" => serde_json::json!({"kind": "capability_answer", "granted": granted}),
        "goal_proposal" => serde_json::json!({"kind": "goal_proposal_answer", "accepted": granted}),
        // For clarification and plan_handoff, we can't fully answer without
        // structured input from the user. Cancel if rejected; default if granted.
        _ => {
            warn!(
                interrogative_type = %interrogative_type,
                "Unsupported interrogative type for generic response; cancelling"
            );
            serde_json::json!({"kind": "cancel"})
        }
    };

    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", bearer_token))
        .json(&body)
        .send()
        .await?;
    if !resp.status().is_success() {
        warn!(status = %resp.status(), "Generic interrogative response failed");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_thought_level_config_option_effort() {
        let state = serde_json::json!({
            "active_model": "zai/glm-5.2",
            "active_reasoning_effort": "high",
            "available_models": [
                {
                    "name": "zai/glm-5.2",
                    "label": "zai/glm-5.2",
                    "reasoning": {
                        "type": "effort",
                        "effort_set": "zai_glm_5_2",
                        "levels": ["high", "max", "none"],
                        "default_level": "high",
                        "can_disable": true
                    }
                }
            ]
        });
        let state_result: Result<serde_json::Value, anyhow::Error> = Ok(state);
        let opt = build_thought_level_config_option(&state_result).unwrap();
        assert_eq!(opt.id.0.as_ref(), "thought_level");
        match &opt.kind {
            acp::SessionConfigKind::Select(s) => {
                assert_eq!(s.current_value.0.as_ref(), "high");
                match &s.options {
                    acp::SessionConfigSelectOptions::Ungrouped(opts) => {
                        assert_eq!(opts.len(), 3);
                    }
                    _ => panic!("Expected Ungrouped options"),
                }
            }
            _ => panic!("Expected Select kind"),
        }
    }

    #[test]
    fn test_build_thought_level_config_option_effort_suffix() {
        // When a reasoning effort is active, the daemon reports active_model
        // with a "(effort)" suffix (e.g. "zai/glm-5.2(high)"), but
        // available_models entries use the base name. The builder must strip
        // the suffix before matching.
        let state = serde_json::json!({
            "active_model": "zai/glm-5.2(high)",
            "active_reasoning_effort": "high",
            "available_models": [
                {
                    "name": "zai/glm-5.2",
                    "label": "zai/glm-5.2",
                    "reasoning": {
                        "type": "effort",
                        "effort_set": "zai_glm_5_2",
                        "levels": ["high", "max", "none"],
                        "default_level": "high",
                        "can_disable": true
                    }
                }
            ]
        });
        let state_result: Result<serde_json::Value, anyhow::Error> = Ok(state);
        let opt = build_thought_level_config_option(&state_result).unwrap();
        assert_eq!(opt.id.0.as_ref(), "thought_level");
        match &opt.kind {
            acp::SessionConfigKind::Select(s) => {
                assert_eq!(s.current_value.0.as_ref(), "high");
                match &s.options {
                    acp::SessionConfigSelectOptions::Ungrouped(opts) => {
                        assert_eq!(opts.len(), 3);
                    }
                    _ => panic!("Expected Ungrouped options"),
                }
            }
            _ => panic!("Expected Select kind"),
        }
    }

    #[test]
    fn test_build_thought_level_config_option_thinking() {
        let state = serde_json::json!({
            "active_model": "zai/glm-5.1",
            "active_reasoning_effort": "t",
            "available_models": [
                {
                    "name": "zai/glm-5.1",
                    "label": "zai/glm-5.1",
                    "reasoning": {
                        "type": "thinking",
                        "can_disable": true
                    }
                }
            ]
        });
        let state_result: Result<serde_json::Value, anyhow::Error> = Ok(state);
        let opt = build_thought_level_config_option(&state_result).unwrap();
        assert_eq!(opt.id.0.as_ref(), "thought_level");
        match &opt.kind {
            acp::SessionConfigKind::Select(s) => {
                assert_eq!(s.current_value.0.as_ref(), "thinking");
                match &s.options {
                    acp::SessionConfigSelectOptions::Ungrouped(opts) => {
                        assert_eq!(opts.len(), 2);
                    }
                    _ => panic!("Expected Ungrouped options"),
                }
            }
            _ => panic!("Expected Select kind"),
        }
    }

    #[test]
    fn test_build_thought_level_config_option_no_reasoning() {
        let state = serde_json::json!({
            "active_model": "zai/glm-4.5-air",
            "available_models": [
                {
                    "name": "zai/glm-4.5-air",
                    "label": "zai/glm-4.5-air",
                    "reasoning": {"type": "no_reasoning"}
                }
            ]
        });
        let state_result: Result<serde_json::Value, anyhow::Error> = Ok(state);
        assert!(build_thought_level_config_option(&state_result).is_none());
    }

    #[test]
    fn test_build_thought_level_config_option_default_level() {
        let state = serde_json::json!({
            "active_model": "zai/glm-5.2",
            "available_models": [
                {
                    "name": "zai/glm-5.2",
                    "label": "zai/glm-5.2",
                    "reasoning": {
                        "type": "effort",
                        "levels": ["low", "medium", "high"],
                        "default_level": "medium",
                        "can_disable": true
                    }
                }
            ]
        });
        let state_result: Result<serde_json::Value, anyhow::Error> = Ok(state);
        let opt = build_thought_level_config_option(&state_result).unwrap();
        match &opt.kind {
            acp::SessionConfigKind::Select(s) => {
                assert_eq!(s.current_value.0.as_ref(), "medium");
            }
            _ => panic!("Expected Select kind"),
        }
    }

    #[test]
    fn test_build_thought_level_config_option_model_not_found() {
        let state = serde_json::json!({
            "active_model": "nonexistent/model",
            "available_models": [
                {
                    "name": "other/model",
                    "label": "other/model",
                    "reasoning": {"type": "no_reasoning"}
                }
            ]
        });
        let state_result: Result<serde_json::Value, anyhow::Error> = Ok(state);
        assert!(build_thought_level_config_option(&state_result).is_none());
    }

    #[test]
    fn test_build_skill_commands() {
        let state = serde_json::json!({
            "available_skills": ["triage", "release", "debug"]
        });
        let state_result: Result<serde_json::Value, anyhow::Error> = Ok(state);
        let commands = build_skill_commands(&state_result);
        assert_eq!(commands.len(), 3);
        assert_eq!(commands[0].name, "triage");
        assert_eq!(commands[1].name, "release");
        assert_eq!(commands[2].name, "debug");
        // Each should have _meta with polytoken.kind = skill
        let meta = commands[0].meta.as_ref().expect("missing _meta");
        assert_eq!(meta["polytoken"]["kind"], "skill");
    }

    #[test]
    fn test_build_skill_commands_empty() {
        let state = serde_json::json!({});
        let state_result: Result<serde_json::Value, anyhow::Error> = Ok(state);
        let commands = build_skill_commands(&state_result);
        assert!(commands.is_empty());
    }

    #[test]
    fn test_build_mcp_config_options() {
        let state = serde_json::json!({
            "mcp_servers": [
                {"server_name": "filesystem", "status": "connected", "tool_count": 5},
                {"server_name": "web-reader", "status": "disabled", "tool_count": 0},
                {"server_name": "github", "status": "disconnected", "tool_count": 3},
            ]
        });
        let state_result: Result<serde_json::Value, anyhow::Error> = Ok(state);
        let options = build_mcp_config_options(&state_result);
        assert_eq!(options.len(), 3);

        // Connected server → enabled=true
        assert_eq!(options[0].id.0.as_ref(), "mcp:filesystem");
        assert!(matches!(&options[0].kind, acp::SessionConfigKind::Boolean(b) if b.current_value));

        // Disabled server → enabled=false
        assert_eq!(options[1].id.0.as_ref(), "mcp:web-reader");
        assert!(matches!(&options[1].kind, acp::SessionConfigKind::Boolean(b) if !b.current_value));

        // Disconnected server → still enabled=true (not disabled)
        assert_eq!(options[2].id.0.as_ref(), "mcp:github");
        assert!(matches!(&options[2].kind, acp::SessionConfigKind::Boolean(b) if b.current_value));
    }

    #[test]
    fn test_build_mcp_config_options_empty() {
        let state = serde_json::json!({});
        let state_result: Result<serde_json::Value, anyhow::Error> = Ok(state);
        let options = build_mcp_config_options(&state_result);
        assert!(options.is_empty());
    }

    #[test]
    fn test_translate_skill_invocations_no_skill() {
        // Regular slash commands pass through unchanged
        let cwd = std::path::Path::new("/tmp");
        assert_eq!(translate_skill_invocations("/clear", cwd), "/clear");
        assert_eq!(
            translate_skill_invocations("/compact some text", cwd),
            "/compact some text"
        );
    }

    #[test]
    fn test_translate_skill_invocations_non_command() {
        let cwd = std::path::Path::new("/tmp");
        // Regular text passes through
        assert_eq!(
            translate_skill_invocations("hello world", cwd),
            "hello world"
        );
    }

    #[test]
    fn test_translate_skill_invocations_real_skill() {
        // Create a temp dir with a skill
        let tmp = std::env::temp_dir().join("polytoken-acp-test-skill");
        let skill_dir = tmp.join(".polytoken/skills/triage");
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(skill_dir.join("SKILL.md"), "# Triage\n").unwrap();

        assert_eq!(
            translate_skill_invocations("/triage", &tmp),
            "@skill:triage"
        );
        assert_eq!(
            translate_skill_invocations("/triage some context", &tmp),
            "@skill:triage some context"
        );
        // Non-skill command in same dir passes through
        assert_eq!(translate_skill_invocations("/clear", &tmp), "/clear");

        // Cleanup
        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_build_session_mode_from_permission_monitor() {
        let pm = serde_json::json!({
            "monitor": {"type": "bypass"},
            "config_default": {"type": "standard"}
        });
        let pm_result: Result<serde_json::Value, anyhow::Error> = Ok(pm);

        let ms = build_session_mode_from_permission_monitor(&pm_result)
            .expect("should return Some for valid permission monitor response");

        assert_eq!(ms.current_mode_id.0.as_ref(), "bypass");
        assert_eq!(ms.available_modes.len(), 4);

        let ids: Vec<&str> = ms.available_modes.iter().map(|m| m.id.0.as_ref()).collect();
        assert!(ids.contains(&"standard"));
        assert!(ids.contains(&"bypass"));
        assert!(ids.contains(&"bypass_plus"));
        assert!(ids.contains(&"autonomous"));
    }

    #[test]
    fn test_build_session_mode_from_permission_monitor_error() {
        let pm_result: Result<serde_json::Value, anyhow::Error> =
            Err(anyhow::anyhow!("permission-monitor fetch failed"));

        let ms = build_session_mode_from_permission_monitor(&pm_result)
            .expect("should still return Some with default on error");
        assert_eq!(ms.current_mode_id.0.as_ref(), "standard");
    }

    #[test]
    fn test_write_mcp_servers_config_empty() {
        let temp = tempfile::tempdir().unwrap();
        let result = write_mcp_servers_config(&[], temp.path());
        assert!(result.is_none(), "empty list should return None");
    }

    #[test]
    fn test_write_mcp_servers_config_stdio() {
        let temp = tempfile::tempdir().unwrap();
        let server = acp::McpServer::Stdio(
            acp::McpServerStdio::new("my-server", "/usr/bin/node")
                .args(vec!["server.js".into(), "--verbose".into()])
                .env(vec![acp::EnvVariable::new("API_KEY", "secret123")]),
        );
        let config_dir =
            write_mcp_servers_config(&[server], temp.path()).expect("should return a config dir");
        let content = std::fs::read_to_string(config_dir.join("config.yaml")).unwrap();

        assert!(content.contains("transport: stdio"));
        assert!(content.contains("command: /usr/bin/node"));
        assert!(content.contains("server.js"));
        assert!(content.contains("--verbose"));
        assert!(content.contains("API_KEY"));
        assert!(content.contains("secret123"));
        assert!(content.contains("my-server"));
        assert!(content.contains("pass_env"));
        assert!(content.contains("PATH"));
        assert!(content.contains("HOME"));
        assert!(content.contains("log_stdout"));
        assert!(content.contains("true"));
    }

    #[test]
    fn test_write_mcp_servers_config_http() {
        let temp = tempfile::tempdir().unwrap();
        let server = acp::McpServer::Http(
            acp::McpServerHttp::new("api-server", "https://api.example.com/mcp").headers(vec![
                acp::HttpHeader::new("Authorization", "Bearer token123"),
                acp::HttpHeader::new("X-Custom-Header", "custom-value"),
            ]),
        );
        let config_dir =
            write_mcp_servers_config(&[server], temp.path()).expect("should return a config dir");
        let content = std::fs::read_to_string(config_dir.join("config.yaml")).unwrap();

        assert!(content.contains("transport: http"));
        assert!(content.contains("url: https://api.example.com/mcp"));
        assert!(content.contains("api-server"));

        // Authorization header should be mapped to the auth field, not headers
        assert!(content.contains("auth"));
        assert!(content.contains("authorization-header"));
        assert!(content.contains("Bearer token123"));

        // Non-auth header stays in headers
        assert!(content.contains("headers"));
        assert!(content.contains("X-Custom-Header"));
        assert!(content.contains("custom-value"));
    }

    #[test]
    fn test_write_mcp_servers_config_sse_skipped() {
        let temp = tempfile::tempdir().unwrap();
        let server = acp::McpServer::Sse(acp::McpServerSse::new(
            "sse-server",
            "https://sse.example.com/events",
        ));
        // SSE is not supported by polytoken, so it should be skipped.
        // The config is still written but the SSE server won't appear in it.
        let result = write_mcp_servers_config(&[server], temp.path());
        if let Some(config_dir) = result {
            let content = std::fs::read_to_string(config_dir.join("config.yaml")).unwrap();
            // SSE server name should NOT appear since we skipped it
            assert!(!content.contains("sse-server"));
            assert!(!content.contains("transport: http"));
        }
    }

    #[test]
    fn test_write_mcp_servers_config_multiple() {
        let temp = tempfile::tempdir().unwrap();
        let servers = vec![
            acp::McpServer::Stdio(acp::McpServerStdio::new("local", "/bin/tool")),
            acp::McpServer::Http(acp::McpServerHttp::new(
                "remote",
                "https://remote.example.com/mcp",
            )),
        ];
        let config_dir =
            write_mcp_servers_config(&servers, temp.path()).expect("should return a config dir");
        let content = std::fs::read_to_string(config_dir.join("config.yaml")).unwrap();

        assert!(content.contains("local"));
        assert!(content.contains("remote"));
        assert!(content.contains("stdio"));
        assert!(content.contains("http"));
    }

    // ----- Facet discovery tests -----

    #[test]
    fn test_scan_facets_dir_finds_md_files() {
        let tmp = tempfile::tempdir().unwrap();
        let facets_dir = tmp.path().join("facets");
        std::fs::create_dir_all(&facets_dir).unwrap();
        std::fs::write(facets_dir.join("execute.md"), "# execute").unwrap();
        std::fs::write(facets_dir.join("plan.md"), "# plan").unwrap();
        std::fs::write(facets_dir.join("scribe.md"), "# scribe").unwrap();

        let mut map = std::collections::BTreeMap::new();
        scan_facets_dir(&facets_dir, &mut map);
        assert_eq!(map.len(), 3);
        assert!(map.contains_key("execute"));
        assert!(map.contains_key("plan"));
        assert!(map.contains_key("scribe"));
    }

    #[test]
    fn test_scan_facets_dir_ignores_non_md() {
        let tmp = tempfile::tempdir().unwrap();
        let facets_dir = tmp.path().join("facets");
        std::fs::create_dir_all(&facets_dir).unwrap();
        std::fs::write(facets_dir.join("execute.md"), "# execute").unwrap();
        std::fs::write(facets_dir.join("notes.txt"), "not a facet").unwrap();
        std::fs::write(facets_dir.join("README"), "not a facet").unwrap();
        std::fs::create_dir(facets_dir.join("subdir.md")).unwrap();

        let mut map = std::collections::BTreeMap::new();
        scan_facets_dir(&facets_dir, &mut map);
        assert_eq!(map.len(), 1);
        assert!(map.contains_key("execute"));
    }

    #[test]
    fn test_scan_facets_dir_nonexistent() {
        let mut map = std::collections::BTreeMap::new();
        scan_facets_dir(std::path::Path::new("/nonexistent/path/facets"), &mut map);
        assert!(map.is_empty());
    }

    #[test]
    fn test_discover_facets_project_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let project_facets = tmp.path().join(".polytoken").join("facets");
        std::fs::create_dir_all(&project_facets).unwrap();
        std::fs::write(project_facets.join("scribe.md"), "# scribe").unwrap();
        std::fs::write(project_facets.join("reviewer.md"), "# reviewer").unwrap();

        let facets = discover_facets(tmp.path());
        // Should include project facets; shipped facets (execute, plan) may or may
        // not be present depending on whether `polytoken` is on PATH.
        assert!(facets.contains(&"scribe".to_string()));
        assert!(facets.contains(&"reviewer".to_string()));
        // Result should be sorted.
        assert_eq!(facets, {
            let mut v = facets.clone();
            v.sort();
            v
        });
    }

    #[test]
    fn test_discover_facets_dedup() {
        // Create a project facet with the same name as a shipped facet.
        // The result should have no duplicates.
        let tmp = tempfile::tempdir().unwrap();
        let project_facets = tmp.path().join(".polytoken").join("facets");
        std::fs::create_dir_all(&project_facets).unwrap();
        std::fs::write(project_facets.join("execute.md"), "# custom execute").unwrap();

        let facets = discover_facets(tmp.path());
        let execute_count = facets.iter().filter(|f| f.as_str() == "execute").count();
        assert_eq!(execute_count, 1, "execute should appear at most once");
    }

    #[test]
    fn test_discover_facets_empty_cwd() {
        let tmp = tempfile::tempdir().unwrap();
        let facets = discover_facets(tmp.path());
        // May include shipped facets from VFS, but project dir is empty.
        // The important invariant: no panics, valid output.
        assert!(facets.iter().all(|f| !f.is_empty()));
    }
}
