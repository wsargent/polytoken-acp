//! ACP `Agent` trait implementation for the polytoken daemon shim.

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use agent_client_protocol::{self as acp, Client};
use tracing::{debug, error, info, warn};

use crate::daemon::DaemonHandle;
use crate::events::{self, EventTranslation};

/// A boxed future used with tokio::task::spawn_local.
#[allow(dead_code)]
type LocalBoxFuture = Pin<Box<dyn Future<Output = ()>>>;

/// The polytoken ACP agent — implements `acp::Agent`.
///
/// Holds a map of ACP sessions to daemon handles, and a back-reference to
/// the `AgentSideConnection` (set after connection creation).
pub struct PolytokenAgent {
    sessions: RefCell<HashMap<String, DaemonHandle>>,
    conn: RefCell<Option<Rc<acp::AgentSideConnection>>>,
}

impl PolytokenAgent {
    pub fn new() -> Self {
        Self {
            sessions: RefCell::new(HashMap::new()),
            conn: RefCell::new(None),
        }
    }

    /// Called after `AgentSideConnection::new` to inject the connection ref.
    pub fn set_connection(&self, conn: Rc<acp::AgentSideConnection>) {
        *self.conn.borrow_mut() = Some(conn);
    }

    fn conn(&self) -> Rc<acp::AgentSideConnection> {
        self.conn.borrow().clone().expect("connection not set")
    }

    /// Terminates all daemon processes.
    pub async fn shutdown(&self) {
        // Take all daemons out of the map first, then terminate without holding borrow
        let daemons: Vec<(String, DaemonHandle)> = {
            let mut sessions = self.sessions.borrow_mut();
            sessions.drain().collect()
        };
        for (_, mut daemon) in daemons {
            daemon.terminate().await;
        }
    }
}

#[async_trait::async_trait(?Send)]
impl acp::Agent for PolytokenAgent {
    async fn initialize(&self, req: acp::InitializeRequest) -> acp::Result<acp::InitializeResponse> {
        info!("ACP initialize from client");
        let caps = acp::AgentCapabilities::new()
            .load_session(false)
            .prompt_capabilities(acp::PromptCapabilities::new().embedded_context(true))
            .mcp_capabilities(acp::McpCapabilities::new()) // http=false, sse=false
            .session_capabilities(
                acp::SessionCapabilities::new().list(acp::SessionListCapabilities::new()),
            );

        Ok(acp::InitializeResponse::new(req.protocol_version)
            .agent_capabilities(caps)
            .agent_info(
                acp::Implementation::new("polytoken", env!("CARGO_PKG_VERSION"))
                    .title("Polytoken"),
            ))
    }

    async fn authenticate(
        &self,
        _req: acp::AuthenticateRequest,
    ) -> acp::Result<acp::AuthenticateResponse> {
        Ok(acp::AuthenticateResponse::new())
    }

    async fn new_session(
        &self,
        req: acp::NewSessionRequest,
    ) -> acp::Result<acp::NewSessionResponse> {
        info!(cwd = ?req.cwd, "ACP new_session");

        // Acknowledge but ignore MCP servers — polytoken manages its own MCP config.
        if !req.mcp_servers.is_empty() {
            warn!(
                count = req.mcp_servers.len(),
                "MCP servers passed by client are acknowledged but not forwarded (v1)"
            );
        }

        let daemon = DaemonHandle::spawn(&req.cwd)
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to spawn daemon");
                acp::Error::internal_error().data(serde_json::json!({
                    "error": "Failed to start polytoken daemon",
                    "detail": e.to_string(),
                }))
            })?;

        let session_id = daemon.session_id().to_string();

        // Fetch available models from the daemon so the ACP client (Paseo)
        // can display a model selector — before moving the handle into the map.
        let models = match daemon.fetch_session_state().await {
            Ok(state) => {
                let available: Vec<acp::ModelInfo> = state
                    .available_models
                    .iter()
                    .map(|m| acp::ModelInfo::new(m.name.clone(), m.label.clone()))
                    .collect();
                let current = state
                    .active_model
                    .unwrap_or_else(|| {
                        state
                            .available_models
                            .first()
                            .map(|m| m.name.clone())
                            .unwrap_or_default()
                    });
                if available.is_empty() {
                    None
                } else {
                    Some(acp::SessionModelState::new(current, available))
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to fetch session state for models; continuing without model list");
                None
            }
        };

        self.sessions
            .borrow_mut()
            .insert(session_id.clone(), daemon);

        info!(session_id = %session_id, models = ?models.is_some(), "New session created");
        let mut response = acp::NewSessionResponse::new(session_id);
        if let Some(ms) = models {
            response = response.models(ms);
        }
        Ok(response)
    }

    async fn prompt(&self, req: acp::PromptRequest) -> acp::Result<acp::PromptResponse> {
        let session_id = req.session_id.to_string();
        let prompt_text = events::extract_text(&req.prompt);

        info!(session_id = %session_id, "ACP prompt");

        // Collect connection info without holding borrow across await
        let (events_url, bearer_token, base_url) = {
            let sessions = self.sessions.borrow();
            let daemon = sessions.get(&session_id).ok_or_else(|| {
                error!(session_id = %session_id, "Session not found");
                acp::Error::internal_error().data(serde_json::json!({
                    "error": "Session not found"
                }))
            })?;
            let events_url = daemon.events_url();
            let bearer_token = daemon.bearer_token().to_string();
            let base_url = daemon.base_url().to_string();
            (events_url, bearer_token, base_url)
        };
        // Borrow released here — send prompt with owned data
        let prompt_id = DaemonHandle::prompt_with(
            &base_url,
            &bearer_token,
            &prompt_text,
        )
        .await
        .map_err(|e| {
            error!(error = %e, "Failed to send prompt to daemon");
            acp::Error::internal_error().data(serde_json::json!({
                "error": "Failed to forward prompt to daemon",
                "detail": e.to_string(),
            }))
        })?;

        info!(session_id = %session_id, prompt_id = %prompt_id, "Prompt forwarded to daemon");

        // Get connection rc for the SSE consumer
        let conn = self.conn();

        // Channel for signaling turn completion
        let (tx, rx) = tokio::sync::oneshot::channel();

        // Spawn SSE consumer on the LocalSet
        let consumer = SseConsumer {
            conn,
            session_id: session_id.clone(),
            prompt_id: prompt_id.clone(),
            events_url,
            bearer_token,
            base_url,
            done_tx: tx,
        };

        tokio::task::spawn_local(async move {
            consumer.run().await;
        });

        // Wait for the turn to complete
        let stop_reason = match rx.await {
            Ok(reason) => reason,
            Err(_) => {
                // Consumer task was cancelled/closed unexpectedly
                error!(prompt_id = %prompt_id, "SSE consumer task ended unexpectedly");
                acp::StopReason::EndTurn
            }
        };

        info!(session_id = %session_id, prompt_id = %prompt_id, stop_reason = ?stop_reason, "Prompt turn complete");
        Ok(acp::PromptResponse::new(stop_reason))
    }

    async fn cancel(&self, req: acp::CancelNotification) -> acp::Result<()> {
        let session_id = req.session_id.to_string();
        info!(session_id = %session_id, "ACP cancel");

        let sessions = self.sessions.borrow();
        if let Some(daemon) = sessions.get(&session_id) {
            if let Err(e) = daemon.cancel_turn().await {
                warn!(error = %e, "Failed to cancel daemon turn");
            }
        }
        Ok(())
    }

    async fn list_sessions(
        &self,
        _req: acp::ListSessionsRequest,
    ) -> acp::Result<acp::ListSessionsResponse> {
        // v1 stub: return empty list
        Ok(acp::ListSessionsResponse::new(vec![]))
    }

    async fn set_session_mode(
        &self,
        _req: acp::SetSessionModeRequest,
    ) -> acp::Result<acp::SetSessionModeResponse> {
        Ok(acp::SetSessionModeResponse::new())
    }

    async fn set_session_config_option(
        &self,
        _req: acp::SetSessionConfigOptionRequest,
    ) -> acp::Result<acp::SetSessionConfigOptionResponse> {
        Ok(acp::SetSessionConfigOptionResponse::new(vec![]))
    }

    async fn set_session_model(
        &self,
        req: acp::SetSessionModelRequest,
    ) -> acp::Result<acp::SetSessionModelResponse> {
        let session_id = req.session_id.to_string();
        let model_id = req.model_id.0.to_string();

        info!(session_id = %session_id, model_id = %model_id, "ACP set_session_model");

        let sessions = self.sessions.borrow();
        let daemon = sessions.get(&session_id).ok_or_else(|| {
            error!(session_id = %session_id, "Session not found for set_session_model");
            acp::Error::internal_error().data(serde_json::json!({
                "error": "Session not found"
            }))
        })?;

        daemon
            .set_model(&model_id)
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to set model on daemon");
                acp::Error::internal_error().data(serde_json::json!({
                    "error": "Failed to switch model",
                    "detail": e.to_string(),
                }))
            })?;

        info!(session_id = %session_id, model_id = %model_id, "Model switched");
        Ok(acp::SetSessionModelResponse::new())
    }

    async fn ext_method(&self, _req: acp::ExtRequest) -> acp::Result<acp::ExtResponse> {
        Ok(acp::ExtResponse::new(
            serde_json::value::RawValue::NULL.to_owned().into(),
        ))
    }

    async fn ext_notification(&self, _req: acp::ExtNotification) -> acp::Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SSE Consumer
// ---------------------------------------------------------------------------

/// The SSE consumer task that bridges daemon events to ACP notifications.
struct SseConsumer {
    conn: Rc<acp::AgentSideConnection>,
    session_id: String,
    prompt_id: String,
    events_url: String,
    bearer_token: String,
    base_url: String,
    done_tx: tokio::sync::oneshot::Sender<acp::StopReason>,
}

impl SseConsumer {
    async fn run(self) {
        let max_retries = 3;
        let mut retry_count = 0;

        loop {
            match self.connect_and_consume().await {
                ConsumeOutcome::Done(stop_reason) => {
                    let _ = self.done_tx.send(stop_reason);
                    return;
                }
                ConsumeOutcome::Error => {
                    retry_count += 1;
                    if retry_count >= max_retries {
                        error!(
                            prompt_id = %self.prompt_id,
                            retries = retry_count,
                            "SSE consumer exhausted retries; ending turn"
                        );
                        let _ = self.done_tx.send(acp::StopReason::EndTurn);
                        return;
                    }
                    warn!(
                        prompt_id = %self.prompt_id,
                        retry = retry_count,
                        "SSE connection error; retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                ConsumeOutcome::Continue => {
                    // Stream reconnected; continue the outer loop
                }
            }
        }
    }

    async fn connect_and_consume(&self) -> ConsumeOutcome {
        let client = reqwest::Client::new();
        let response = client
            .get(&self.events_url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
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
                match self.process_sse_event(&data).await {
                    ConsumeOutcome::Done(reason) => return ConsumeOutcome::Done(reason),
                    ConsumeOutcome::Error => return ConsumeOutcome::Error,
                    ConsumeOutcome::Continue => {}
                }
            }
        }

        // Stream ended without message_complete
        warn!(prompt_id = %self.prompt_id, "SSE stream ended without explicit turn end");
        ConsumeOutcome::Done(acp::StopReason::EndTurn)
    }

    async fn process_sse_event(&self, data: &str) -> ConsumeOutcome {
        let event = match events::parse_sse_event(data) {
            Some(e) => e,
            None => {
                debug!(data = %data.chars().take(200).collect::<String>(), "Failed to parse SSE event; skipping");
                return ConsumeOutcome::Continue;
            }
        };

        debug!(event_type = ?std::mem::discriminant(&event), "SSE event received");

        // Filter by prompt_id if the event has one
        if let Some(epid) = events::event_prompt_id(&event) {
            if epid != self.prompt_id {
                return ConsumeOutcome::Continue;
            }
        }

        match events::translate_event(&event) {
            EventTranslation::Update(update) => {
                let session_id = acp::SessionId::new(self.session_id.clone());
                let notification = acp::SessionNotification::new(session_id, update.clone());
                if let Err(e) = self.conn.session_notification(notification).await {
                    error!(error = %e, "Failed to send session notification");
                }
                ConsumeOutcome::Continue
            }
            EventTranslation::TurnEnd => {
                // Small delay to ensure pending notifications are flushed
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                ConsumeOutcome::Done(acp::StopReason::EndTurn)
            }
            EventTranslation::TurnCancelled => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                ConsumeOutcome::Done(acp::StopReason::Cancelled)
            }
            EventTranslation::PermissionRequest {
                interrogative_id,
                question,
            } => {
                self.handle_permission(&interrogative_id.clone(), &question.clone()).await;
                ConsumeOutcome::Continue
            }
            EventTranslation::Ignore => ConsumeOutcome::Continue,
        }
    }

    async fn handle_permission(&self, interrogative_id: &str, question: &str) {
        info!(interrogative_id = %interrogative_id, "Forwarding permission request to ACP client");

        let options = events::build_permission_options();
        let tool_call_update = acp::ToolCallUpdate::new(
            interrogative_id.to_string(),
            acp::ToolCallUpdateFields::new()
                .title(question.to_string())
                .status(acp::ToolCallStatus::Pending),
        );

        let session_id = acp::SessionId::new(self.session_id.clone());
        let request = acp::RequestPermissionRequest::new(
            session_id,
            tool_call_update,
            options,
        );

        match self.conn.request_permission(request).await {
            Ok(response) => {
                let granted = events::resolve_permission_outcome(&response.outcome);
                info!(interrogative_id = %interrogative_id, granted, "Permission response from client");

                // Respond to the daemon
                if let Err(e) =
                    Self::respond_interrogative(&self.base_url, &self.bearer_token, interrogative_id, granted)
                        .await
                {
                    error!(error = %e, "Failed to respond to interrogative on daemon");
                }
            }
            Err(e) => {
                error!(error = %e, "Permission request failed");
                // Default to not granted
                let _ = Self::respond_interrogative(
                    &self.base_url,
                    &self.bearer_token,
                    interrogative_id,
                    false,
                )
                .await;
            }
        }
    }

    async fn respond_interrogative(
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
}

enum ConsumeOutcome {
    Continue,
    Done(acp::StopReason),
    Error,
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
