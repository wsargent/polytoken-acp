//! Daemon SSE event deserialization and ACP translation.

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use agent_client_protocol::schema::v1 as acp;

// ---------------------------------------------------------------------------
// Daemon event types (from GET /events SSE stream)
// ---------------------------------------------------------------------------

/// The SSE envelope wrapping each event.
///
/// Each SSE `data:` line from the daemon looks like:
/// `{ "seq": N, "emitted_at": "...", "session_id": "...", "event": { "type": "...", ... } }`
#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct DaemonEventEnvelope {
    #[serde(default)]
    pub seq: Option<u64>,
    #[serde(default)]
    pub event: Option<serde_json::Value>,
}

/// Parse an SSE data line: unwrap the envelope, then deserialize the inner event.
pub fn parse_sse_event(data: &str) -> Option<DaemonEvent> {
    // First try to parse as an envelope (with "event" wrapper)
    if let Ok(envelope) = serde_json::from_str::<DaemonEventEnvelope>(data)
        && let Some(event_value) = envelope.event
        && let Ok(evt) = serde_json::from_value::<DaemonEvent>(event_value)
    {
        return Some(evt);
    }
    // Fallback: try parsing directly as a DaemonEvent (no envelope)
    serde_json::from_str::<DaemonEvent>(data).ok()
}

/// Tagged union of daemon event types we handle.
#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[allow(dead_code)]
/// Events emitted by the Polytoken daemon SSE stream.
///
/// `Interrogative` is the largest variant (it carries optional
/// `PlanHandoffContext` and `GoalProposalContext` payloads), which makes
/// boxing impractical for a deserialized event enum. The size is acceptable
/// because each event is processed once and dropped immediately.
#[allow(clippy::large_enum_variant)]
pub enum DaemonEvent {
    #[serde(rename = "message_start")]
    MessageStart { prompt_id: String },
    #[serde(rename = "content_block_start")]
    ContentBlockStart {
        prompt_id: String,
        block_index: u32,
        #[serde(default)]
        block_type: serde_json::Value,
    },
    #[serde(rename = "content_block_delta")]
    ContentBlockDelta {
        prompt_id: String,
        block_index: u32,
        delta: BlockDeltaPayload,
    },
    #[serde(rename = "content_block_stop")]
    ContentBlockStop { prompt_id: String, block_index: u32 },
    #[serde(rename = "message_complete")]
    MessageComplete { prompt_id: String },
    #[serde(rename = "turn_cancelled")]
    TurnCancelled { prompt_id: String },
    #[serde(rename = "model_error")]
    ModelError { prompt_id: String },
    #[serde(rename = "tool_call")]
    ToolCall {
        prompt_id: String,
        call_id: String,
        name: String,
        #[serde(default)]
        input: Option<serde_json::Value>,
    },
    #[serde(rename = "tool_result")]
    ToolResult {
        prompt_id: String,
        call_id: String,
        #[serde(default)]
        content: Option<String>,
        #[serde(default)]
        content_full: Option<String>,
        #[serde(default)]
        is_error: Option<bool>,
    },
    #[serde(rename = "interrogative")]
    Interrogative {
        prompt_id: String,
        interrogative_id: String,
        question: String,
        interrogative_type: String,
        /// Structured payload present when `interrogative_type == "plan_handoff"`.
        /// Carries the plan review surface (plan text, title, action labels) so
        /// non-TUI clients can render the plan-to-execution choice.
        #[serde(default)]
        plan_handoff: Option<PlanHandoffContext>,
        /// Structured payload present when `interrogative_type == "goal_proposal"`.
        /// Carries the proposed goal summary and accept/reject labels so non-TUI
        /// clients can render the binary approval surface.
        #[serde(default)]
        goal_proposal: Option<GoalProposalContext>,
    },
    #[serde(rename = "ask_user_question")]
    AskUserQuestion {
        prompt_id: String,
        interrogative_id: String,
        payload: AskUserQuestionPayload,
    },
    #[serde(rename = "model_changed")]
    ModelChanged { model: String },
    #[serde(rename = "session_title_changed")]
    SessionTitleChanged { title: String },
    #[serde(rename = "facet_changed")]
    FacetChanged { facet: String },
    #[serde(rename = "subagent_started")]
    SubagentStarted {
        handle: String,
        subagent_type: String,
        model: String,
    },
    #[serde(rename = "subagent_completed")]
    SubagentCompleted {
        handle: String,
        #[serde(default)]
        outcome: Option<SubagentOutcome>,
        #[serde(default)]
        result_summary: Option<String>,
    },
    #[serde(rename = "job_promoted")]
    JobPromoted {
        job_id: String,
        #[serde(default)]
        subagent_handle: Option<String>,
    },
    #[serde(rename = "job_completed")]
    JobCompleted {
        job_id: String,
        exit_code: i32,
        #[serde(default)]
        subagent_handle: Option<String>,
    },
    #[serde(rename = "job_expiring")]
    JobExpiring {
        job_id: String,
        #[serde(default)]
        subagent_handle: Option<String>,
    },
    #[serde(rename = "job_cancelled")]
    JobCancelled {
        job_id: String,
        #[serde(default)]
        subagent_handle: Option<String>,
    },
    #[serde(rename = "job_updated")]
    JobUpdated {
        job_id: String,
        #[serde(default)]
        subagent_handle: Option<String>,
    },
    #[serde(rename = "session_state_changed")]
    SessionStateChanged {
        #[serde(default)]
        domains: Vec<String>,
    },
    #[serde(rename = "todo_status_nudge")]
    TodoStatusNudge,
    #[serde(rename = "goal_driver_update")]
    GoalDriverUpdate {
        transition: String,
        #[serde(default)]
        goal: Option<serde_json::Value>,
        #[serde(default)]
        proposed_summary: Option<String>,
    },
    #[serde(rename = "system_reminder")]
    SystemReminder {
        slug: String,
        display_name: String,
        body: String,
        reason: serde_json::Value,
    },
    #[serde(rename = "usage_throttle")]
    UsageThrottle {
        #[serde(default)]
        prompt_id: Option<String>,
        provider: String,
    },
    #[serde(rename = "heartbeat")]
    Heartbeat,
    #[serde(rename = "permission_monitor_switch")]
    PermissionMonitorSwitch {
        to_monitor: PermissionMonitorSummary,
        from_monitor: PermissionMonitorSummary,
    },
    #[serde(other)]
    Other,
}

/// Outcome kind reported by the daemon when a subagent finishes.
///
/// Corresponds to `SubagentResultKind` in the daemon's event schema.
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubagentResultKind {
    Success,
    Failure,
    Cancelled,
}

/// The `outcome` object on `subagent_completed` events.
///
/// Corresponds to `SubagentOutcome` in the daemon's event schema.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SubagentOutcome {
    pub kind: SubagentResultKind,
    #[serde(default)]
    pub message: Option<String>,
}

/// Delta payload for content_block_delta events.
#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
pub enum BlockDeltaPayload {
    #[serde(rename = "text")]
    TextDelta { text: String },
    #[serde(rename = "tool_use_input")]
    ToolUseInput { partial_json: String },
    #[serde(rename = "thinking")]
    Thinking { text: String },
    #[serde(rename = "signature_delta")]
    SignatureDelta { signature: String },
    #[serde(rename = "redacted_thinking")]
    RedactedThinking { data: String },
    #[serde(rename = "open_ai_reasoning_opaque")]
    OpenAiReasoning { id: String, data: String },
    #[serde(other)]
    Other,
}

/// Minimal extraction of the permission monitor `type` discriminator.
/// The full PermissionMonitor tagged union has additional variant-specific
/// fields (classifier_model, classifier_rules, max_consecutive_denials for
/// autonomous), but we only need the mode string for config option updates.
#[derive(Deserialize, Debug, Clone)]
pub struct PermissionMonitorSummary {
    #[serde(rename = "type")]
    pub kind: String,
}

// ---------------------------------------------------------------------------
// ask_user_question payload types (from daemon SSE events)
// ---------------------------------------------------------------------------

/// The payload carried by an `ask_user_question` daemon event.
///
/// Contains a batch of questions the agent wants the user to answer.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct AskUserQuestionPayload {
    pub questions: Vec<AskUserQuestionItem>,
}

/// One question in an `ask_user_question` batch.
#[derive(Deserialize, Serialize, Debug, Clone)]
#[allow(dead_code)]
pub struct AskUserQuestionItem {
    pub id: String,
    #[serde(default)]
    pub context: Option<String>,
    pub question: String,
    pub mode: AskUserQuestionMode,
    #[serde(default = "default_true")]
    pub allow_free_text: bool,
    #[serde(default)]
    pub options: Vec<AskUserQuestionOption>,
}

/// The selection mode for a question.
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AskUserQuestionMode {
    SingleSelect,
    MultiSelect,
    Text,
}

/// One selectable option for a question.
#[derive(Deserialize, Serialize, Debug, Clone)]
#[allow(dead_code)]
pub struct AskUserQuestionOption {
    pub id: String,
    pub label: String,
    pub description: String,
    #[serde(default)]
    pub justification: Option<String>,
    #[serde(default)]
    pub preview: Option<String>,
}

fn default_true() -> bool {
    true
}

// ---------------------------------------------------------------------------
// plan_handoff interrogative payload types (from daemon SSE events)
// ---------------------------------------------------------------------------

/// Structured payload attached to a `plan_handoff` interrogative.
///
/// The daemon carries the presentation strings on the event so non-TUI clients
/// (like polytoken-acp) can render the same plan-review surface without
/// reconstructing daemon-local state. This is the "plan → execution" transition.
#[derive(Deserialize, Serialize, Debug, Clone)]
#[allow(dead_code)]
pub struct PlanHandoffContext {
    pub plan_path: String,
    pub display_path: String,
    pub plan_text: String,
    pub target_facet: String,
    pub title: String,
    pub action_labels: PlanHandoffActionLabels,
}

/// Human-readable labels for each plan-handoff decision, supplied by the daemon.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct PlanHandoffActionLabels {
    pub implement_new_context: String,
    pub implement_current_context: String,
    pub cancel: String,
    #[serde(default)]
    pub refuse: String,
}

// ---------------------------------------------------------------------------
// Goal proposal (goal_proposal interrogative)
// ---------------------------------------------------------------------------

/// Structured payload attached to a `goal_proposal` interrogative. Carries
/// the proposal summary so non-TUI clients can render the same review surface.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct GoalProposalContext {
    pub proposed_summary: String,
    #[serde(default)]
    pub proposed_file_path: Option<String>,
    pub title: String,
    pub action_labels: GoalProposalActionLabels,
}

/// Labels for the binary approve/reject affordance on a goal proposal.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct GoalProposalActionLabels {
    pub accept: String,
    pub reject: String,
}

// ---------------------------------------------------------------------------
// Translation: daemon event → ACP SessionUpdate
// ---------------------------------------------------------------------------

/// Outcome of processing a daemon event — either something to send to the
/// ACP client, or a signal that the turn is complete.
#[allow(clippy::large_enum_variant)]
pub enum EventTranslation {
    /// Send this SessionUpdate to the ACP client.
    Update(acp::SessionUpdate),
    /// The turn ended naturally (message_complete).
    TurnEnd,
    /// The turn was cancelled.
    TurnCancelled,
    /// A permission request from the daemon that needs an ACP round-trip.
    PermissionRequest {
        interrogative_id: String,
        question: String,
    },
    /// A non-permission interrogative (confirmation, clarification, etc.) that
    /// needs an ACP round-trip via the permission request mechanism.
    InterrogativeRequest {
        interrogative_id: String,
        question: String,
        interrogative_type: String,
    },
    /// An ask_user_question request from the daemon that needs an ACP round-trip
    /// via ext_method.
    AskUserQuestion {
        interrogative_id: String,
        payload: AskUserQuestionPayload,
    },
    /// A `plan_handoff` interrogative — the plan-to-execution transition. Carries
    /// the structured plan-review context so it can be forwarded as a rich
    /// single-select question and answered with a `plan_handoff_answer`.
    PlanHandoff {
        interrogative_id: String,
        question: String,
        context: PlanHandoffContext,
    },
    /// A `goal_proposal` interrogative. Carries the proposed goal summary and
    /// accept/reject labels so the client can present the binary choice as a
    /// single-select question. The answer is mapped back to a
    /// `goal_proposal_answer`.
    GoalProposal {
        interrogative_id: String,
        question: String,
        context: GoalProposalContext,
    },
    /// A subagent started — emit a ToolCall + extension notification.
    SubagentStarted {
        handle: String,
        subagent_type: String,
        model: String,
    },
    /// A subagent completed — emit a ToolCallUpdate + extension notification.
    SubagentCompleted {
        handle: String,
        result_summary: Option<String>,
        outcome_kind: Option<SubagentResultKind>,
        outcome_message: Option<String>,
    },
    /// A job lifecycle event — emit extension notification (and ToolCall for shell jobs).
    /// Subagent jobs (subagent_handle is Some) are skipped to avoid duplicates with
    /// the SubagentStarted/SubagentCompleted translations.
    JobEvent {
        job_id: String,
        event_type: String,
        #[allow(dead_code)]
        subagent_handle: Option<String>,
        exit_code: Option<i32>,
    },
    /// A system reminder — forward as ext notification.
    SystemReminder {
        slug: String,
        display_name: String,
        body: String,
        reason: serde_json::Value,
    },
    /// A goal driver update — forward as ACP Plan.
    GoalDriverUpdate { transition: String, summary: String },
    /// Re-fetch /state for todo/plan updates.
    TodoStateChange,
    /// Re-discover facets and send updated available_commands_update if changed.
    FacetChoicesCheck,
    /// Permission monitor mode changed externally — re-fetch and send ConfigOptionUpdate.
    PermissionMonitorSwitch { mode: String },
    /// Nothing to send (heartbeat, unknown, etc.)
    Ignore,
}

/// Human-readable type name for a daemon event (for logging).
pub fn event_type_name(evt: &DaemonEvent) -> &'static str {
    match evt {
        DaemonEvent::MessageStart { .. } => "message_start",
        DaemonEvent::ContentBlockStart { .. } => "content_block_start",
        DaemonEvent::ContentBlockDelta { .. } => "content_block_delta",
        DaemonEvent::ContentBlockStop { .. } => "content_block_stop",
        DaemonEvent::MessageComplete { .. } => "message_complete",
        DaemonEvent::TurnCancelled { .. } => "turn_cancelled",
        DaemonEvent::ModelError { .. } => "model_error",
        DaemonEvent::ToolCall { .. } => "tool_call",
        DaemonEvent::ToolResult { .. } => "tool_result",
        DaemonEvent::Interrogative { .. } => "interrogative",
        DaemonEvent::AskUserQuestion { .. } => "ask_user_question",
        DaemonEvent::ModelChanged { .. } => "model_changed",
        DaemonEvent::SessionTitleChanged { .. } => "session_title_changed",
        DaemonEvent::FacetChanged { .. } => "facet_changed",
        DaemonEvent::SubagentStarted { .. } => "subagent_started",
        DaemonEvent::SubagentCompleted { .. } => "subagent_completed",
        DaemonEvent::JobPromoted { .. } => "job_promoted",
        DaemonEvent::JobCompleted { .. } => "job_completed",
        DaemonEvent::JobExpiring { .. } => "job_expiring",
        DaemonEvent::JobCancelled { .. } => "job_cancelled",
        DaemonEvent::JobUpdated { .. } => "job_updated",
        DaemonEvent::SessionStateChanged { .. } => "session_state_changed",
        DaemonEvent::TodoStatusNudge => "todo_status_nudge",
        DaemonEvent::GoalDriverUpdate { .. } => "goal_driver_update",
        DaemonEvent::SystemReminder { .. } => "system_reminder",
        DaemonEvent::UsageThrottle { .. } => "usage_throttle",
        DaemonEvent::Heartbeat => "heartbeat",
        DaemonEvent::PermissionMonitorSwitch { .. } => "permission_monitor_switch",
        DaemonEvent::Other => "unknown",
    }
}

/// Human-readable summary of key fields for a daemon event (for logging).
/// Returns a compact, single-line description suitable for log fields.
pub fn event_summary(evt: &DaemonEvent) -> String {
    match evt {
        DaemonEvent::MessageStart { prompt_id } => format!("prompt_id={}", prompt_id),
        DaemonEvent::ContentBlockStart {
            prompt_id,
            block_index,
            ..
        } => {
            format!("prompt_id={} block_index={}", prompt_id, block_index)
        }
        DaemonEvent::ContentBlockDelta {
            prompt_id,
            block_index,
            delta,
        } => {
            let delta_desc = match delta {
                BlockDeltaPayload::TextDelta { text } => {
                    let preview: String = text.chars().take(80).collect();
                    format!("text_delta len={} preview={:?}", text.len(), preview)
                }
                BlockDeltaPayload::ToolUseInput { partial_json } => {
                    format!("tool_use_input len={}", partial_json.len())
                }
                BlockDeltaPayload::Thinking { text } => {
                    let preview: String = text.chars().take(80).collect();
                    format!("thinking len={} preview={:?}", text.len(), preview)
                }
                BlockDeltaPayload::SignatureDelta { .. } => "signature_delta".to_string(),
                BlockDeltaPayload::RedactedThinking { data } => {
                    format!("redacted_thinking len={}", data.len())
                }
                BlockDeltaPayload::OpenAiReasoning { id, data } => {
                    format!("openai_reasoning id={} len={}", id, data.len())
                }
                BlockDeltaPayload::Other => "other_delta".to_string(),
            };
            format!(
                "prompt_id={} block_index={} {}",
                prompt_id, block_index, delta_desc
            )
        }
        DaemonEvent::ContentBlockStop {
            prompt_id,
            block_index,
        } => {
            format!("prompt_id={} block_index={}", prompt_id, block_index)
        }
        DaemonEvent::MessageComplete { prompt_id } => format!("prompt_id={}", prompt_id),
        DaemonEvent::TurnCancelled { prompt_id } => format!("prompt_id={}", prompt_id),
        DaemonEvent::ModelError { prompt_id } => format!("prompt_id={}", prompt_id),
        DaemonEvent::ToolCall {
            prompt_id,
            call_id,
            name,
            input,
            ..
        } => {
            let input_desc = match input {
                Some(v) => {
                    let s = serde_json::to_string(v).unwrap_or_default();
                    let preview: String = s.chars().take(120).collect();
                    format!(" input={}", preview)
                }
                None => String::new(),
            };
            format!(
                "prompt_id={} call_id={} name={}{}",
                prompt_id, call_id, name, input_desc
            )
        }
        DaemonEvent::ToolResult {
            prompt_id,
            call_id,
            content,
            content_full,
            is_error,
            ..
        } => {
            let content_desc = if let Some(c) = content_full.as_ref().or(content.as_ref()) {
                let preview: String = c.chars().take(120).collect();
                format!(" content={}", preview)
            } else {
                String::new()
            };
            format!(
                "prompt_id={} call_id={} is_error={}{}",
                prompt_id,
                call_id,
                is_error.unwrap_or(false),
                content_desc
            )
        }
        DaemonEvent::Interrogative {
            prompt_id,
            interrogative_id,
            question,
            interrogative_type,
            ..
        } => {
            let preview: String = question.chars().take(100).collect();
            format!(
                "prompt_id={} id={} type={} question={:?}",
                prompt_id, interrogative_id, interrogative_type, preview
            )
        }
        DaemonEvent::AskUserQuestion {
            prompt_id,
            interrogative_id,
            payload,
            ..
        } => {
            format!(
                "prompt_id={} id={} questions={}",
                prompt_id,
                interrogative_id,
                payload.questions.len()
            )
        }
        DaemonEvent::ModelChanged { model } => format!("model={}", model),
        DaemonEvent::SessionTitleChanged { title } => format!("title={}", title),
        DaemonEvent::FacetChanged { facet } => format!("facet={}", facet),
        DaemonEvent::SubagentStarted {
            handle,
            subagent_type,
            model,
        } => format!("handle={} type={} model={}", handle, subagent_type, model),
        DaemonEvent::SubagentCompleted {
            handle,
            outcome,
            result_summary,
        } => {
            let summary = result_summary.as_deref().unwrap_or("(none)");
            let outcome_str = outcome.as_ref().map(|o| match o.kind {
                SubagentResultKind::Success => "success",
                SubagentResultKind::Failure => "failure",
                SubagentResultKind::Cancelled => "cancelled",
            });
            format!(
                "handle={} summary={} outcome={}",
                handle,
                summary,
                outcome_str.unwrap_or("(none)")
            )
        }
        DaemonEvent::JobPromoted {
            job_id,
            subagent_handle,
        } => {
            format!("job_id={} subagent={:?}", job_id, subagent_handle)
        }
        DaemonEvent::JobCompleted {
            job_id,
            exit_code,
            subagent_handle,
        } => {
            format!(
                "job_id={} exit_code={} subagent={:?}",
                job_id, exit_code, subagent_handle
            )
        }
        DaemonEvent::JobExpiring {
            job_id,
            subagent_handle,
        } => {
            format!("job_id={} subagent={:?}", job_id, subagent_handle)
        }
        DaemonEvent::JobCancelled {
            job_id,
            subagent_handle,
        } => {
            format!("job_id={} subagent={:?}", job_id, subagent_handle)
        }
        DaemonEvent::JobUpdated {
            job_id,
            subagent_handle,
        } => {
            format!("job_id={} subagent={:?}", job_id, subagent_handle)
        }
        DaemonEvent::SessionStateChanged { domains } => format!("domains={:?}", domains),
        DaemonEvent::TodoStatusNudge => String::new(),
        DaemonEvent::GoalDriverUpdate {
            transition,
            goal,
            proposed_summary,
        } => {
            let summary = goal
                .as_ref()
                .and_then(|g| g.get("summary"))
                .and_then(|s| s.as_str())
                .or(proposed_summary.as_deref())
                .unwrap_or("");
            format!("transition={} summary={}", transition, summary)
        }
        DaemonEvent::SystemReminder {
            slug, display_name, ..
        } => {
            format!("slug={} name={}", slug, display_name)
        }
        DaemonEvent::UsageThrottle { provider, .. } => format!("provider={}", provider),
        DaemonEvent::Heartbeat => String::new(),
        DaemonEvent::PermissionMonitorSwitch { to_monitor, .. } => {
            format!("mode={}", to_monitor.kind)
        }
        DaemonEvent::Other => String::new(),
    }
}

/// Human-readable name for an ACP SessionUpdate variant (for logging).
pub fn session_update_name(update: &acp::SessionUpdate) -> &'static str {
    match update {
        acp::SessionUpdate::UserMessageChunk(_) => "user_message_chunk",
        acp::SessionUpdate::AgentMessageChunk(_) => "agent_message_chunk",
        acp::SessionUpdate::AgentThoughtChunk(_) => "agent_thought_chunk",
        acp::SessionUpdate::ToolCall(_) => "tool_call",
        acp::SessionUpdate::ToolCallUpdate(_) => "tool_call_update",
        acp::SessionUpdate::Plan(_) => "plan",
        acp::SessionUpdate::AvailableCommandsUpdate(_) => "available_commands_update",
        acp::SessionUpdate::CurrentModeUpdate(_) => "current_mode_update",
        acp::SessionUpdate::ConfigOptionUpdate(_) => "config_option_update",
        acp::SessionUpdate::SessionInfoUpdate(_) => "session_info_update",
        acp::SessionUpdate::UsageUpdate(_) => "usage_update",
        _ => "other",
    }
}

/// Extract the prompt_id from a daemon event (if present).
pub fn event_prompt_id(evt: &DaemonEvent) -> Option<&str> {
    match evt {
        DaemonEvent::MessageStart { prompt_id }
        | DaemonEvent::ContentBlockStart { prompt_id, .. }
        | DaemonEvent::ContentBlockDelta { prompt_id, .. }
        | DaemonEvent::ContentBlockStop { prompt_id, .. }
        | DaemonEvent::MessageComplete { prompt_id }
        | DaemonEvent::TurnCancelled { prompt_id }
        | DaemonEvent::ModelError { prompt_id }
        | DaemonEvent::ToolCall { prompt_id, .. }
        | DaemonEvent::ToolResult { prompt_id, .. }
        | DaemonEvent::Interrogative { prompt_id, .. }
        | DaemonEvent::AskUserQuestion { prompt_id, .. } => Some(prompt_id.as_str()),
        _ => None,
    }
}

/// Translate a daemon event into an ACP action.
pub fn translate_event(evt: &DaemonEvent) -> EventTranslation {
    match evt {
        // Text delta → AgentMessageChunk
        DaemonEvent::ContentBlockDelta {
            delta: BlockDeltaPayload::TextDelta { text },
            ..
        } => {
            let content_block: acp::ContentBlock = text.clone().into();
            let chunk = acp::ContentChunk::new(content_block);
            EventTranslation::Update(acp::SessionUpdate::AgentMessageChunk(chunk))
        }

        // Thinking delta → AgentThoughtChunk
        DaemonEvent::ContentBlockDelta {
            delta: BlockDeltaPayload::Thinking { text },
            ..
        } => {
            let content_block: acp::ContentBlock = text.clone().into();
            let chunk = acp::ContentChunk::new(content_block);
            EventTranslation::Update(acp::SessionUpdate::AgentThoughtChunk(chunk))
        }

        // Redacted thinking → AgentThoughtChunk (placeholder text)
        DaemonEvent::ContentBlockDelta {
            delta: BlockDeltaPayload::RedactedThinking { data },
            ..
        } => {
            let text = format!("[redacted reasoning: {}]", data);
            let content_block: acp::ContentBlock = text.into();
            let chunk = acp::ContentChunk::new(content_block);
            EventTranslation::Update(acp::SessionUpdate::AgentThoughtChunk(chunk))
        }

        // Signature delta → ignore (no ACP equivalent)
        DaemonEvent::ContentBlockDelta {
            delta: BlockDeltaPayload::SignatureDelta { .. },
            ..
        } => EventTranslation::Ignore,

        // OpenAI reasoning → AgentThoughtChunk
        DaemonEvent::ContentBlockDelta {
            delta: BlockDeltaPayload::OpenAiReasoning { data, .. },
            ..
        } => {
            let content_block: acp::ContentBlock = data.clone().into();
            let chunk = acp::ContentChunk::new(content_block);
            EventTranslation::Update(acp::SessionUpdate::AgentThoughtChunk(chunk))
        }

        // Other content_block_delta variants → ignore
        DaemonEvent::ContentBlockDelta { .. } => EventTranslation::Ignore,

        // Message start → ignore (no ACP equivalent needed)
        DaemonEvent::MessageStart { .. } => EventTranslation::Ignore,

        // Tool call → ToolCall session update
        DaemonEvent::ToolCall {
            call_id,
            name,
            input,
            ..
        } => {
            let kind = tool_kind_for_name(name);
            let mut tool_call = acp::ToolCall::new(call_id.clone(), name.clone())
                .kind(kind)
                .status(acp::ToolCallStatus::Pending);
            if let Some(input) = input {
                // For tools with large/redundant payloads (e.g.
                // ask_user_question), suppress raw_input and set a
                // concise title instead so the transcript stays clean.
                if let Some(title) = tool_call_title_override(name, input) {
                    tool_call.title = title;
                } else {
                    tool_call = tool_call.raw_input(input.clone());
                    // Extract file locations from input
                    if let Some(locs) = extract_locations(input) {
                        tool_call = tool_call.locations(locs);
                    }
                }
            }
            EventTranslation::Update(acp::SessionUpdate::ToolCall(tool_call))
        }

        // Tool result → ToolCallUpdate
        DaemonEvent::ToolResult {
            call_id,
            content,
            content_full,
            is_error,
            ..
        } => {
            let status = if is_error.unwrap_or(false) {
                acp::ToolCallStatus::Failed
            } else {
                acp::ToolCallStatus::Completed
            };
            let mut fields = acp::ToolCallUpdateFields::new().status(status);
            // Prefer content_full over content for the tool call output
            let effective_content = content_full.as_ref().or(content.as_ref());
            if let Some(c) = effective_content {
                // Try to parse content as JSON for raw_output
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(c) {
                    fields = fields.raw_output(json_val);
                }
                let block: acp::ContentBlock = c.clone().into();
                fields = fields.content(vec![acp::ToolCallContent::from(block)]);
            }
            let update = acp::ToolCallUpdate::new(call_id.clone(), fields);
            EventTranslation::Update(acp::SessionUpdate::ToolCallUpdate(update))
        }

        // Message complete → turn end
        DaemonEvent::MessageComplete { .. } => EventTranslation::TurnEnd,

        // Turn cancelled
        DaemonEvent::TurnCancelled { .. } => EventTranslation::TurnCancelled,

        // Model error → treat as turn end (best effort)
        DaemonEvent::ModelError { .. } => {
            warn!("Received model_error event; treating as turn end");
            EventTranslation::TurnEnd
        }

        // Interrogative (permission) → forward to ACP client
        DaemonEvent::Interrogative {
            interrogative_id,
            question,
            interrogative_type,
            plan_handoff,
            goal_proposal,
            ..
        } => {
            if interrogative_type == "permission" {
                debug!(interrogative_type = %interrogative_type, "Permission request from daemon");
                EventTranslation::PermissionRequest {
                    interrogative_id: interrogative_id.clone(),
                    question: question.clone(),
                }
            } else if interrogative_type == "plan_handoff"
                && let Some(context) = plan_handoff
            {
                // The plan-to-execution transition is a multi-way choice
                // (new context / current context / refuse / cancel). Forward it
                // as a rich single-select question rather than a binary
                // allow/reject so the user's decision reaches the daemon intact.
                debug!("plan_handoff interrogative from daemon; forwarding as structured choice");
                EventTranslation::PlanHandoff {
                    interrogative_id: interrogative_id.clone(),
                    question: question.clone(),
                    context: context.clone(),
                }
            } else if interrogative_type == "goal_proposal"
                && let Some(context) = goal_proposal
            {
                // Goal proposals are a binary accept/reject choice. Route them
                // through the dedicated handler so the client renders a proper
                // question surface (elicitation form or text fallback) instead
                // of falling through to the generic permission path where some
                // clients auto-reject.
                debug!("goal_proposal interrogative from daemon; forwarding as structured choice");
                EventTranslation::GoalProposal {
                    interrogative_id: interrogative_id.clone(),
                    question: question.clone(),
                    context: context.clone(),
                }
            } else {
                // Forward non-permission interrogatives (confirmation, clarification,
                // capability, etc.) as permission-like requests
                debug!(
                    interrogative_type = %interrogative_type,
                    "Non-permission interrogative from daemon; forwarding to ACP client"
                );
                EventTranslation::InterrogativeRequest {
                    interrogative_id: interrogative_id.clone(),
                    question: question.clone(),
                    interrogative_type: interrogative_type.clone(),
                }
            }
        }

        // ask_user_question → forward to ACP client via ext_method
        DaemonEvent::AskUserQuestion {
            interrogative_id,
            payload,
            ..
        } => {
            debug!(
                interrogative_id = %interrogative_id,
                question_count = payload.questions.len(),
                "ask_user_question event received; forwarding to ACP client"
            );
            EventTranslation::AskUserQuestion {
                interrogative_id: interrogative_id.clone(),
                payload: payload.clone(),
            }
        }

        // Model changed → send config_option_update so client refreshes model picker
        DaemonEvent::ModelChanged { model } => {
            debug!(model = %model, "Model changed; forwarding as session_info_update");
            // We can't build a full config_option_update here without the full list,
            // so we send a session_info_update with the model in _meta for clients
            // that want it. The model config option is already set from /state.
            EventTranslation::Ignore
        }

        // Session title changed → forward as session_info_update
        DaemonEvent::SessionTitleChanged { title } => {
            debug!(title = %title, "Session title changed; forwarding as session_info_update");
            let info_update = acp::SessionInfoUpdate::new().title(title.clone());
            EventTranslation::Update(acp::SessionUpdate::SessionInfoUpdate(info_update))
        }

        // Facet changed → no longer forwarded as current_mode_update.
        // Mode now maps to the permission monitor, not facets. Facet
        // switching is handled via the /facet slash command. We log it
        // but don't emit an ACP notification.
        DaemonEvent::FacetChanged { facet } => {
            debug!(facet = %facet, "Facet changed; not forwarding (mode = permissions)");
            EventTranslation::Ignore
        }

        // Subagent started → emit as ToolCall + extension notification
        DaemonEvent::SubagentStarted {
            handle,
            subagent_type,
            model,
        } => {
            debug!(handle = %handle, subagent_type = %subagent_type, "Subagent started");
            EventTranslation::SubagentStarted {
                handle: handle.clone(),
                subagent_type: subagent_type.clone(),
                model: model.clone(),
            }
        }

        // Subagent completed → emit as ToolCallUpdate + extension notification
        DaemonEvent::SubagentCompleted {
            handle,
            outcome,
            result_summary,
        } => {
            debug!(handle = %handle, "Subagent completed");
            EventTranslation::SubagentCompleted {
                handle: handle.clone(),
                result_summary: result_summary.clone(),
                outcome_kind: outcome.as_ref().map(|o| o.kind.clone()),
                outcome_message: outcome.as_ref().and_then(|o| o.message.clone()),
            }
        }

        // Job lifecycle events. All 5 types go through translate_job_event,
        // which skips subagent jobs (already handled by SubagentStarted/Completed).
        DaemonEvent::JobPromoted {
            job_id,
            subagent_handle,
        } => translate_job_event(job_id, "job_promoted", subagent_handle, None),
        DaemonEvent::JobCompleted {
            job_id,
            exit_code,
            subagent_handle,
        } => translate_job_event(job_id, "job_completed", subagent_handle, Some(*exit_code)),
        DaemonEvent::JobExpiring {
            job_id,
            subagent_handle,
        } => translate_job_event(job_id, "job_expiring", subagent_handle, None),
        DaemonEvent::JobCancelled {
            job_id,
            subagent_handle,
        } => translate_job_event(job_id, "job_cancelled", subagent_handle, None),
        DaemonEvent::JobUpdated {
            job_id,
            subagent_handle,
        } => translate_job_event(job_id, "job_updated", subagent_handle, None),

        // Session state changed → re-fetch /state for todo updates if domains
        // include todos; otherwise check whether the facet list has changed
        // (e.g. user added a facet file and ran /daemon-reload).
        DaemonEvent::SessionStateChanged { domains } => {
            if domains.iter().any(|d| d == "todos") {
                debug!(domains = ?domains, "Session state changed (todos); will re-fetch /state");
                EventTranslation::TodoStateChange
            } else {
                debug!(domains = ?domains, "Session state changed (non-todos); will check facets");
                EventTranslation::FacetChoicesCheck
            }
        }
        DaemonEvent::TodoStatusNudge => {
            debug!("Todo status nudge; will re-fetch /state");
            EventTranslation::TodoStateChange
        }

        // Goal driver update → ACP Plan
        DaemonEvent::GoalDriverUpdate {
            transition,
            goal,
            proposed_summary,
        } => {
            let summary = goal
                .as_ref()
                .and_then(|g| g.get("summary"))
                .and_then(|s| s.as_str())
                .or(proposed_summary.as_deref())
                .unwrap_or("");
            if summary.is_empty() && transition != "cleared" {
                debug!(transition = %transition, "Goal driver update with no summary; ignoring");
                EventTranslation::Ignore
            } else {
                debug!(transition = %transition, summary = %summary, "Goal driver update");
                EventTranslation::GoalDriverUpdate {
                    transition: transition.clone(),
                    summary: summary.to_string(),
                }
            }
        }

        // System reminder → ext notification
        DaemonEvent::SystemReminder {
            slug,
            display_name,
            body,
            reason,
        } => {
            debug!(slug = %slug, "System reminder");
            EventTranslation::SystemReminder {
                slug: slug.clone(),
                display_name: display_name.clone(),
                body: body.clone(),
                reason: reason.clone(),
            }
        }

        // Usage throttle → logged but not forwarded (context_pressure handles mid-turn usage)
        DaemonEvent::UsageThrottle { provider, .. } => {
            debug!(provider = %provider, "Usage throttle (logged, not forwarded)");
            EventTranslation::Ignore
        }

        // Permission monitor switched → re-fetch and send ConfigOptionUpdate
        DaemonEvent::PermissionMonitorSwitch { to_monitor, .. } => {
            debug!(mode = %to_monitor.kind, "Permission monitor switched");
            EventTranslation::PermissionMonitorSwitch {
                mode: to_monitor.kind.clone(),
            }
        }

        _ => EventTranslation::Ignore,
    }
}

/// Translate daemon job events. All 5 job event types map to a single
/// `JobEvent` translation carrying the event type name, so the handler in
/// agent.rs can decide what to do based on the event type.
///
/// Deduplication: when `subagent_handle` is `Some`, the subagent lifecycle
/// events (SubagentStarted/SubagentCompleted) already emit ToolCalls and
/// extension notifications. The job event for that same subagent is
/// translated to `Ignore` to avoid duplicates.
fn translate_job_event(
    job_id: &str,
    event_type: &str,
    subagent_handle: &Option<String>,
    exit_code: Option<i32>,
) -> EventTranslation {
    let _ = (job_id, event_type); // used by the caller for logging
    if subagent_handle.is_some() {
        // Subagent jobs are already handled by SubagentStarted/SubagentCompleted.
        EventTranslation::Ignore
    } else {
        // Shell job — emit extension notification.
        EventTranslation::JobEvent {
            job_id: job_id.to_string(),
            event_type: event_type.to_string(),
            subagent_handle: subagent_handle.clone(),
            exit_code,
        }
    }
}

/// Map a daemon tool name to an ACP `ToolKind`.
///
/// This lets the ACP client (Paseo) choose the right UI treatment —
/// read view for file reads, diff view for edits, shell view for commands,
/// search results for grep/glob, etc.
pub(crate) fn tool_kind_for_name(name: &str) -> acp::ToolKind {
    match name {
        // File reading
        "file_read" | "file_read_hashline" => acp::ToolKind::Read,

        // File modification
        "file_edit_search_replace" | "file_edit_hashline" | "patch_edit" | "file_write" => {
            acp::ToolKind::Edit
        }

        // Searching
        "glob" | "grep" => acp::ToolKind::Search,

        // Shell execution
        "shell_exec" | "shell_monitor" | "pushd" | "popd" => acp::ToolKind::Execute,

        // Job management (subprocess lifecycle)
        "job_status" | "job_block" | "job_result" | "job_cancel" => acp::ToolKind::Execute,

        // Web fetching
        "web_fetch" | "web_search" => acp::ToolKind::Fetch,

        // MCP resource reading
        "mcp_list_resources" | "mcp_read_resource" => acp::ToolKind::Read,

        // Planning / goal management
        "write_plan" | "edit_plan" | "handoff_plan" | "propose_goal" | "read_goal"
        | "complete_goal" | "block_goal" => acp::ToolKind::Think,

        // Todo management (internal reasoning)
        "todo_create" | "todo_update" | "todo_complete" | "todo_delete" | "todo_list" => {
            acp::ToolKind::Think
        }

        // Facet switching (no longer SwitchMode — mode = permissions now)
        "switch_facet" => acp::ToolKind::Other,

        // Tool search / interaction / delegation
        "tool_search" | "ask_user_question" | "subagent" | "skill" | "flag_important" => {
            acp::ToolKind::Other
        }

        _ => acp::ToolKind::Other,
    }
}

/// Decide whether a tool call's `raw_input` should be suppressed in favor of
/// a clean human-readable title.
///
/// Some tools carry very large JSON inputs that render as noise in the
/// transcript. `ask_user_question` is the primary case: its full questions
/// payload (context, options, justifications) is already delivered to the
/// client via the `_polytoken/ask_user_question` extension request, so the
/// ToolCall's `raw_input` is redundant. Returning `Some(title)` suppresses
/// `raw_input` and sets a concise title instead; returning `None` preserves
/// the default behaviour (set `raw_input`, extract locations).
pub(crate) fn tool_call_title_override(name: &str, input: &serde_json::Value) -> Option<String> {
    if name == "ask_user_question" {
        let count = input
            .get("questions")
            .and_then(|q| q.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        Some(if count == 1 {
            "Asking 1 question".to_string()
        } else {
            format!("Asking {} questions", count)
        })
    } else {
        None
    }
}

/// Extract file locations from a tool call's input JSON.
///
/// Looks for common path fields (`path`, `filePath`, `file`, `old_path`,
/// `new_path`) and returns `ToolCallLocation` entries for each found path.
pub(crate) fn extract_locations(input: &serde_json::Value) -> Option<Vec<acp::ToolCallLocation>> {
    let obj = input.as_object()?;
    let mut locations = Vec::new();

    // Primary path fields
    for key in &["path", "filePath", "file"] {
        if let Some(path_str) = obj.get(*key).and_then(|v| v.as_str()) {
            let mut loc = acp::ToolCallLocation::new(path_str.to_string());
            if let Some(line) = obj
                .get("line")
                .or_else(|| obj.get("offset"))
                .and_then(|v| v.as_u64())
            {
                loc = loc.line(line as u32);
            }
            locations.push(loc);
        }
    }

    // Edit tools may have old_path / new_path
    for key in &["old_path", "new_path"] {
        if let Some(path_str) = obj.get(*key).and_then(|v| v.as_str()) {
            locations.push(acp::ToolCallLocation::new(path_str.to_string()));
        }
    }

    if locations.is_empty() {
        None
    } else {
        Some(locations)
    }
}

/// Build an ACP Plan from the daemon's `/state` todos.
///
/// Maps each todo to a `PlanEntry` with:
/// - `PlanEntryState::Completed` for `done` todos
/// - `PlanEntryState::Pending` for everything else
/// - Priority `High` for `blocked`/`in_progress`, `Medium` otherwise
pub fn build_plan_from_state(state: &serde_json::Value) -> Option<acp::Plan> {
    let todos = state.get("todos")?.as_array()?;
    if todos.is_empty() {
        return None;
    }

    let total = todos.len();
    let entries: Vec<acp::PlanEntry> = todos
        .iter()
        .enumerate()
        .map(|(i, todo)| {
            let title = todo
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("(untitled)");
            let status = todo
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("pending");
            let plan_status = if status == "done" {
                acp::PlanEntryStatus::Completed
            } else if status == "in_progress" {
                acp::PlanEntryStatus::InProgress
            } else {
                acp::PlanEntryStatus::Pending
            };
            let priority = if status == "blocked" || status == "in_progress" {
                acp::PlanEntryPriority::High
            } else {
                acp::PlanEntryPriority::Medium
            };
            // When there are multiple tasks, append a position/count suffix
            // (e.g. "Investigate the bug (1 of 4)") so the collapsed view in
            // clients like Paseo — which shows the first incomplete entry's
            // content as a secondary label — clearly indicates there are more
            // items to expand.
            let content = if total > 1 {
                format!("{} ({} of {})", title, i + 1, total)
            } else {
                title.to_string()
            };
            acp::PlanEntry::new(content, priority, plan_status)
        })
        .collect();

    Some(acp::Plan::new(entries))
}

/// Build the ACP permission options for a permission request.
pub fn build_permission_options() -> Vec<acp::PermissionOption> {
    vec![
        acp::PermissionOption::new("allow_once", "Allow", acp::PermissionOptionKind::AllowOnce),
        acp::PermissionOption::new(
            "reject_once",
            "Reject",
            acp::PermissionOptionKind::RejectOnce,
        ),
    ]
}

/// Decision option IDs for a plan-handoff, matching the daemon's
/// `PlanHandoffDecision` variants (the value of `plan_handoff_answer.decision`).
pub const PLAN_HANDOFF_NEW_CONTEXT: &str = "implement_new_context";
pub const PLAN_HANDOFF_CURRENT_CONTEXT: &str = "implement_current_context";
pub const PLAN_HANDOFF_REFUSE: &str = "refuse";
/// Not a `PlanHandoffDecision`; maps to the sibling `{"kind":"cancel"}` response.
pub const PLAN_HANDOFF_CANCEL: &str = "cancel";
/// The synthesized question ID used when forwarding a plan-handoff as an
/// `ask_user_question` (single-select). Answers echo this back.
pub const PLAN_HANDOFF_QUESTION_ID: &str = "plan_handoff";

/// Build a single-select `ask_user_question` payload from a plan-handoff
/// interrogative. The plan text becomes the question `context` so the client
/// can render the same review surface the TUI shows; each action label becomes
/// a selectable option whose ID is the daemon decision string.
pub fn build_plan_handoff_payload(
    question: &str,
    context: &PlanHandoffContext,
) -> AskUserQuestionPayload {
    let labels = &context.action_labels;
    let mut options = vec![
        AskUserQuestionOption {
            id: PLAN_HANDOFF_NEW_CONTEXT.to_string(),
            label: labels.implement_new_context.clone(),
            description: "Hand the plan off to a fresh execution context.".to_string(),
            justification: None,
            preview: None,
        },
        AskUserQuestionOption {
            id: PLAN_HANDOFF_CURRENT_CONTEXT.to_string(),
            label: labels.implement_current_context.clone(),
            description: "Continue implementing in the current context.".to_string(),
            justification: None,
            preview: None,
        },
    ];
    // `refuse` is optional: the daemon leaves the label empty when unavailable.
    if !labels.refuse.is_empty() {
        options.push(AskUserQuestionOption {
            id: PLAN_HANDOFF_REFUSE.to_string(),
            label: labels.refuse.clone(),
            description: "Send the plan back with feedback instead of implementing it.".to_string(),
            justification: None,
            preview: None,
        });
    }
    options.push(AskUserQuestionOption {
        id: PLAN_HANDOFF_CANCEL.to_string(),
        label: labels.cancel.clone(),
        description: "Dismiss the handoff without a decision.".to_string(),
        justification: None,
        preview: None,
    });

    // Surface the plan text (and its display path) as review context.
    let context_md = format!(
        "**{}**\n\n`{}`\n\n{}",
        context.title, context.display_path, context.plan_text
    );

    AskUserQuestionPayload {
        questions: vec![AskUserQuestionItem {
            id: PLAN_HANDOFF_QUESTION_ID.to_string(),
            context: Some(context_md),
            question: question.to_string(),
            mode: AskUserQuestionMode::SingleSelect,
            // Free text is the `refuse` feedback channel.
            allow_free_text: true,
            options,
        }],
    }
}

/// Map a selected plan-handoff option ID (and any free-text feedback) to the
/// daemon response body posted to `/interrogative/{id}/respond`.
///
/// - `implement_new_context` / `implement_current_context` →
///   `{"kind":"plan_handoff_answer","decision":<id>}`
/// - `refuse` → `{"kind":"plan_handoff_answer","decision":"refuse","feedback":<text>}`
/// - `cancel`, `None`, or an unknown ID → `{"kind":"cancel"}` (safe default so
///   the agent can proceed rather than hang)
pub fn build_plan_handoff_response(
    selected_option_id: Option<&str>,
    free_text: Option<&str>,
) -> serde_json::Value {
    match selected_option_id {
        Some(PLAN_HANDOFF_NEW_CONTEXT) => serde_json::json!({
            "kind": "plan_handoff_answer",
            "decision": PLAN_HANDOFF_NEW_CONTEXT,
        }),
        Some(PLAN_HANDOFF_CURRENT_CONTEXT) => serde_json::json!({
            "kind": "plan_handoff_answer",
            "decision": PLAN_HANDOFF_CURRENT_CONTEXT,
        }),
        Some(PLAN_HANDOFF_REFUSE) => serde_json::json!({
            "kind": "plan_handoff_answer",
            "decision": PLAN_HANDOFF_REFUSE,
            "feedback": free_text.unwrap_or(""),
        }),
        _ => serde_json::json!({ "kind": "cancel" }),
    }
}

// ---------------------------------------------------------------------------
// Goal proposal payload + response builders
// ---------------------------------------------------------------------------

/// Option ID for accepting a goal proposal.
pub const GOAL_PROPOSAL_ACCEPT: &str = "accept";
/// Option ID for rejecting a goal proposal.
pub const GOAL_PROPOSAL_REJECT: &str = "reject";
/// Synthesized question ID used when forwarding a goal proposal as an
/// `ask_user_question` (single-select). Answers echo this back.
pub const GOAL_PROPOSAL_QUESTION_ID: &str = "goal_proposal";

/// Build a single-select `ask_user_question` payload from a goal-proposal
/// interrogative. The proposed summary becomes the question `context` so the
/// client can render the proposal text; accept/reject labels become selectable
/// options whose IDs map to the daemon's boolean `accepted` field.
pub fn build_goal_proposal_payload(
    question: &str,
    context: &GoalProposalContext,
) -> AskUserQuestionPayload {
    let labels = &context.action_labels;
    let options = vec![
        AskUserQuestionOption {
            id: GOAL_PROPOSAL_ACCEPT.to_string(),
            label: labels.accept.clone(),
            description: "Accept the proposed goal.".to_string(),
            justification: None,
            preview: None,
        },
        AskUserQuestionOption {
            id: GOAL_PROPOSAL_REJECT.to_string(),
            label: labels.reject.clone(),
            description: "Reject the proposed goal.".to_string(),
            justification: None,
            preview: None,
        },
    ];

    // Surface the proposed summary (and optional file path) as review context.
    let mut context_md = format!("**{}**\n\n{}", context.title, context.proposed_summary);
    if let Some(path) = &context.proposed_file_path {
        context_md.push_str(&format!("\n\n`{}`", path));
    }

    AskUserQuestionPayload {
        questions: vec![AskUserQuestionItem {
            id: GOAL_PROPOSAL_QUESTION_ID.to_string(),
            context: Some(context_md),
            question: question.to_string(),
            mode: AskUserQuestionMode::SingleSelect,
            allow_free_text: false,
            options,
        }],
    }
}

/// Map a selected goal-proposal option ID to the daemon response body posted
/// to `/interrogative/{id}/respond`.
///
/// - `accept` → `{"kind":"goal_proposal_answer","accepted":true}`
/// - `reject`, `None`, or unknown → `{"kind":"goal_proposal_answer","accepted":false}`
pub fn build_goal_proposal_response(selected_option_id: Option<&str>) -> serde_json::Value {
    let accepted = matches!(selected_option_id, Some(GOAL_PROPOSAL_ACCEPT));
    serde_json::json!({
        "kind": "goal_proposal_answer",
        "accepted": accepted,
    })
}

/// Resolve a permission outcome to a boolean granted value.
/// Returns (granted, option_id_was_allow) for testability.
pub fn resolve_permission_outcome(outcome: &acp::RequestPermissionOutcome) -> bool {
    match outcome {
        acp::RequestPermissionOutcome::Cancelled => false,
        acp::RequestPermissionOutcome::Selected(selected) => {
            // Check if the selected option is an allow option
            // The option_ids we use are "allow_once" and "reject_once"
            // option_id is a PermissionOptionId (Arc<str>)
            let id = selected.option_id.0.as_ref();
            id == "allow_once" || id == "allow_always"
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Content block text extraction (from ACP prompt)
// ---------------------------------------------------------------------------

/// Extract joined text from ACP ContentBlock array, converting non-text blocks
/// to descriptive placeholders (since the daemon only accepts plain text).
///
/// - `Text` blocks are joined directly.
/// - `Image` blocks become `[image: <mime_type>, <size> bytes]`.
/// - `ResourceLink` blocks become `[resource: <uri>]` (or `[resource: <name>] (<uri>)`).
/// - `Resource` blocks become `[embedded resource: <mime_type>]`.
/// - `Audio` blocks become `[audio: <mime_type>, <size> bytes]`.
pub fn extract_text(blocks: &[acp::ContentBlock]) -> String {
    use tracing::warn;

    blocks
        .iter()
        .map(|block| match block {
            acp::ContentBlock::Text(text) => text.text.clone(),
            acp::ContentBlock::Image(img) => {
                let size = img.data.len();
                warn!(
                    mime_type = %img.mime_type,
                    bytes = size,
                    "Image content block in prompt — daemon only accepts text; converting to placeholder"
                );
                format!("[image: {}, {} bytes]", img.mime_type, size)
            }
            acp::ContentBlock::ResourceLink(link) => {
                warn!(
                    uri = %link.uri,
                    "ResourceLink content block in prompt — daemon only accepts text; converting to placeholder"
                );
                match &link.title {
                    Some(title) => format!("[resource: {}] ({})", title, link.uri),
                    None => format!("[resource: {}]", link.uri),
                }
            }
            acp::ContentBlock::Resource(resource) => {
                let mime = match &resource.resource {
                    acp::EmbeddedResourceResource::TextResourceContents(t) => {
                        t.mime_type.as_deref().unwrap_or("text/plain")
                    }
                    acp::EmbeddedResourceResource::BlobResourceContents(b) => {
                        b.mime_type.as_deref().unwrap_or("application/octet-stream")
                    }
                    _ => "unknown",
                };
                warn!(
                    mime_type = mime,
                    "Embedded resource in prompt — daemon only accepts text; converting to placeholder"
                );
                format!("[embedded resource: {}]", mime)
            }
            acp::ContentBlock::Audio(audio) => {
                let size = audio.data.len();
                warn!(
                    mime_type = %audio.mime_type,
                    bytes = size,
                    "Audio content block in prompt — daemon only accepts text; converting to placeholder"
                );
                format!("[audio: {}, {} bytes]", audio.mime_type, size)
            }
            _ => {
                warn!("Unknown content block type in prompt — dropping");
                String::new()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_text_delta() {
        let json = r#"{"type":"content_block_delta","prompt_id":"abc","block_index":0,"delta":{"type":"text","text":"hello "}}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        match evt {
            DaemonEvent::ContentBlockDelta {
                delta, prompt_id, ..
            } => {
                assert_eq!(prompt_id, "abc");
                match delta {
                    BlockDeltaPayload::TextDelta { text } => assert_eq!(text, "hello "),
                    _ => panic!("Expected TextDelta"),
                }
            }
            _ => panic!("Expected ContentBlockDelta"),
        }
    }

    #[test]
    fn test_deserialize_message_complete() {
        let json = r#"{"type":"message_complete","prompt_id":"abc"}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        assert!(matches!(evt, DaemonEvent::MessageComplete { .. }));
    }

    #[test]
    fn test_deserialize_turn_cancelled() {
        let json = r#"{"type":"turn_cancelled","prompt_id":"abc","reason":"user_cancelled"}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        assert!(matches!(evt, DaemonEvent::TurnCancelled { .. }));
    }

    #[test]
    fn test_deserialize_tool_call() {
        let json = r#"{"type":"tool_call","prompt_id":"abc","call_id":"call_1","name":"read_file","input":{"path":"/tmp/test"}}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        match evt {
            DaemonEvent::ToolCall { call_id, name, .. } => {
                assert_eq!(call_id, "call_1");
                assert_eq!(name, "read_file");
            }
            _ => panic!("Expected ToolCall"),
        }
    }

    #[test]
    fn test_deserialize_tool_call_no_input() {
        let json =
            r#"{"type":"tool_call","prompt_id":"abc","call_id":"call_1","name":"read_file"}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        assert!(matches!(evt, DaemonEvent::ToolCall { .. }));
    }

    #[test]
    fn test_deserialize_tool_result() {
        let json = r#"{"type":"tool_result","prompt_id":"abc","call_id":"call_1","content":"file contents","is_error":false}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        assert!(matches!(evt, DaemonEvent::ToolResult { .. }));
    }

    #[test]
    fn test_deserialize_interrogative() {
        let json = r#"{"type":"interrogative","prompt_id":"abc","interrogative_id":"int_1","question":"Allow read?","interrogative_type":"permission"}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        match evt {
            DaemonEvent::Interrogative {
                interrogative_id,
                question,
                ..
            } => {
                assert_eq!(interrogative_id, "int_1");
                assert_eq!(question, "Allow read?");
            }
            _ => panic!("Expected Interrogative"),
        }
    }

    #[test]
    fn test_deserialize_model_error() {
        let json = r#"{"type":"model_error","prompt_id":"abc","error":{"message":"rate limited"}}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        assert!(matches!(evt, DaemonEvent::ModelError { .. }));
    }

    #[test]
    fn test_deserialize_heartbeat() {
        let json = r#"{"type":"heartbeat","timestamp":"2025-01-01T00:00:00Z"}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        assert!(matches!(evt, DaemonEvent::Heartbeat));
    }

    #[test]
    fn test_deserialize_unknown_event() {
        let json = r#"{"type":"some_future_event","prompt_id":"abc"}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        assert!(matches!(evt, DaemonEvent::Other));
    }

    #[test]
    fn test_deserialize_content_block_start() {
        let json = r#"{"type":"content_block_start","prompt_id":"abc","block_index":0,"block_type":{"type":"text"}}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        assert!(matches!(evt, DaemonEvent::ContentBlockStart { .. }));
    }

    #[test]
    fn test_deserialize_content_block_stop() {
        let json = r#"{"type":"content_block_stop","prompt_id":"abc","block_index":0}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        assert!(matches!(evt, DaemonEvent::ContentBlockStop { .. }));
    }

    #[test]
    fn test_translate_text_delta() {
        let evt = DaemonEvent::ContentBlockDelta {
            prompt_id: "abc".into(),
            block_index: 0,
            delta: BlockDeltaPayload::TextDelta {
                text: "world".into(),
            },
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::AgentMessageChunk(_)) => {}
            _ => panic!("Expected AgentMessageChunk"),
        }
    }

    #[test]
    fn test_translate_tool_call() {
        let evt = DaemonEvent::ToolCall {
            prompt_id: "abc".into(),
            call_id: "c1".into(),
            name: "read_file".into(),
            input: None,
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::ToolCall(tc)) => {
                assert_eq!(tc.title, "read_file");
            }
            _ => panic!("Expected ToolCall update"),
        }
    }

    #[test]
    fn test_translate_tool_call_ask_user_question_suppresses_raw_input() {
        let evt = DaemonEvent::ToolCall {
            prompt_id: "abc".into(),
            call_id: "c1".into(),
            name: "ask_user_question".into(),
            input: Some(serde_json::json!({
                "questions": [
                    {"id": "q1", "question": "Pick one", "mode": "single_select", "options": []},
                    {"id": "q2", "question": "Name it", "mode": "text", "options": []},
                ]
            })),
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::ToolCall(tc)) => {
                // Title should be a clean summary, not the tool name
                assert_eq!(tc.title, "Asking 2 questions");
                // raw_input should be None (suppressed)
                assert!(
                    tc.raw_input.is_none(),
                    "raw_input should be suppressed for ask_user_question, got: {:?}",
                    tc.raw_input
                );
            }
            _ => panic!("Expected ToolCall update"),
        }
    }

    #[test]
    fn test_translate_tool_call_other_tools_keep_raw_input() {
        let evt = DaemonEvent::ToolCall {
            prompt_id: "abc".into(),
            call_id: "c1".into(),
            name: "file_read".into(),
            input: Some(serde_json::json!({"path": "/tmp/test.md"})),
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::ToolCall(tc)) => {
                assert_eq!(tc.title, "file_read");
                assert!(tc.raw_input.is_some());
            }
            _ => panic!("Expected ToolCall update"),
        }
    }

    #[test]
    fn test_translate_tool_result() {
        let evt = DaemonEvent::ToolResult {
            prompt_id: "abc".into(),
            call_id: "c1".into(),
            content: Some("result".into()),
            content_full: None,
            is_error: Some(false),
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::ToolCallUpdate(u)) => {
                assert_eq!(u.fields.status, Some(acp::ToolCallStatus::Completed));
            }
            _ => panic!("Expected ToolCallUpdate"),
        }
    }

    #[test]
    fn test_translate_tool_result_error() {
        let evt = DaemonEvent::ToolResult {
            prompt_id: "abc".into(),
            call_id: "c1".into(),
            content: None,
            content_full: None,
            is_error: Some(true),
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::ToolCallUpdate(u)) => {
                assert_eq!(u.fields.status, Some(acp::ToolCallStatus::Failed));
            }
            _ => panic!("Expected ToolCallUpdate with Failed status"),
        }
    }

    #[test]
    fn test_translate_message_complete() {
        let evt = DaemonEvent::MessageComplete {
            prompt_id: "abc".into(),
        };
        assert!(matches!(translate_event(&evt), EventTranslation::TurnEnd));
    }

    #[test]
    fn test_translate_turn_cancelled() {
        let evt = DaemonEvent::TurnCancelled {
            prompt_id: "abc".into(),
        };
        assert!(matches!(
            translate_event(&evt),
            EventTranslation::TurnCancelled
        ));
    }

    #[test]
    fn test_translate_interrogative() {
        let evt = DaemonEvent::Interrogative {
            prompt_id: "abc".into(),
            interrogative_id: "int_1".into(),
            question: "Allow?".into(),
            interrogative_type: "permission".into(),
            plan_handoff: None,
            goal_proposal: None,
        };
        match translate_event(&evt) {
            EventTranslation::PermissionRequest {
                interrogative_id,
                question,
            } => {
                assert_eq!(interrogative_id, "int_1");
                assert_eq!(question, "Allow?");
            }
            _ => panic!("Expected PermissionRequest"),
        }
    }

    // Permission translation tests (AC.6a)

    #[test]
    fn test_permission_options_have_allow_and_reject() {
        let options = build_permission_options();
        assert_eq!(options.len(), 2);
        // First is AllowOnce, second is RejectOnce
        assert_eq!(options[0].kind, acp::PermissionOptionKind::AllowOnce);
        assert_eq!(options[1].kind, acp::PermissionOptionKind::RejectOnce);
    }

    #[test]
    fn test_resolve_outcome_selected_allow() {
        let outcome = acp::RequestPermissionOutcome::Selected(acp::SelectedPermissionOutcome::new(
            "allow_once",
        ));
        assert!(resolve_permission_outcome(&outcome));
    }

    #[test]
    fn test_resolve_outcome_selected_reject() {
        let outcome = acp::RequestPermissionOutcome::Selected(acp::SelectedPermissionOutcome::new(
            "reject_once",
        ));
        assert!(!resolve_permission_outcome(&outcome));
    }

    #[test]
    fn test_resolve_outcome_cancelled() {
        let outcome = acp::RequestPermissionOutcome::Cancelled;
        assert!(!resolve_permission_outcome(&outcome));
    }

    #[test]
    fn test_extract_text() {
        let blocks = vec![
            acp::ContentBlock::Text(acp::TextContent::new("Hello ")),
            acp::ContentBlock::Text(acp::TextContent::new("World")),
        ];
        assert_eq!(extract_text(&blocks), "Hello \nWorld");
    }

    #[test]
    fn test_extract_text_empty() {
        let blocks: Vec<acp::ContentBlock> = vec![];
        assert_eq!(extract_text(&blocks), "");
    }

    #[test]
    fn test_extract_text_with_image() {
        let blocks = vec![
            acp::ContentBlock::Text(acp::TextContent::new("Look at this: ")),
            acp::ContentBlock::Image(acp::ImageContent::new("iVBORw...", "image/png")),
        ];
        let result = extract_text(&blocks);
        assert!(result.contains("Look at this:"));
        assert!(result.contains("[image: image/png,"));
    }

    #[test]
    fn test_extract_text_with_resource_link() {
        let blocks = vec![
            acp::ContentBlock::Text(acp::TextContent::new("See file: ")),
            acp::ContentBlock::ResourceLink(acp::ResourceLink::new(
                "main.rs",
                "file:///src/main.rs",
            )),
        ];
        let result = extract_text(&blocks);
        assert!(result.contains("See file:"));
        assert!(result.contains("[resource: file:///src/main.rs]"));
    }

    #[test]
    fn test_extract_text_with_resource_link_title() {
        let link = acp::ResourceLink::new("main.rs", "file:///src/main.rs").title("Main Source");
        let blocks = vec![acp::ContentBlock::ResourceLink(link)];
        let result = extract_text(&blocks);
        assert!(result.contains("[resource: Main Source] (file:///src/main.rs)"));
    }

    // ask_user_question tests

    #[test]
    fn test_deserialize_ask_user_question() {
        let json = r#"{
            "type": "ask_user_question",
            "prompt_id": "abc",
            "interrogative_id": "int_1",
            "payload": {
                "questions": [
                    {
                        "id": "q1",
                        "context": "Choose an approach",
                        "question": "Which approach?",
                        "mode": "single_select",
                        "allow_free_text": true,
                        "options": [
                            {"id": "a", "label": "Option A", "description": "Do A"},
                            {"id": "b", "label": "Option B", "description": "Do B", "justification": "faster", "preview": "diff here"}
                        ]
                    },
                    {
                        "id": "q2",
                        "question": "Any notes?",
                        "mode": "text"
                    }
                ]
            }
        }"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        match evt {
            DaemonEvent::AskUserQuestion {
                prompt_id,
                interrogative_id,
                payload,
            } => {
                assert_eq!(prompt_id, "abc");
                assert_eq!(interrogative_id, "int_1");
                assert_eq!(payload.questions.len(), 2);

                let q1 = &payload.questions[0];
                assert_eq!(q1.id, "q1");
                assert_eq!(q1.context.as_deref(), Some("Choose an approach"));
                assert_eq!(q1.question, "Which approach?");
                assert_eq!(q1.mode, AskUserQuestionMode::SingleSelect);
                assert!(q1.allow_free_text);
                assert_eq!(q1.options.len(), 2);
                assert_eq!(q1.options[0].id, "a");
                assert_eq!(q1.options[1].justification.as_deref(), Some("faster"));
                assert_eq!(q1.options[1].preview.as_deref(), Some("diff here"));

                let q2 = &payload.questions[1];
                assert_eq!(q2.mode, AskUserQuestionMode::Text);
                // allow_free_text defaults to true
                assert!(q2.allow_free_text);
                assert!(q2.options.is_empty());
                assert!(q2.context.is_none());
            }
            _ => panic!("Expected AskUserQuestion"),
        }
    }

    #[test]
    fn test_deserialize_ask_user_question_multi_select() {
        let json = r#"{
            "type": "ask_user_question",
            "prompt_id": "abc",
            "interrogative_id": "int_1",
            "payload": {
                "questions": [
                    {
                        "id": "q1",
                        "question": "Select all that apply",
                        "mode": "multi_select",
                        "options": []
                    }
                ]
            }
        }"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        match evt {
            DaemonEvent::AskUserQuestion { payload, .. } => {
                assert_eq!(payload.questions[0].mode, AskUserQuestionMode::MultiSelect);
            }
            _ => panic!("Expected AskUserQuestion"),
        }
    }

    #[test]
    fn test_translate_ask_user_question() {
        let evt = DaemonEvent::AskUserQuestion {
            prompt_id: "abc".into(),
            interrogative_id: "int_1".into(),
            payload: AskUserQuestionPayload {
                questions: vec![AskUserQuestionItem {
                    id: "q1".into(),
                    context: None,
                    question: "Which?".into(),
                    mode: AskUserQuestionMode::SingleSelect,
                    allow_free_text: true,
                    options: vec![AskUserQuestionOption {
                        id: "a".into(),
                        label: "A".into(),
                        description: "Do A".into(),
                        justification: None,
                        preview: None,
                    }],
                }],
            },
        };
        match translate_event(&evt) {
            EventTranslation::AskUserQuestion {
                interrogative_id,
                payload,
            } => {
                assert_eq!(interrogative_id, "int_1");
                assert_eq!(payload.questions.len(), 1);
                assert_eq!(payload.questions[0].id, "q1");
            }
            _ => panic!("Expected AskUserQuestion translation"),
        }
    }

    #[test]
    fn test_ask_user_question_serializes_for_ext_method() {
        // Verify that the payload round-trips through Serialize → json → Deserialize
        let payload = AskUserQuestionPayload {
            questions: vec![AskUserQuestionItem {
                id: "q1".into(),
                context: Some("ctx".into()),
                question: "Which?".into(),
                mode: AskUserQuestionMode::SingleSelect,
                allow_free_text: false,
                options: vec![AskUserQuestionOption {
                    id: "a".into(),
                    label: "A".into(),
                    description: "desc".into(),
                    justification: Some("because".into()),
                    preview: Some("preview".into()),
                }],
            }],
        };
        let json_val = serde_json::to_value(&payload).unwrap();
        let back: AskUserQuestionPayload = serde_json::from_value(json_val).unwrap();
        assert_eq!(back.questions.len(), 1);
        assert_eq!(back.questions[0].id, "q1");
        assert!(!back.questions[0].allow_free_text);
        assert_eq!(
            back.questions[0].options[0].justification.as_deref(),
            Some("because")
        );
    }

    #[test]
    fn test_deserialize_message_start() {
        let json = r#"{"type":"message_start","prompt_id":"abc"}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        assert!(matches!(evt, DaemonEvent::MessageStart { .. }));
    }

    #[test]
    fn test_translate_thinking_delta() {
        let evt = DaemonEvent::ContentBlockDelta {
            prompt_id: "abc".into(),
            block_index: 0,
            delta: BlockDeltaPayload::Thinking {
                text: "I should check...".into(),
            },
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::AgentThoughtChunk(_)) => {}
            _ => panic!("Expected AgentThoughtChunk"),
        }
    }

    #[test]
    fn test_translate_redacted_thinking_delta() {
        let evt = DaemonEvent::ContentBlockDelta {
            prompt_id: "abc".into(),
            block_index: 0,
            delta: BlockDeltaPayload::RedactedThinking {
                data: "opaque".into(),
            },
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::AgentThoughtChunk(_)) => {}
            _ => panic!("Expected AgentThoughtChunk for redacted thinking"),
        }
    }

    #[test]
    fn test_translate_openai_reasoning_delta() {
        let evt = DaemonEvent::ContentBlockDelta {
            prompt_id: "abc".into(),
            block_index: 0,
            delta: BlockDeltaPayload::OpenAiReasoning {
                id: "r1".into(),
                data: "reasoning text".into(),
            },
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::AgentThoughtChunk(_)) => {}
            _ => panic!("Expected AgentThoughtChunk for OpenAI reasoning"),
        }
    }

    #[test]
    fn test_deserialize_session_title_changed() {
        let json = r#"{"type":"session_title_changed","title":"New Title","source":"inferred"}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        match evt {
            DaemonEvent::SessionTitleChanged { title } => {
                assert_eq!(title, "New Title");
            }
            _ => panic!("Expected SessionTitleChanged"),
        }
    }

    #[test]
    fn test_translate_session_title_changed() {
        let evt = DaemonEvent::SessionTitleChanged {
            title: "Updated Title".into(),
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::SessionInfoUpdate(u)) => {
                assert_eq!(
                    u.title.as_opt_ref().unwrap(),
                    Some(&"Updated Title".to_string())
                );
            }
            _ => panic!("Expected SessionInfoUpdate"),
        }
    }

    #[test]
    fn test_deserialize_model_changed() {
        let json = r#"{"type":"model_changed","model":"gpt-4o"}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        assert!(matches!(evt, DaemonEvent::ModelChanged { .. }));
    }

    #[test]
    fn test_deserialize_facet_changed() {
        let json = r#"{"type":"facet_changed","facet":"plan"}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        match evt {
            DaemonEvent::FacetChanged { facet } => {
                assert_eq!(facet, "plan");
            }
            _ => panic!("Expected FacetChanged"),
        }
    }

    #[test]
    fn test_translate_facet_changed() {
        // Facet changes are no longer forwarded as CurrentModeUpdate.
        // Mode now maps to the permission monitor, not facets.
        let evt = DaemonEvent::FacetChanged {
            facet: "plan".into(),
        };
        match translate_event(&evt) {
            EventTranslation::Ignore => {}
            _ => panic!("Expected Ignore for FacetChanged"),
        }
    }

    #[test]
    fn test_translate_interrogative_confirmation() {
        let evt = DaemonEvent::Interrogative {
            prompt_id: "abc".into(),
            interrogative_id: "int_1".into(),
            question: "Are you sure?".into(),
            interrogative_type: "confirmation".into(),
            plan_handoff: None,
            goal_proposal: None,
        };
        match translate_event(&evt) {
            EventTranslation::InterrogativeRequest {
                interrogative_type, ..
            } => {
                assert_eq!(interrogative_type, "confirmation");
            }
            _ => panic!("Expected InterrogativeRequest"),
        }
    }

    #[test]
    fn test_translate_tool_result_content_full() {
        let evt = DaemonEvent::ToolResult {
            prompt_id: "abc".into(),
            call_id: "c1".into(),
            content: Some("truncated".into()),
            content_full: Some("full content here".into()),
            is_error: Some(false),
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::ToolCallUpdate(u)) => {
                assert_eq!(u.fields.status, Some(acp::ToolCallStatus::Completed));
            }
            _ => panic!("Expected ToolCallUpdate"),
        }
    }

    #[test]
    fn test_tool_kind_for_file_read() {
        let evt = DaemonEvent::ToolCall {
            prompt_id: "abc".into(),
            call_id: "c1".into(),
            name: "file_read".into(),
            input: Some(serde_json::json!({"path": "/tmp/test.rs", "line": 10})),
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::ToolCall(tc)) => {
                assert_eq!(tc.kind, acp::ToolKind::Read);
                assert_eq!(tc.locations.len(), 1);
                assert_eq!(
                    tc.locations[0].path,
                    std::path::PathBuf::from("/tmp/test.rs")
                );
                assert_eq!(tc.locations[0].line, Some(10));
            }
            _ => panic!("Expected ToolCall"),
        }
    }

    #[test]
    fn test_tool_kind_for_file_edit() {
        let evt = DaemonEvent::ToolCall {
            prompt_id: "abc".into(),
            call_id: "c2".into(),
            name: "file_edit_search_replace".into(),
            input: Some(serde_json::json!({"path": "/tmp/test.rs"})),
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::ToolCall(tc)) => {
                assert_eq!(tc.kind, acp::ToolKind::Edit);
            }
            _ => panic!("Expected ToolCall"),
        }
    }

    #[test]
    fn test_tool_kind_for_shell_exec() {
        let evt = DaemonEvent::ToolCall {
            prompt_id: "abc".into(),
            call_id: "c3".into(),
            name: "shell_exec".into(),
            input: Some(serde_json::json!({"command": "ls"})),
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::ToolCall(tc)) => {
                assert_eq!(tc.kind, acp::ToolKind::Execute);
                // No path fields → no locations
                assert!(tc.locations.is_empty());
            }
            _ => panic!("Expected ToolCall"),
        }
    }

    #[test]
    fn test_tool_kind_for_grep() {
        let evt = DaemonEvent::ToolCall {
            prompt_id: "abc".into(),
            call_id: "c4".into(),
            name: "grep".into(),
            input: None,
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::ToolCall(tc)) => {
                assert_eq!(tc.kind, acp::ToolKind::Search);
            }
            _ => panic!("Expected ToolCall"),
        }
    }

    #[test]
    fn test_tool_kind_for_web_fetch() {
        let evt = DaemonEvent::ToolCall {
            prompt_id: "abc".into(),
            call_id: "c5".into(),
            name: "web_fetch".into(),
            input: Some(serde_json::json!({"url": "https://example.com"})),
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::ToolCall(tc)) => {
                assert_eq!(tc.kind, acp::ToolKind::Fetch);
            }
            _ => panic!("Expected ToolCall"),
        }
    }

    #[test]
    fn test_tool_kind_for_switch_facet() {
        let evt = DaemonEvent::ToolCall {
            prompt_id: "abc".into(),
            call_id: "c6".into(),
            name: "switch_facet".into(),
            input: None,
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::ToolCall(tc)) => {
                // switch_facet is no longer SwitchMode — mode now maps to
                // the permission monitor. Facet switching is a slash command.
                assert_eq!(tc.kind, acp::ToolKind::Other);
            }
            _ => panic!("Expected ToolCall"),
        }
    }

    #[test]
    fn test_extract_locations_from_edit() {
        let input = serde_json::json!({
            "old_path": "/tmp/old.rs",
            "new_path": "/tmp/new.rs"
        });
        let evt = DaemonEvent::ToolCall {
            prompt_id: "abc".into(),
            call_id: "c7".into(),
            name: "patch_edit".into(),
            input: Some(input),
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::ToolCall(tc)) => {
                assert_eq!(tc.kind, acp::ToolKind::Edit);
                assert_eq!(tc.locations.len(), 2);
            }
            _ => panic!("Expected ToolCall"),
        }
    }

    #[test]
    fn test_deserialize_subagent_started() {
        let json = r#"{"type":"subagent_started","handle":"general-purpose:abc","subagent_type":"general-purpose","model":"zai/glm-5.2"}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        match evt {
            DaemonEvent::SubagentStarted {
                handle,
                subagent_type,
                model,
            } => {
                assert_eq!(handle, "general-purpose:abc");
                assert_eq!(subagent_type, "general-purpose");
                assert_eq!(model, "zai/glm-5.2");
            }
            _ => panic!("Expected SubagentStarted"),
        }
    }

    #[test]
    fn test_deserialize_subagent_completed() {
        let json = r#"{"type":"subagent_completed","handle":"general-purpose:abc","outcome":{"kind":"success","message":null},"result_summary":"Done"}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        match evt {
            DaemonEvent::SubagentCompleted {
                handle,
                outcome,
                result_summary,
            } => {
                assert_eq!(handle, "general-purpose:abc");
                assert_eq!(result_summary.as_deref(), Some("Done"));
                let outcome = outcome.expect("outcome should be present");
                assert_eq!(outcome.kind, SubagentResultKind::Success);
                assert!(outcome.message.is_none());
            }
            _ => panic!("Expected SubagentCompleted"),
        }
    }

    #[test]
    fn test_deserialize_subagent_completed_no_summary() {
        let json = r#"{"type":"subagent_completed","handle":"general-purpose:abc","outcome":{"kind":"cancelled","message":"user aborted"}}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        match evt {
            DaemonEvent::SubagentCompleted {
                handle,
                outcome,
                result_summary,
            } => {
                assert_eq!(handle, "general-purpose:abc");
                assert!(result_summary.is_none());
                let outcome = outcome.expect("outcome should be present");
                assert_eq!(outcome.kind, SubagentResultKind::Cancelled);
                assert_eq!(outcome.message.as_deref(), Some("user aborted"));
            }
            _ => panic!("Expected SubagentCompleted"),
        }
    }

    #[test]
    fn test_translate_subagent_started() {
        let evt = DaemonEvent::SubagentStarted {
            handle: "general-purpose:abc".into(),
            subagent_type: "general-purpose".into(),
            model: "zai/glm-5.2".into(),
        };
        match translate_event(&evt) {
            EventTranslation::SubagentStarted {
                handle,
                subagent_type,
                model,
            } => {
                assert_eq!(handle, "general-purpose:abc");
                assert_eq!(subagent_type, "general-purpose");
                assert_eq!(model, "zai/glm-5.2");
            }
            _ => panic!("Expected SubagentStarted translation"),
        }
    }

    #[test]
    fn test_translate_subagent_completed() {
        let evt = DaemonEvent::SubagentCompleted {
            handle: "general-purpose:abc".into(),
            outcome: Some(SubagentOutcome {
                kind: SubagentResultKind::Failure,
                message: Some("panicked".into()),
            }),
            result_summary: Some("All done".into()),
        };
        match translate_event(&evt) {
            EventTranslation::SubagentCompleted {
                handle,
                result_summary,
                outcome_kind,
                outcome_message,
            } => {
                assert_eq!(handle, "general-purpose:abc");
                assert_eq!(result_summary.as_deref(), Some("All done"));
                assert_eq!(outcome_kind, Some(SubagentResultKind::Failure));
                assert_eq!(outcome_message.as_deref(), Some("panicked"));
            }
            _ => panic!("Expected SubagentCompleted translation"),
        }
    }

    #[test]
    fn test_deserialize_subagent_completed_no_outcome() {
        // The daemon schema marks `outcome` as required, but we use
        // #[serde(default)] so that an older daemon (or a partially-formed
        // event) does not break deserialization.
        let json =
            r#"{"type":"subagent_completed","handle":"general-purpose:abc","result_summary":"ok"}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        match evt {
            DaemonEvent::SubagentCompleted {
                handle,
                outcome,
                result_summary,
            } => {
                assert_eq!(handle, "general-purpose:abc");
                assert!(outcome.is_none());
                assert_eq!(result_summary.as_deref(), Some("ok"));
            }
            _ => panic!("Expected SubagentCompleted"),
        }
    }

    #[test]
    fn test_deserialize_subagent_completed_outcome_all_kinds() {
        for (kind_str, expected) in [
            ("success", SubagentResultKind::Success),
            ("failure", SubagentResultKind::Failure),
            ("cancelled", SubagentResultKind::Cancelled),
        ] {
            let json = format!(
                r#"{{"type":"subagent_completed","handle":"h","outcome":{{"kind":"{}","message":"m"}}}}"#,
                kind_str
            );
            let evt: DaemonEvent = serde_json::from_str(&json).unwrap();
            match evt {
                DaemonEvent::SubagentCompleted { outcome, .. } => {
                    let outcome = outcome.expect("outcome should be present");
                    assert_eq!(outcome.kind, expected, "mismatch for kind={}", kind_str);
                    assert_eq!(outcome.message.as_deref(), Some("m"));
                }
                _ => panic!("Expected SubagentCompleted for kind={}", kind_str),
            }
        }
    }

    #[test]
    fn test_deserialize_job_promoted() {
        let json = r#"{"type":"job_promoted","job_id":"job_1","subagent_handle":null}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        match evt {
            DaemonEvent::JobPromoted {
                job_id,
                subagent_handle,
            } => {
                assert_eq!(job_id, "job_1");
                assert!(subagent_handle.is_none());
            }
            _ => panic!("Expected JobPromoted"),
        }
    }

    #[test]
    fn test_deserialize_job_completed() {
        let json =
            r#"{"type":"job_completed","job_id":"job_1","exit_code":0,"subagent_handle":null}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        match evt {
            DaemonEvent::JobCompleted {
                job_id,
                exit_code,
                subagent_handle,
            } => {
                assert_eq!(job_id, "job_1");
                assert_eq!(exit_code, 0);
                assert!(subagent_handle.is_none());
            }
            _ => panic!("Expected JobCompleted"),
        }
    }

    #[test]
    fn test_deserialize_job_cancelled() {
        let json = r#"{"type":"job_cancelled","job_id":"job_2","subagent_handle":"sa_1"}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        match evt {
            DaemonEvent::JobCancelled {
                job_id,
                subagent_handle,
            } => {
                assert_eq!(job_id, "job_2");
                assert_eq!(subagent_handle.as_deref(), Some("sa_1"));
            }
            _ => panic!("Expected JobCancelled"),
        }
    }

    #[test]
    fn test_translate_job_event_shell() {
        // Shell job (subagent_handle is None) → JobEvent
        let evt = DaemonEvent::JobCompleted {
            job_id: "job_1".into(),
            exit_code: 0,
            subagent_handle: None,
        };
        match translate_event(&evt) {
            EventTranslation::JobEvent {
                job_id,
                event_type,
                exit_code,
                ..
            } => {
                assert_eq!(job_id, "job_1");
                assert_eq!(event_type, "job_completed");
                assert_eq!(exit_code, Some(0));
            }
            _ => panic!("Expected JobEvent translation for shell job"),
        }
    }

    #[test]
    fn test_translate_job_event_subagent_skip() {
        // Subagent job (subagent_handle is Some) → Ignore (dedup)
        let evt = DaemonEvent::JobCompleted {
            job_id: "job_1".into(),
            exit_code: 0,
            subagent_handle: Some("sa_1".into()),
        };
        match translate_event(&evt) {
            EventTranslation::Ignore => {}
            _ => panic!("Expected Ignore for subagent job (dedup)"),
        }
    }

    #[test]
    fn test_translate_job_promoted_shell() {
        let evt = DaemonEvent::JobPromoted {
            job_id: "job_1".into(),
            subagent_handle: None,
        };
        match translate_event(&evt) {
            EventTranslation::JobEvent { event_type, .. } => {
                assert_eq!(event_type, "job_promoted");
            }
            _ => panic!("Expected JobEvent"),
        }
    }

    #[test]
    fn test_translate_job_expiring_subagent_skip() {
        let evt = DaemonEvent::JobExpiring {
            job_id: "job_1".into(),
            subagent_handle: Some("sa_1".into()),
        };
        match translate_event(&evt) {
            EventTranslation::Ignore => {}
            _ => panic!("Expected Ignore for subagent job (dedup)"),
        }
    }

    #[test]
    fn test_build_plan_from_state() {
        let state = serde_json::json!({
            "todos": [
                {"id": 1, "title": "Write code", "status": "in_progress", "dependencies": []},
                {"id": 2, "title": "Write tests", "status": "pending", "dependencies": [1]},
                {"id": 3, "title": "Review", "status": "done", "dependencies": [2]},
            ]
        });
        let plan = build_plan_from_state(&state).expect("plan should exist");
        assert_eq!(plan.entries.len(), 3);
        // Multiple tasks get a "(N of M)" suffix for collapsed-view clarity.
        assert_eq!(plan.entries[0].content, "Write code (1 of 3)");
        assert_eq!(plan.entries[1].content, "Write tests (2 of 3)");
        assert_eq!(plan.entries[2].content, "Review (3 of 3)");
        assert_eq!(plan.entries[0].status, acp::PlanEntryStatus::InProgress);
        assert_eq!(plan.entries[0].priority, acp::PlanEntryPriority::High);
        assert_eq!(plan.entries[1].status, acp::PlanEntryStatus::Pending);
        assert_eq!(plan.entries[2].status, acp::PlanEntryStatus::Completed);
    }

    #[test]
    fn test_build_plan_from_state_empty() {
        let state = serde_json::json!({"todos": []});
        assert!(build_plan_from_state(&state).is_none());
    }

    #[test]
    fn test_build_plan_from_state_single() {
        let state = serde_json::json!({
            "todos": [
                {"id": 1, "title": "Only task", "status": "pending"},
            ]
        });
        let plan = build_plan_from_state(&state).expect("plan should exist");
        assert_eq!(plan.entries.len(), 1);
        // Single task — no count suffix.
        assert_eq!(plan.entries[0].content, "Only task");
    }

    #[test]
    fn test_translate_goal_driver_update() {
        let evt = DaemonEvent::GoalDriverUpdate {
            transition: "accepted".into(),
            goal: Some(serde_json::json!({"summary": "Ship the feature"})),
            proposed_summary: None,
        };
        match translate_event(&evt) {
            EventTranslation::GoalDriverUpdate {
                transition,
                summary,
            } => {
                assert_eq!(transition, "accepted");
                assert_eq!(summary, "Ship the feature");
            }
            _ => panic!("Expected GoalDriverUpdate"),
        }
    }

    #[test]
    fn test_translate_goal_driver_update_cleared() {
        let evt = DaemonEvent::GoalDriverUpdate {
            transition: "cleared".into(),
            goal: None,
            proposed_summary: None,
        };
        match translate_event(&evt) {
            EventTranslation::GoalDriverUpdate {
                transition,
                summary,
            } => {
                assert_eq!(transition, "cleared");
                assert!(summary.is_empty());
            }
            _ => panic!("Expected GoalDriverUpdate for cleared"),
        }
    }

    #[test]
    fn test_translate_system_reminder() {
        let evt = DaemonEvent::SystemReminder {
            slug: "repo-status".into(),
            display_name: "Repository status".into(),
            body: "clean".into(),
            reason: serde_json::json!({"type": "repository_status"}),
        };
        match translate_event(&evt) {
            EventTranslation::SystemReminder {
                slug,
                display_name,
                body,
                ..
            } => {
                assert_eq!(slug, "repo-status");
                assert_eq!(display_name, "Repository status");
                assert_eq!(body, "clean");
            }
            _ => panic!("Expected SystemReminder"),
        }
    }

    #[test]
    fn test_deserialize_system_reminder() {
        let json = r#"{"type":"system_reminder","slug":"repo-status","display_name":"Repository status","body":"clean","reason":{"type":"repository_status"}}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        match evt {
            DaemonEvent::SystemReminder {
                slug,
                display_name,
                body,
                ..
            } => {
                assert_eq!(slug, "repo-status");
                assert_eq!(display_name, "Repository status");
                assert_eq!(body, "clean");
            }
            _ => panic!("Expected SystemReminder"),
        }
    }

    #[test]
    fn test_deserialize_goal_driver_update() {
        let json = r#"{"type":"goal_driver_update","transition":"accepted","goal":{"summary":"Ship it"},"proposed_summary":null}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        match evt {
            DaemonEvent::GoalDriverUpdate {
                transition, goal, ..
            } => {
                assert_eq!(transition, "accepted");
                assert_eq!(goal.unwrap()["summary"], "Ship it");
            }
            _ => panic!("Expected GoalDriverUpdate"),
        }
    }

    #[test]
    fn test_deserialize_session_state_changed() {
        let json = r#"{"type":"session_state_changed","domains":["todos"]}"#;
        let evt: DaemonEvent = serde_json::from_str(json).unwrap();
        match evt {
            DaemonEvent::SessionStateChanged { domains } => {
                assert_eq!(domains, vec!["todos"]);
            }
            _ => panic!("Expected SessionStateChanged"),
        }
    }

    #[test]
    fn test_translate_session_state_changed_todos() {
        let evt = DaemonEvent::SessionStateChanged {
            domains: vec!["todos".into()],
        };
        match translate_event(&evt) {
            EventTranslation::TodoStateChange => {}
            _ => panic!("Expected TodoStateChange for todos domain"),
        }
    }

    #[test]
    fn test_translate_session_state_changed_non_todos() {
        let evt = DaemonEvent::SessionStateChanged {
            domains: vec!["flags".into()],
        };
        match translate_event(&evt) {
            EventTranslation::FacetChoicesCheck => {}
            _ => panic!("Expected FacetChoicesCheck for non-todos domain"),
        }
    }

    #[test]
    fn test_deserialize_permission_monitor_switch() {
        let json = r#"{
            "type": "permission_monitor_switch",
            "from_monitor": {"type": "standard"},
            "to_monitor": {"type": "bypass"}
        }"#;
        let evt: DaemonEvent = serde_json::from_str(json).expect("Failed to deserialize");
        match evt {
            DaemonEvent::PermissionMonitorSwitch {
                to_monitor,
                from_monitor,
            } => {
                assert_eq!(to_monitor.kind, "bypass");
                assert_eq!(from_monitor.kind, "standard");
            }
            _ => panic!("Expected PermissionMonitorSwitch"),
        }
    }

    #[test]
    fn test_deserialize_permission_monitor_switch_autonomous() {
        // The autonomous variant has extra fields that should be ignored.
        let json = r#"{
            "type": "permission_monitor_switch",
            "from_monitor": {"type": "standard"},
            "to_monitor": {"type": "autonomous", "classifier_model": null, "classifier_rules": null, "max_consecutive_denials": 3}
        }"#;
        let evt: DaemonEvent = serde_json::from_str(json).expect("Failed to deserialize");
        match evt {
            DaemonEvent::PermissionMonitorSwitch { to_monitor, .. } => {
                assert_eq!(to_monitor.kind, "autonomous");
            }
            _ => panic!("Expected PermissionMonitorSwitch"),
        }
    }

    #[test]
    fn test_translate_permission_monitor_switch() {
        let evt = DaemonEvent::PermissionMonitorSwitch {
            to_monitor: PermissionMonitorSummary {
                kind: "autonomous".into(),
            },
            from_monitor: PermissionMonitorSummary {
                kind: "standard".into(),
            },
        };
        match translate_event(&evt) {
            EventTranslation::PermissionMonitorSwitch { mode } => {
                assert_eq!(mode, "autonomous");
            }
            _ => panic!("Expected PermissionMonitorSwitch translation"),
        }
    }

    fn sample_plan_handoff_json() -> &'static str {
        r#"{
            "type":"interrogative",
            "prompt_id":"p1",
            "interrogative_id":"i1",
            "question":"Ready to implement?",
            "interrogative_type":"plan_handoff",
            "plan_handoff":{
                "plan_path":"/tmp/plan.md",
                "display_path":"plan.md",
                "plan_text":"Step 1. Do the thing.",
                "target_facet":"execute",
                "title":"Implementation Plan",
                "action_labels":{
                    "implement_new_context":"Implement in a new context",
                    "implement_current_context":"Implement in current context",
                    "cancel":"Cancel",
                    "refuse":"Send back with feedback"
                }
            }
        }"#
    }

    #[test]
    fn test_translate_plan_handoff_interrogative() {
        let evt: DaemonEvent = serde_json::from_str(sample_plan_handoff_json()).unwrap();
        match translate_event(&evt) {
            EventTranslation::PlanHandoff {
                interrogative_id,
                context,
                ..
            } => {
                assert_eq!(interrogative_id, "i1");
                assert_eq!(context.target_facet, "execute");
                assert_eq!(context.plan_text, "Step 1. Do the thing.");
                assert_eq!(
                    context.action_labels.implement_new_context,
                    "Implement in a new context"
                );
            }
            _ => panic!("Expected PlanHandoff translation"),
        }
    }

    #[test]
    fn test_plan_handoff_missing_context_falls_back() {
        // Without the structured payload we cannot render the choice; fall back
        // to the generic interrogative path rather than dropping the event.
        let evt = DaemonEvent::Interrogative {
            prompt_id: "p1".into(),
            interrogative_id: "i1".into(),
            question: "Ready?".into(),
            interrogative_type: "plan_handoff".into(),
            plan_handoff: None,
            goal_proposal: None,
        };
        match translate_event(&evt) {
            EventTranslation::InterrogativeRequest {
                interrogative_type, ..
            } => assert_eq!(interrogative_type, "plan_handoff"),
            _ => panic!("Expected InterrogativeRequest fallback"),
        }
    }

    #[test]
    fn test_build_plan_handoff_payload_options() {
        let evt: DaemonEvent = serde_json::from_str(sample_plan_handoff_json()).unwrap();
        let context = match evt {
            DaemonEvent::Interrogative {
                plan_handoff: Some(c),
                ..
            } => c,
            _ => unreachable!(),
        };
        let payload = build_plan_handoff_payload("Ready?", &context);
        let q = &payload.questions[0];
        assert_eq!(q.mode, AskUserQuestionMode::SingleSelect);
        assert!(q.allow_free_text);
        let ids: Vec<&str> = q.options.iter().map(|o| o.id.as_str()).collect();
        assert_eq!(
            ids,
            vec![
                PLAN_HANDOFF_NEW_CONTEXT,
                PLAN_HANDOFF_CURRENT_CONTEXT,
                PLAN_HANDOFF_REFUSE,
                PLAN_HANDOFF_CANCEL,
            ]
        );
        // The plan text is surfaced as review context.
        assert!(
            q.context
                .as_ref()
                .unwrap()
                .contains("Step 1. Do the thing.")
        );
    }

    #[test]
    fn test_build_plan_handoff_payload_omits_empty_refuse() {
        let mut context: PlanHandoffContext =
            match serde_json::from_str::<DaemonEvent>(sample_plan_handoff_json()).unwrap() {
                DaemonEvent::Interrogative {
                    plan_handoff: Some(c),
                    ..
                } => c,
                _ => unreachable!(),
            };
        context.action_labels.refuse = String::new();
        let payload = build_plan_handoff_payload("Ready?", &context);
        let ids: Vec<&str> = payload.questions[0]
            .options
            .iter()
            .map(|o| o.id.as_str())
            .collect();
        assert!(!ids.contains(&PLAN_HANDOFF_REFUSE));
    }

    #[test]
    fn test_build_plan_handoff_response_decisions() {
        assert_eq!(
            build_plan_handoff_response(Some(PLAN_HANDOFF_NEW_CONTEXT), None),
            serde_json::json!({"kind":"plan_handoff_answer","decision":"implement_new_context"})
        );
        assert_eq!(
            build_plan_handoff_response(Some(PLAN_HANDOFF_CURRENT_CONTEXT), None),
            serde_json::json!({"kind":"plan_handoff_answer","decision":"implement_current_context"})
        );
        assert_eq!(
            build_plan_handoff_response(Some(PLAN_HANDOFF_REFUSE), Some("needs tests")),
            serde_json::json!({"kind":"plan_handoff_answer","decision":"refuse","feedback":"needs tests"})
        );
    }

    #[test]
    fn test_build_plan_handoff_response_cancel_and_unknown() {
        assert_eq!(
            build_plan_handoff_response(Some(PLAN_HANDOFF_CANCEL), None),
            serde_json::json!({"kind":"cancel"})
        );
        assert_eq!(
            build_plan_handoff_response(None, None),
            serde_json::json!({"kind":"cancel"})
        );
        assert_eq!(
            build_plan_handoff_response(Some("bogus"), None),
            serde_json::json!({"kind":"cancel"})
        );
    }

    // ----- Goal proposal tests ------------------------------------------------

    #[test]
    fn test_translate_goal_proposal() {
        let evt = DaemonEvent::Interrogative {
            prompt_id: "p1".into(),
            interrogative_id: "i1".into(),
            question: "Accept this goal?".into(),
            interrogative_type: "goal_proposal".into(),
            plan_handoff: None,
            goal_proposal: Some(GoalProposalContext {
                proposed_summary: "Ship the feature".into(),
                proposed_file_path: None,
                title: "Goal Proposal".into(),
                action_labels: GoalProposalActionLabels {
                    accept: "Accept".into(),
                    reject: "Reject".into(),
                },
            }),
        };
        match translate_event(&evt) {
            EventTranslation::GoalProposal {
                interrogative_id,
                context,
                ..
            } => {
                assert_eq!(interrogative_id, "i1");
                assert_eq!(context.proposed_summary, "Ship the feature");
                assert_eq!(context.title, "Goal Proposal");
            }
            _ => panic!("Expected GoalProposal"),
        }
    }

    #[test]
    fn test_goal_proposal_missing_context_falls_back() {
        let evt = DaemonEvent::Interrogative {
            prompt_id: "p1".into(),
            interrogative_id: "i1".into(),
            question: "Accept?".into(),
            interrogative_type: "goal_proposal".into(),
            plan_handoff: None,
            goal_proposal: None,
        };
        match translate_event(&evt) {
            EventTranslation::InterrogativeRequest {
                interrogative_type, ..
            } => assert_eq!(interrogative_type, "goal_proposal"),
            _ => panic!("Expected InterrogativeRequest fallback"),
        }
    }

    #[test]
    fn test_build_goal_proposal_payload() {
        let context = GoalProposalContext {
            proposed_summary: "Ship the feature".into(),
            proposed_file_path: Some("/tmp/goal.md".into()),
            title: "Goal Proposal".into(),
            action_labels: GoalProposalActionLabels {
                accept: "Accept".into(),
                reject: "Reject".into(),
            },
        };
        let payload = build_goal_proposal_payload("Accept this goal?", &context);
        let q = &payload.questions[0];
        assert_eq!(q.mode, AskUserQuestionMode::SingleSelect);
        assert!(!q.allow_free_text);
        let ids: Vec<&str> = q.options.iter().map(|o| o.id.as_str()).collect();
        assert_eq!(ids, vec![GOAL_PROPOSAL_ACCEPT, GOAL_PROPOSAL_REJECT]);
        // The proposed summary and file path are surfaced as review context.
        assert!(q.context.as_ref().unwrap().contains("Ship the feature"));
        assert!(q.context.as_ref().unwrap().contains("/tmp/goal.md"));
    }

    #[test]
    fn test_build_goal_proposal_response_accept() {
        let body = build_goal_proposal_response(Some(GOAL_PROPOSAL_ACCEPT));
        assert_eq!(body["kind"], "goal_proposal_answer");
        assert_eq!(body["accepted"], true);
    }

    #[test]
    fn test_build_goal_proposal_response_reject() {
        let body = build_goal_proposal_response(Some(GOAL_PROPOSAL_REJECT));
        assert_eq!(body["kind"], "goal_proposal_answer");
        assert_eq!(body["accepted"], false);
    }

    #[test]
    fn test_build_goal_proposal_response_none() {
        let body = build_goal_proposal_response(None);
        assert_eq!(body["kind"], "goal_proposal_answer");
        assert_eq!(body["accepted"], false);
    }
}
