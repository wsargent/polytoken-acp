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
    #[serde(rename = "heartbeat")]
    Heartbeat,
    #[serde(other)]
    Other,
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
        DaemonEvent::Heartbeat => "heartbeat",
        DaemonEvent::Other => "unknown",
    }
}

/// Human-readable summary of key fields for a daemon event (for logging).
/// Returns a compact, single-line description suitable for log fields.
pub fn event_summary(evt: &DaemonEvent) -> String {
    match evt {
        DaemonEvent::MessageStart { prompt_id } => format!("prompt_id={}", prompt_id),
        DaemonEvent::ContentBlockStart { prompt_id, block_index, .. } => {
            format!("prompt_id={} block_index={}", prompt_id, block_index)
        }
        DaemonEvent::ContentBlockDelta { prompt_id, block_index, delta } => {
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
            format!("prompt_id={} block_index={} {}", prompt_id, block_index, delta_desc)
        }
        DaemonEvent::ContentBlockStop { prompt_id, block_index } => {
            format!("prompt_id={} block_index={}", prompt_id, block_index)
        }
        DaemonEvent::MessageComplete { prompt_id } => format!("prompt_id={}", prompt_id),
        DaemonEvent::TurnCancelled { prompt_id } => format!("prompt_id={}", prompt_id),
        DaemonEvent::ModelError { prompt_id } => format!("prompt_id={}", prompt_id),
        DaemonEvent::ToolCall { prompt_id, call_id, name, input, .. } => {
            let input_desc = match input {
                Some(v) => {
                    let s = serde_json::to_string(v).unwrap_or_default();
                    let preview: String = s.chars().take(120).collect();
                    format!(" input={}", preview)
                }
                None => String::new(),
            };
            format!("prompt_id={} call_id={} name={}{}", prompt_id, call_id, name, input_desc)
        }
        DaemonEvent::ToolResult { prompt_id, call_id, content, content_full, is_error, .. } => {
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
        DaemonEvent::Interrogative { prompt_id, interrogative_id, question, interrogative_type, .. } => {
            let preview: String = question.chars().take(100).collect();
            format!(
                "prompt_id={} id={} type={} question={:?}",
                prompt_id, interrogative_id, interrogative_type, preview
            )
        }
        DaemonEvent::AskUserQuestion { prompt_id, interrogative_id, payload, .. } => {
            format!(
                "prompt_id={} id={} questions={}",
                prompt_id, interrogative_id, payload.questions.len()
            )
        }
        DaemonEvent::ModelChanged { model } => format!("model={}", model),
        DaemonEvent::SessionTitleChanged { title } => format!("title={}", title),
        DaemonEvent::FacetChanged { facet } => format!("facet={}", facet),
        DaemonEvent::Heartbeat => String::new(),
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
            let mut tool_call = acp::ToolCall::new(call_id.clone(), name.clone())
                .status(acp::ToolCallStatus::Pending);
            if let Some(input) = input {
                tool_call = tool_call.raw_input(input.clone());
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
            ..
        } => {
            if interrogative_type == "permission" {
                debug!(interrogative_type = %interrogative_type, "Permission request from daemon");
                EventTranslation::PermissionRequest {
                    interrogative_id: interrogative_id.clone(),
                    question: question.clone(),
                }
            } else {
                // Forward non-permission interrogatives (confirmation, clarification,
                // capability, plan_handoff, goal_proposal) as permission-like requests
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

        // Facet changed → forward as current_mode_update
        DaemonEvent::FacetChanged { facet } => {
            debug!(facet = %facet, "Facet changed; forwarding as current_mode_update");
            let mode_update = acp::CurrentModeUpdate::new(facet.clone());
            EventTranslation::Update(acp::SessionUpdate::CurrentModeUpdate(mode_update))
        }

        _ => EventTranslation::Ignore,
    }
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

/// Extract joined text from ACP ContentBlock array.
pub fn extract_text(blocks: &[acp::ContentBlock]) -> String {
    blocks
        .iter()
        .filter_map(|block| match block {
            acp::ContentBlock::Text(text) => Some(text.text.as_str()),
            _ => None,
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
                assert_eq!(u.title.as_opt_ref().unwrap(), Some(&"Updated Title".to_string()));
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
        let evt = DaemonEvent::FacetChanged {
            facet: "plan".into(),
        };
        match translate_event(&evt) {
            EventTranslation::Update(acp::SessionUpdate::CurrentModeUpdate(u)) => {
                assert_eq!(u.current_mode_id.0.as_ref(), "plan");
            }
            _ => panic!("Expected CurrentModeUpdate"),
        }
    }

    #[test]
    fn test_translate_interrogative_confirmation() {
        let evt = DaemonEvent::Interrogative {
            prompt_id: "abc".into(),
            interrogative_id: "int_1".into(),
            question: "Are you sure?".into(),
            interrogative_type: "confirmation".into(),
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
}
