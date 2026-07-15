//! Translation of daemon history items → ACP session notifications.
//!
//! When a session is loaded via `session/load`, the daemon's `GET /history`
//! endpoint returns typed history items (`KnownSessionHistoryItem`). This
//! module translates each item into one or more ACP `SessionUpdate` values
//! that are sent as `SessionNotification` messages before the
//! `LoadSessionResponse`.
//!
//! # Daemon history item shapes (from `polytoken openapi`)
//!
//! All items share `HistoryItemMeta` fields (`projected_index`, `item_id`)
//! plus a `type` discriminant. The translatable variants are:
//!
//! - `user`: `{ type: "user", content: String, prompt_id, emitted_at }`
//! - `assistant`: `{ type: "assistant", blocks: [ContentBlock], prompt_id, emitted_at }`
//!   - `ContentBlock` variants: `{ type: "text", text }`, `{ type: "tool_use", id, name, input }`,
//!     `{ type: "thinking", text, signature }`, `{ type: "redacted_thinking", data }`
//! - `tool_result`: `{ type: "tool_result", call_id: String, content: ToolResultContent?, is_error?, prompt_id, emitted_at }`
//!   - `ToolResultContent`: `{ text: String }` | `{ blocks: [ContentBlock] }` | `{ image: {...} }`
//! - `facet_switch`: `{ type: "facet_switch", from_facet, to_facet, prompt_id, emitted_at? }`
//!
//! Internal/skipped types: `session_lifecycle`, `state_update`, `model_switch`,
//! `compaction_fencepost`, `system_reminder`, `classifier_decision`,
//! `context_cleared`, `image_reference`.

use agent_client_protocol::schema::v1 as acp;
use tracing::debug;

use crate::events::{extract_locations, tool_kind_for_name};

/// Translate a single daemon history item into zero or more ACP session updates.
///
/// Items with `type` values that have no ACP equivalent (e.g. `session_lifecycle`,
/// `state_update`, `model_switch`) produce an empty vec.
pub fn translate_history_item(item: &serde_json::Value) -> Vec<acp::SessionUpdate> {
    let item_type = item
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("(missing type)");

    let updates = match item_type {
        "user" => translate_user_item(item),
        "assistant" => translate_assistant_item(item),
        "tool_result" => translate_tool_result_item(item),
        "facet_switch" => translate_facet_switch_item(item),
        // Internal/unknown types — skip gracefully
        _ => Vec::new(),
    };

    debug!(
        history_type = %item_type,
        updates_produced = updates.len(),
        "Translated history item"
    );

    updates
}

/// Translate a list of history items into ACP session notifications.
///
/// Each `SessionUpdate` is wrapped in a `SessionNotification` with the given
/// session ID, ready to be sent to the client.
pub fn translate_history_to_notifications(
    items: &[serde_json::Value],
    session_id: &str,
) -> Vec<acp::SessionNotification> {
    let sid = acp::SessionId::new(session_id.to_string());
    let mut notifications = Vec::new();
    for item in items {
        let updates = translate_history_item(item);
        for update in updates {
            notifications.push(acp::SessionNotification::new(sid.clone(), update));
        }
    }
    notifications
}

// ---------------------------------------------------------------------------
// Per-type translators
// ---------------------------------------------------------------------------

/// `user` → `UserMessageChunk`
///
/// Daemon shape: `{ content: String, ... }`
fn translate_user_item(item: &serde_json::Value) -> Vec<acp::SessionUpdate> {
    let content = item.get("content").and_then(|v| v.as_str()).unwrap_or("");
    if content.is_empty() {
        return Vec::new();
    }
    let content_block: acp::ContentBlock = content.to_string().into();
    let chunk = acp::ContentChunk::new(content_block);
    vec![acp::SessionUpdate::UserMessageChunk(chunk)]
}

/// `assistant` → `AgentMessageChunk` + `ToolCall` + `AgentThoughtChunk`
///
/// Daemon shape: `{ blocks: [ContentBlock], ... }`
/// Each block is one of: `text`, `tool_use`, `thinking`, `redacted_thinking`
fn translate_assistant_item(item: &serde_json::Value) -> Vec<acp::SessionUpdate> {
    let blocks = match item.get("blocks").and_then(|v| v.as_array()) {
        Some(b) => b,
        None => return Vec::new(),
    };

    let mut updates = Vec::new();
    for block in blocks {
        let block_type = block.get("type").and_then(|v| v.as_str()).unwrap_or("");

        match block_type {
            "text" => {
                let text = block.get("text").and_then(|v| v.as_str()).unwrap_or("");
                if !text.is_empty() {
                    let content_block: acp::ContentBlock = text.to_string().into();
                    let chunk = acp::ContentChunk::new(content_block);
                    updates.push(acp::SessionUpdate::AgentMessageChunk(chunk));
                }
            }
            "tool_use" => {
                let call_id = block.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let name = block.get("name").and_then(|v| v.as_str()).unwrap_or("");
                if call_id.is_empty() || name.is_empty() {
                    continue;
                }
                let kind = tool_kind_for_name(name);
                let mut tool_call = acp::ToolCall::new(call_id.to_string(), name.to_string())
                    .kind(kind)
                    .status(acp::ToolCallStatus::Pending);
                if let Some(input) = block.get("input") {
                    tool_call = tool_call.raw_input(input.clone());
                    if let Some(locs) = extract_locations(input) {
                        tool_call = tool_call.locations(locs);
                    }
                }
                updates.push(acp::SessionUpdate::ToolCall(tool_call));
            }
            "thinking" => {
                let text = block.get("text").and_then(|v| v.as_str()).unwrap_or("");
                if !text.is_empty() {
                    let content_block: acp::ContentBlock = text.to_string().into();
                    let chunk = acp::ContentChunk::new(content_block);
                    updates.push(acp::SessionUpdate::AgentThoughtChunk(chunk));
                }
            }
            "redacted_thinking" => {
                let data = block.get("data").and_then(|v| v.as_str()).unwrap_or("");
                let text = format!("[redacted reasoning: {}]", data);
                let content_block: acp::ContentBlock = text.into();
                let chunk = acp::ContentChunk::new(content_block);
                updates.push(acp::SessionUpdate::AgentThoughtChunk(chunk));
            }
            _ => {
                // Unknown block type — skip
            }
        }
    }
    updates
}

/// `tool_result` → `ToolCallUpdate`
///
/// Daemon shape: `{ call_id: String, content: ToolResultContent?, is_error?: bool, ... }`
/// `ToolResultContent` can be `{ text: String }`, `{ blocks: [ContentBlock] }`, or `{ image: {...} }`
fn translate_tool_result_item(item: &serde_json::Value) -> Vec<acp::SessionUpdate> {
    let call_id = item.get("call_id").and_then(|v| v.as_str()).unwrap_or("");
    if call_id.is_empty() {
        return Vec::new();
    }

    let is_error = item
        .get("is_error")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let status = if is_error {
        acp::ToolCallStatus::Failed
    } else {
        acp::ToolCallStatus::Completed
    };

    let mut fields = acp::ToolCallUpdateFields::new().status(status);

    // Extract text content from the ToolResultContent
    let content = item.get("content");
    if let Some(content) = content {
        // Shape 1: { text: String }
        if let Some(text) = content.get("text").and_then(|v| v.as_str()) {
            // Try to parse as JSON for raw_output
            if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(text) {
                fields = fields.raw_output(json_val);
            }
            let block: acp::ContentBlock = text.to_string().into();
            fields = fields.content(vec![acp::ToolCallContent::from(block)]);
        }
        // Shape 2: { blocks: [ContentBlock] }
        else if let Some(blocks) = content.get("blocks").and_then(|v| v.as_array()) {
            let mut tool_contents = Vec::new();
            let mut raw_parts: Vec<serde_json::Value> = Vec::new();
            for block in blocks {
                if let Some(text) = block.get("text").and_then(|v| v.as_str()) {
                    let cb: acp::ContentBlock = text.to_string().into();
                    tool_contents.push(acp::ToolCallContent::from(cb));
                    raw_parts.push(serde_json::Value::String(text.to_string()));
                }
            }
            if !tool_contents.is_empty() {
                fields = fields.content(tool_contents);
            }
            if raw_parts.len() == 1 {
                fields = fields.raw_output(raw_parts.into_iter().next().unwrap());
            } else if raw_parts.len() > 1 {
                fields = fields.raw_output(serde_json::Value::Array(raw_parts));
            }
        }
    }

    let update = acp::ToolCallUpdate::new(call_id.to_string(), fields);
    vec![acp::SessionUpdate::ToolCallUpdate(update)]
}

/// `facet_switch` → `CurrentModeUpdate`
///
/// Daemon shape: `{ from_facet: String, to_facet: String, ... }`
fn translate_facet_switch_item(item: &serde_json::Value) -> Vec<acp::SessionUpdate> {
    let to_facet = item.get("to_facet").and_then(|v| v.as_str()).unwrap_or("");
    if to_facet.is_empty() {
        return Vec::new();
    }
    let mode_update = acp::CurrentModeUpdate::new(to_facet.to_string());
    vec![acp::SessionUpdate::CurrentModeUpdate(mode_update)]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_translate_user_item() {
        let item = serde_json::json!({
            "type": "user",
            "content": "Hello, world!",
            "prompt_id": "abc123",
            "emitted_at": "2026-07-14T20:00:00Z",
        });
        let updates = translate_history_item(&item);
        assert_eq!(updates.len(), 1);
        // Should be a UserMessageChunk
        assert!(matches!(
            &updates[0],
            acp::SessionUpdate::UserMessageChunk(_)
        ));
    }

    #[test]
    fn test_translate_assistant_text() {
        let item = serde_json::json!({
            "type": "assistant",
            "blocks": [
                {"type": "text", "text": "I can help with that."},
            ],
            "prompt_id": "abc123",
            "emitted_at": "2026-07-14T20:01:00Z",
        });
        let updates = translate_history_item(&item);
        assert_eq!(updates.len(), 1);
        assert!(matches!(
            &updates[0],
            acp::SessionUpdate::AgentMessageChunk(_)
        ));
    }

    #[test]
    fn test_translate_assistant_tool_use() {
        let item = serde_json::json!({
            "type": "assistant",
            "blocks": [
                {"type": "text", "text": "Let me check that file."},
                {"type": "tool_use", "id": "call_001", "name": "file_read", "input": {"path": "/tmp/test.txt"}},
            ],
            "prompt_id": "abc123",
            "emitted_at": "2026-07-14T20:02:00Z",
        });
        let updates = translate_history_item(&item);
        // Should produce 2 updates: AgentMessageChunk + ToolCall
        assert_eq!(updates.len(), 2);
        assert!(matches!(
            &updates[0],
            acp::SessionUpdate::AgentMessageChunk(_)
        ));
        match &updates[1] {
            acp::SessionUpdate::ToolCall(tc) => {
                assert_eq!(tc.tool_call_id, "call_001".into());
                // ToolCall has `title` not `name` in ACP SDK
                assert_eq!(tc.title, "file_read");
            }
            other => panic!("Expected ToolCall second, got {other:?}"),
        }
    }

    #[test]
    fn test_translate_assistant_thinking() {
        let item = serde_json::json!({
            "type": "assistant",
            "blocks": [
                {"type": "thinking", "text": "Hmm, let me think about this...", "signature": "sig"},
            ],
            "prompt_id": "abc123",
            "emitted_at": "2026-07-14T20:03:00Z",
        });
        let updates = translate_history_item(&item);
        assert_eq!(updates.len(), 1);
        assert!(matches!(
            &updates[0],
            acp::SessionUpdate::AgentThoughtChunk(_)
        ));
    }

    #[test]
    fn test_translate_tool_result() {
        let item = serde_json::json!({
            "type": "tool_result",
            "call_id": "call_001",
            "content": {"text": "File contents here"},
            "is_error": false,
            "prompt_id": "abc123",
            "emitted_at": "2026-07-14T20:04:00Z",
        });
        let updates = translate_history_item(&item);
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            acp::SessionUpdate::ToolCallUpdate(update) => {
                assert_eq!(update.tool_call_id, "call_001".into());
                assert!(update.fields.status.is_some());
                // Should be Completed
                let status = update.fields.status.as_ref().unwrap();
                assert_eq!(status, &acp::ToolCallStatus::Completed);
            }
            other => panic!("Expected ToolCallUpdate, got {other:?}"),
        }
    }

    #[test]
    fn test_translate_facet_switch() {
        let item = serde_json::json!({
            "type": "facet_switch",
            "from_facet": "plan",
            "to_facet": "execute",
            "prompt_id": "abc123",
            "emitted_at": "2026-07-14T20:05:00Z",
        });
        let updates = translate_history_item(&item);
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            acp::SessionUpdate::CurrentModeUpdate(mode) => {
                assert_eq!(mode.current_mode_id, "execute".into());
            }
            other => panic!("Expected CurrentModeUpdate, got {other:?}"),
        }
    }

    #[test]
    fn test_translate_skip_unknown() {
        let types = [
            "session_lifecycle",
            "state_update",
            "model_switch",
            "compaction_fencepost",
            "system_reminder",
            "classifier_decision",
            "context_cleared",
            "image_reference",
        ];
        for t in &types {
            let item = serde_json::json!({
                "type": t,
                "emitted_at": "2026-07-14T20:00:00Z",
            });
            let updates = translate_history_item(&item);
            assert!(
                updates.is_empty(),
                "Expected empty vec for type '{}', got {} updates",
                t,
                updates.len()
            );
        }
    }
}
