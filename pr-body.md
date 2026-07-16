## Problem

When the agent calls `ask_user_question`, the daemon emits a `tool_call` SSE event containing the full questions payload (context, options, justifications). The ACP shim was setting that entire blob as `raw_input` on the ACP `ToolCall` update — and Paseo renders `raw_input` as visible JSON in the transcript, producing a wall of raw JSON.

The actual interactive question UI is delivered separately via the `_polytoken/ask_user_question` extension request, so the `raw_input` is pure noise.

## Fix

Added a `tool_call_title_override()` helper in `events.rs` that detects `ask_user_question` tool calls and returns a concise title (`"Asking N questions"`) instead of letting the full `raw_input` through. Applied in both code paths:

1. **Live events** (`src/events.rs`) — when translating `DaemonEvent::ToolCall`, if the override returns `Some(title)`, set the title field directly and skip `raw_input` / `extract_locations`.
2. **History replay** (`src/history.rs`) — same logic for `session/load` translation, so loaded sessions also get clean titles.

All other tool calls are unaffected — they still get `raw_input` and location extraction as before.

## Testing

- 134 tests pass (including 2 new tests)
- `test_translate_tool_call_ask_user_question_suppresses_raw_input` — verifies title is set and `raw_input` is `None`
- `test_translate_tool_call_other_tools_keep_raw_input` — verifies other tools still get `raw_input`
