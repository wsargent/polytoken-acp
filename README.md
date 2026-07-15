# polytoken-acp

An [Agent Client Protocol (ACP)](https://agentclientprotocol.com/) server shim for the [Polytoken](https://github.com/wsargent/polytoken) daemon.

## What It Does

`polytoken-acp` bridges ACP (JSON-RPC over stdio) to the Polytoken daemon's HTTP/SSE API. This allows Paseo — or any ACP-compatible editor — to drive Polytoken as an agent without the TUI.

```
Paseo  ──stdio JSON-RPC──>  polytoken-acp (Rust binary, ACP Agent)
                               │
                               ├── spawns polytoken daemon (one per ACP session)
                               ├── POST /prompt → 202 {prompt_id}
                               ├── GET /events (SSE) → ACP session/update notifications
                               ├── POST /interrogative/{id}/respond ← ACP session/request_permission
                               └── POST /turn/cancel ← ACP session/cancel
```

## Building

```bash
cargo build --release
```

The binary will be at `target/release/polytoken-acp`.

### Prerequisites

- Rust 1.85+ (edition 2024, required by `agent-client-protocol` 1.x)
- The `polytoken` binary on your `PATH`

## Paseo Configuration

Add the following to `~/.paseo/config.json`:

```json
{
  "agents": {
    "providers": {
      "polytoken": {
        "extends": "acp",
        "label": "Polytoken",
        "command": ["polytoken-acp"]
      }
    }
  }
}
```

Then launch Paseo, select "Polytoken" as the provider, and start a session.

## How It Works

1. **Paseo spawns `polytoken-acp`** as a subprocess and communicates over stdio using JSON-RPC (ACP).
2. **On `session/new`**, the shim spawns a `polytoken daemon` process for the session's working directory, using a random port and credential file.
3. **On `session/prompt`**, the shim forwards the prompt text to the daemon's `POST /prompt` endpoint, then connects to the daemon's SSE event stream (`GET /events`) and translates daemon events into ACP `session/update` notifications.
4. **Permission requests** (interrogative events) are forwarded to the ACP client via `session/request_permission`, and responses are relayed back to the daemon via `POST /interrogative/{id}/respond`. **`ask_user_question` events** are forwarded via the `polytoken/ask_user_question` extension method, and answers are relayed back.
5. **On `session/cancel`**, the shim calls `POST /turn/cancel` on the daemon.
6. **On disconnect** (stdin EOF), the shim terminates all daemon processes.

### Event Translation

| Daemon SSE Event | ACP Action |
|---|---|
| `content_block_delta` (text_delta) | `session/update` → `agent_message_chunk` |
| `content_block_delta` (thinking) | `session/update` → `agent_thought_chunk` |
| `content_block_delta` (redacted_thinking) | `session/update` → `agent_thought_chunk` (placeholder) |
| `content_block_delta` (open_ai_reasoning) | `session/update` → `agent_thought_chunk` |
| `content_block_delta` (signature_delta) | Ignored (no ACP equivalent) |
| `message_start` | Ignored (no ACP equivalent needed) |
| `tool_call` | `session/update` → `tool_call` (status: pending) |
| `tool_result` | `session/update` → `tool_call_update` (status: completed/failed). Uses `content_full` when available, falls back to `content`. |
| `interrogative` (permission) | `session/request_permission` → `POST /interrogative/{id}/respond` |
| `interrogative` (confirmation, capability, goal_proposal) | `session/request_permission` → `POST /interrogative/{id}/respond` (mapped to appropriate response kind) |
| `interrogative` (clarification, plan_handoff) | `session/request_permission` → `POST /interrogative/{id}/respond` (best-effort; cancelled if unsupported) |
| `ask_user_question` | `ext_method` (`polytoken/ask_user_question`) → `POST /interrogative/{id}/respond` |
| `session_title_changed` | `session/update` → `session_info_update` (title) |
| `facet_changed` | `session/update` → `current_mode_update` |
| `model_changed` | Ignored (model config option already tracks state from `/state`) |
| `message_complete` | `session/prompt` response with `stop_reason: end_turn` |
| `turn_cancelled` | `session/prompt` response with `stop_reason: cancelled` |
| `model_error` | `session/prompt` response with `stop_reason: end_turn` |
| heartbeat / unknown | Ignored |

## Testing

### Unit Tests

```bash
cargo test
```

Unit tests cover:
- SSE event deserialization (all handled event types + `#[serde(other)]` catch-all)
- Event-to-ACP translation (text delta, thinking/reasoning deltas, tool call, tool result, message complete, turn cancelled, interrogative)
- `message_start`, `model_changed`, `facet_changed`, `session_title_changed` deserialization and translation
- `tool_result` with `content_full` fallback
- Non-permission interrogative types (confirmation, etc.)
- Permission translation (options construction, outcome→granted mapping)
- `ask_user_question` event deserialization (payload with questions, options, modes) and translation
- `ask_user_question` payload serialization for ext_method forwarding
- Content block text extraction
- MCP server config conversion (Stdio, Http, Sse-skipped, multiple servers, empty list)

### Integration Tests

Integration tests require the `polytoken` binary on PATH and LLM credentials:

```bash
cargo test -- --ignored
```

## Troubleshooting

### Debug logging

The shim logs structured JSON to stderr (stdout is reserved for JSON-RPC). Set the `RUST_LOG` environment variable:

```bash
# Default: conversation-level logging (prompts, tool calls, permissions, turn events)
RUST_LOG=info polytoken-acp

# Include per-event detail: every daemon SSE event and ACP notification
RUST_LOG=debug polytoken-acp

# Shim logs only (suppress daemon stderr)
RUST_LOG=polytoken_acp=info polytoken-acp

# Verbose conversation logging only
RUST_LOG=polytoken_acp::conv=debug polytoken-acp
```

Each log line is a JSON object with fields like `timestamp`, `level`, `target`, `fields`, and `message`. The conversation target (`polytoken_acp::conv`) emits structured events:

| `message` field | Description | Key structured fields |
|---|---|---|
| `prompt_start` | User prompt forwarded to daemon | `session_id`, `prompt_len`, `prompt_preview` |
| `daemon_event` | Daemon SSE event received and translated | `event_type`, `summary` |
| `acp_notification` | ACP session update sent to editor | `update_type` |
| `turn_end` | Assistant turn completed | `prompt_id` |
| `turn_cancelled` | Turn was cancelled | `prompt_id` |
| `permission_request` | Permission interrogative forwarded to client | `interrogative_id`, `question` |
| `permission_response` | Client's permission answer relayed to daemon | `interrogative_id`, `granted` |
| `interrogative_request` | Non-permission interrogative forwarded | `interrogative_id`, `interrogative_type`, `question` |
| `interrogative_response` | Client's interrogative answer relayed | `interrogative_id`, `interrogative_type`, `granted` |
| `ask_user_question` | `ask_user_question` event forwarded via ext_method | `interrogative_id`, `question_count` |
| `cancel` | Client cancelled the turn | `session_id` |

At `debug` level, the `daemon_event` lines include an `event_type` (e.g. `tool_call`, `content_block_delta`) and a `summary` with key fields and content previews (up to 80–120 chars). Example:

```json
{"timestamp":"2025-01-01T12:00:00.123Z","level":"DEBUG","target":"polytoken_acp::conv","fields":{"event_type":"tool_call","summary":"prompt_id=abc call_id=call_1 name=read_file input={\"path\":\"/tmp/test\"}"},"message":"daemon_event"}
```

### Daemon process issues

Each ACP session spawns a `polytoken daemon` process. If the daemon fails to start:

1. Check that `polytoken` is on `PATH` and `polytoken --version` works.
2. Check the shim's stderr output for error messages.
3. The daemon's temp directory is under `$TMPDIR/polytoken-acp-{session_id}/` — check `logs/` for daemon logs.

### stdout pollution

The shim's own logging goes to stderr only. The daemon child process's stdout is piped and drained. If you see JSON-RPC errors, ensure nothing else writes to stdout.

## Limitations (v1)

- **MCP server forwarding**: Paseo's `mcpServers` are forwarded to the polytoken daemon. When a client sends MCP servers in `session/new`, `session/resume`, or `session/load`, the shim converts them to polytoken's YAML config format and passes a temporary `--project-config-dir` to the daemon. The daemon merges this project-level config on top of the user's global config, so ACP-provided servers are available alongside any configured in `~/.config/polytoken/`. ACP `Stdio` servers map to polytoken's stdio transport; `Http` servers map to polytoken's streamable-HTTP transport. The `Sse` variant is **not supported** — polytoken has no SSE transport, so SSE servers are skipped with a warning. See [MCP Server Forwarding](docs/mcp-servers.md) for details.
- **Image/file/audio prompts**: The daemon's `POST /prompt` endpoint only accepts plain text (`{"content": "string"}`). ACP prompt content blocks of type `Image`, `ResourceLink`, `Resource`, and `Audio` are converted to descriptive text placeholders (e.g. `[image: image/png, 1234 bytes]`, `[resource: file:///src/main.rs]`) so the agent knows something was attached. A warning is logged each time a non-text block is converted.
- **`ask_user_question` via ext_method**: The daemon's `ask_user_question` events are forwarded to the ACP client via the `polytoken/ask_user_question` extension method. The client must implement `ext_method` and return a JSON object with an `answers` array (each answer has `question_id`, `selected_option_ids`, and/or `free_text`). If the client does not support the extension or returns no answers, the interrogative is cancelled on the daemon so the agent can proceed.
- **Non-permission interrogatives**: `confirmation`, `capability`, and `goal_proposal` interrogatives are mapped to ACP `session/request_permission` (allow/reject) and translated to the appropriate daemon response kind. `clarification` and `plan_handoff` types cannot be fully answered via ACP's permission mechanism (they need structured input) and are cancelled if rejected.
- **`model_changed` events**: The daemon emits `model_changed` events, but we don't forward them as `config_option_update` because we can't reconstruct the full model list from the event alone. The model config option is populated from `/state` at session creation and when `set_session_config_option` is called.
