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
| `tool_call` | `session/update` → `tool_call` (status: pending) |
| `tool_result` | `session/update` → `tool_call_update` (status: completed/failed) |
| `interrogative` (permission) | `session/request_permission` → `POST /interrogative/{id}/respond` |
| `ask_user_question` | `ext_method` (`polytoken/ask_user_question`) → `POST /interrogative/{id}/respond` |
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
- Event-to-ACP translation (text delta, tool call, tool result, message complete, turn cancelled, interrogative)
- Permission translation (options construction, outcome→granted mapping)
- `ask_user_question` event deserialization (payload with questions, options, modes) and translation
- `ask_user_question` payload serialization for ext_method forwarding
- Content block text extraction

### Integration Tests

Integration tests require the `polytoken` binary on PATH and LLM credentials:

```bash
cargo test -- --ignored
```

## Troubleshooting

### Debug logging

The shim logs to stderr (stdout is reserved for JSON-RPC). Set the `RUST_LOG` environment variable:

```bash
RUST_LOG=debug polytoken-acp
```

### Daemon process issues

Each ACP session spawns a `polytoken daemon` process. If the daemon fails to start:

1. Check that `polytoken` is on `PATH` and `polytoken --version` works.
2. Check the shim's stderr output for error messages.
3. The daemon's temp directory is under `$TMPDIR/polytoken-acp-{session_id}/` — check `logs/` for daemon logs.

### stdout pollution

The shim's own logging goes to stderr only. The daemon child process's stdout is piped and drained. If you see JSON-RPC errors, ensure nothing else writes to stdout.

## Limitations (v1)

- **MCP server forwarding**: Paseo's `mcpServers` are acknowledged but not forwarded to the polytoken daemon. Configure MCP servers in your `~/.config/polytoken/` config instead.
- **`ask_user_question` via ext_method**: The daemon's `ask_user_question` events are forwarded to the ACP client via the `polytoken/ask_user_question` extension method. The client must implement `ext_method` and return a JSON object with an `answers` array (each answer has `question_id`, `selected_option_ids`, and/or `free_text`). If the client does not support the extension or returns no answers, the interrogative is cancelled on the daemon so the agent can proceed.
