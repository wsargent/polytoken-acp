## Summary

Fixes a session crash when a second prompt arrives while a turn is still
active. Previously, concurrent prompts were forwarded directly to the
daemon with no guard — the second prompt would fail and propagate as a
generic `internal_error`, destabilizing the ACP session permanently.

## Changes

### Per-session prompt queue (`src/agent.rs`)

- **`AgentState`** now holds a `prompt_queues: HashMap<String, Sender<PromptQueueItem>>`
  alongside the existing `sessions` map.
- **`spawn_prompt_processor`** — called at session creation (new, resume,
  load) — spawns a per-session processor task that serializes prompt
  execution via a `tokio::sync::mpsc::channel(1)`.
- **`handle_prompt`** — rewritten to enqueue via `try_send` instead of
  directly calling `prompt_with` + spawning `SseConsumer`. Behavior:
  - 1st prompt → queued, processor runs the turn
  - 2nd prompt (while turn active) → buffered, waits for current turn
  - 3rd prompt → `Full` → structured error response
- **`run_prompt_turn`** — extracted helper that sends the prompt to the
  daemon and awaits `SseConsumer::run()` to completion (no spawn — the
  processor task serializes by blocking).
- **`handle_close_session`** + **`AgentState::shutdown`** — remove queue
  senders so processor tasks exit cleanly on session teardown.

### HTTP error handling (`src/daemon.rs`)

- **`prompt_with`** now checks `resp.status()` before attempting JSON
  parse, returning a clear error message with the response body instead
  of relying on JSON parse failure.

### Tests

3 new `#[tokio::test]` tests verifying channel serialization, closed-sender
behavior, and Full rejection with item recovery. All 111 tests pass, clippy
clean.
