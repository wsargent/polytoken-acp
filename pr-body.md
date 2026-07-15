## Summary

Fixes session history loss when Paseo (or any ACP client) bounces/restarts. The conversation timeline is now preserved by implementing `session/load` with history replay.

### Root cause

When Paseo bounces, it calls `session/load` (if `loadSession: true`) or `session/resume` (if `loadSession: false`). polytoken-acp advertised `loadSession: false`, so Paseo always took the resume path — which provides modes/config but **no timeline history**. Paseo has no on-disk timeline store, so the conversation was lost.

The polytoken daemon already persisted history to `log.jsonl` and exposed it via `GET /history` with typed `KnownSessionHistoryItem` variants. polytoken-acp just never fetched or replayed it.

Additionally, a **stale `startup.json` race** caused daemon resume to fail entirely: `poll_startup` read the old file (with the dead daemon's port) before the new daemon could overwrite it.

### Changes

**Stale `startup.json` fix** (`src/daemon.rs`):
- `spawn_with_session_id` now deletes any stale `startup.json` before spawning the child process, forcing `poll_startup` to wait for the new daemon's file.

**Resume state notifications** (`src/agent.rs`):
- `handle_resume_session` now pushes `session_info_update`, `available_commands_update`, and `Plan` notifications — matching `handle_new_session` behavior so resumed sessions aren't a blank slate.

**`session/load` with history replay** (`src/agent.rs`, `src/history.rs`):
- Flipped `load_session(false)` -> `load_session(true)` in the initialize handler.
- Added `handle_load_session` handler that spawns the daemon with `--resume`, fetches history from `GET /history`, translates each item into ACP `SessionNotification` updates, and sends them before returning the `LoadSessionResponse` with modes and config_options.
- Added `fetch_history_raw` standalone function matching the `fetch_daemon_state_raw` pattern.
- Added `src/history.rs` module that translates daemon `KnownSessionHistoryItem` variants to ACP `SessionUpdate`:
  - `user` -> `UserMessageChunk`
  - `assistant` -> `AgentMessageChunk` + `ToolCall` + `AgentThoughtChunk` (iterating content blocks)
  - `tool_result` -> `ToolCallUpdate` (completed/failed)
  - `facet_switch` -> `CurrentModeUpdate`
  - 8 internal types silently skipped

**Module wiring** (`src/events.rs`, `src/main.rs`):
- Made `tool_kind_for_name` and `extract_locations` `pub(crate)` in `events.rs` so `history.rs` can reuse them.
- Added `mod history;` to `main.rs`.

**Tests** (`tests/smoke.rs`):
- Updated `test_initialize_capabilities` to assert `loadSession == true`.
- Added `read_response_with_notifications` method to `AcpClient` to collect notifications while waiting for the response.
- Added `test_session_load` smoke test (ignored -- requires daemon).
- Added 7 unit tests for history translation in `src/history.rs`.

### Test results

- 91 unit tests pass (including 7 new history translation tests)
- `test_initialize_capabilities` smoke test passes
- 6 ignored smoke tests require a real daemon
- Implementation reviewed by subagent: APPROVED -- no blocking issues

### Backward compatibility

`session/resume` is kept working as a fallback for clients that prefer it. Paseo only calls `session/load` when `loadSession: true`.

### Manual smoke test

```sh
cargo test --test smoke -- --ignored test_session_load
```
