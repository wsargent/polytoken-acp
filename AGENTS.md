# polytoken-acp

An [ACP (Agent Client Protocol)](https://agentclientprotocol.com/) server shim for the [Polytoken](https://polytoken.dev) daemon.

## Scope Constraint (read first)

- **Only `polytoken-acp` (this repository) may be modified.** The **Paseo** app (`~/work/paseo`) and the **Polytoken daemon** (the `polytoken` binary) are **off-limits for changes** — they may be read and introspected for understanding, but no feature may depend on modifying either.
- Practical consequence: any capability that would require a Paseo-side change (e.g. a new consumer for `_polytoken/*` extension notifications) or a daemon-side change (e.g. routing subagent spawning through Paseo's `create_agent` MCP tool) is **out of scope**. Solutions must be fully implementable within the ACP shim.
- Example: Paseo's collapsible "subagents track" is populated only by real Paseo agents created via the `create_agent` MCP tool (`relationship: subagent`). Making Polytoken subagents appear there would require either a Paseo bridge or a daemon change — both forbidden. Native Polytoken subagents already render as timeline tool calls, matching how Claude Code's native `Task` subagents render; that is the parity available to us.

## Repository Notes

- **Polytoken is not open source.** There is no public GitHub repository. Do not attempt to read it via GitHub tools (zread MCP, etc.).
- The `polytoken` binary is installed via Homebrew (`polytoken/tap/polytoken` or `polytoken/tap/polytoken-unstable`) and lives at `/opt/homebrew/bin/polytoken`.
- Use `polytoken --help`, `polytoken openapi`, `polytoken event-schema`, and `polytoken print-tools` to introspect the daemon's API and event shapes.
- **Paseo** is a separate codebase located at `~/work/paseo`. It can be read locally for understanding how the ACP client handles extension methods. Paseo uses the JS SDK `@agentclientprotocol/sdk`.
- This project uses ACP 1.x (`agent-client-protocol = "1.2"` with the `unstable` feature). The 1.x SDK uses a builder pattern (`Agent.builder().on_receive_request(...).connect_to(Stdio::new())`) instead of the 0.x trait-based API.
- **`loadSession: true`** — polytoken-acp advertises `loadSession: true` and implements `session/load`. When a client calls `session/load`, polytoken-acp spawns the daemon with `--resume`, fetches history from `GET /history`, and replays each translatable item as an ACP `SessionNotification` before returning the `LoadSessionResponse` with modes and config_options. The `session/resume` handler is kept as a fallback for clients that prefer it.
- **Stale `startup.json` fix** — `DaemonHandle::spawn_with_session_id` deletes any stale `startup.json` from a previous daemon run before spawning the child process. Without this, `poll_startup` would read the old file on its first iteration and return the dead daemon's port.

## Building & Installing

```bash
cargo build --release          # build to target/release/polytoken-acp
cargo install --path . --force # install to ~/.cargo/bin/polytoken-acp
```

### macOS: do not `cp` to install

On macOS, copying the release binary directly with `cp` (e.g. `cp target/release/polytoken-acp ~/.cargo/bin/`) results in a process that is immediately killed (exit code 137 / SIGKILL). This is caused by the `com.apple.provenance` extended attribute that macOS attaches to manually-copied binaries. Use `cargo install --path . --force` instead, which handles code signing correctly.
