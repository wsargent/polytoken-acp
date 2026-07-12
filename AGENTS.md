# polytoken-acp

An [ACP (Agent Client Protocol)](https://agentclientprotocol.com/) server shim for the [Polytoken](https://polytoken.dev) daemon.

## Repository Notes

- **Polytoken is not open source.** There is no public GitHub repository. Do not attempt to read it via GitHub tools (zread MCP, etc.).
- The `polytoken` binary is installed via Homebrew (`polytoken/tap/polytoken` or `polytoken/tap/polytoken-unstable`) and lives at `/opt/homebrew/bin/polytoken`.
- Use `polytoken --help`, `polytoken openapi`, `polytoken event-schema`, and `polytoken print-tools` to introspect the daemon's API and event shapes.
- **Paseo** is a separate codebase located at `~/work/paseo`. It can be read locally for understanding how the ACP client handles extension methods. Paseo uses the JS SDK `@agentclientprotocol/sdk`.
- This project uses ACP 1.x (`agent-client-protocol = "1.2"` with the `unstable` feature). The 1.x SDK uses a builder pattern (`Agent.builder().on_receive_request(...).connect_to(Stdio::new())`) instead of the 0.x trait-based API.
