# MCP Server Forwarding

When an ACP client (e.g. Paseo) sends MCP servers in `session/new`, `session/resume`, or `session/load`, `polytoken-acp` forwards them to the Polytoken daemon so the agent can use their tools alongside the daemon's built-in tools.

This document describes how the forwarding works, which transports are supported, and how it interacts with the daemon's [MCP servers configuration](https://docs.polytoken.dev/reference/configuration/#mcp_servers).

## Overview

```
Paseo                          polytoken-acp                         polytoken daemon
 │                                    │                                      │
 │  session/new                        │                                      │
 │  { mcp_servers: [...] }             │                                      │
 │ ──────────────────────────────────> │                                      │
 │                                     │                                      │
 │                                     │  1. Convert ACP McpServer variants  │
 │                                     │     to polytoken config YAML         │
 │                                     │                                      │
 │                                     │  2. Write temp config.yaml with      │
 │                                     │     mcp_servers map                  │
 │                                     │                                      │
 │                                     │  3. Spawn daemon with                │
 │                                     │     --project-config-dir <temp>      │
 │                                     │ ──────────────────────────────────> │
 │                                     │                                      │
 │                                     │                                      │  4. Daemon merges
 │                                     │                                      │     project config
 │                                     │                                      │     on top of global
 │                                     │                                      │
 │                                     │                                      │  5. Daemon connects
 │                                     │                                      │     to each server
```

## How it works

### 1. Conversion to polytoken config

The shim converts each ACP `McpServer` variant into an entry in a `mcp_servers` map in polytoken's YAML config format. The result is written to a temporary directory as `config.yaml`:

```
/tmp/polytoken-acp-mcp-<random>/
  mcp-config/
    config.yaml
```

### 2. Passing to the daemon

The temp directory is passed to the daemon via `--project-config-dir`. The daemon loads it as a **project-level config layer**, merged on top of the user's global config (which lives at `~/.config/polytoken/config.yaml`). This means:

- Servers defined in the global config (e.g. `zread`, `web-reader`) are still available.
- Servers forwarded from the ACP client are added on top.
- Providers, models, and all other global config keys are inherited unchanged.

### 3. Session isolation

Each ACP session gets its own temp config directory (random suffix) and its own daemon process. There is no cross-contamination between sessions — if Session A sends servers `[foo, bar]` and Session B sends `[baz]`, each daemon only loads its own servers plus the global ones.

## Transport mapping

| ACP variant | Polytoken transport | Status |
|---|---|---|
| `Stdio` | `stdio` | Forwarded |
| `Http` | `http` (streamable-HTTP) | Forwarded |
| `Sse` | — | **Skipped** (polytoken has no SSE transport) |
| Unknown / future variants | — | **Skipped** |

The temp directory and config file are created with owner-only permissions (0700 / 0600) because HTTP server configs may contain auth tokens (e.g. bearer tokens from the `Authorization` header).

### Stdio servers

ACP `Stdio` servers map directly to polytoken's stdio transport:

| ACP field | Polytoken config field |
|---|---|
| `name` | server key under `mcp_servers` |
| `command` (PathBuf) | `command` |
| `args` (Vec&lt;String&gt;) | `args` |
| `env` (Vec&lt;{name, value}&gt;) | `env` (as a map) |

Two additional polytoken-specific settings are applied automatically:

- **`pass_env`**: Set to every environment variable name from the `polytoken-acp` process. Polytoken's default `pass_env` is restrictive (`HOME`, `PATH`, `LANG`, `TMPDIR`), but MCP servers may expect the full parent environment (e.g. `NODE_PATH`, provider keys). Explicit `env` values from the ACP request override these.
- **`log_stdout: true`**: Polytoken logs stderr by default but stdout is off by default. Enabling it makes connection issues and diagnostic output visible in per-server log files.

Example converted config:

```yaml
mcp_servers:
  my-filesystem:
    transport: stdio
    command: /usr/local/bin/npx
    args:
      - -y
      - "@modelcontextprotocol/server-filesystem"
      - /tmp
    env:
      API_KEY: secret123
    pass_env:
      - HOME
      - PATH
      - NODE_PATH
      # ... all env vars from the polytoken-acp process
    log_stdout: true
```

### HTTP servers

ACP `Http` servers map to polytoken's `http` (streamable-HTTP) transport:

| ACP field | Polytoken config field |
|---|---|
| `name` | server key under `mcp_servers` |
| `url` | `url` |
| `headers` (Vec&lt;{name, value}&gt;) | `headers` (as a map) |

**Authentication:** The `Authorization` header is extracted from the ACP `headers` list and mapped to polytoken's dedicated `auth` field (`type: authorization-header`), rather than left in the generic `headers` map. This lets polytoken handle the auth lifecycle properly. All other headers (e.g. `X-Custom-Header`) remain in `headers`.

Example converted config:

```yaml
mcp_servers:
  api-server:
    transport: http
    url: https://api.example.com/mcp
    auth:
      type: authorization-header
      value: Bearer token123
    headers:
      X-Custom-Header: custom-value
```

### SSE servers (not supported)

Polytoken has no SSE transport — only `stdio` and `http` (streamable-HTTP). SSE and streamable-HTTP are different MCP transports, so the `Sse` variant cannot be mapped to `http`. SSE servers are **skipped** with a warning logged to stderr.

If you need an SSE server, configure it in your `~/.config/polytoken/config.yaml` directly (polytoken may add SSE support in the future).

## Interaction with global config

MCP servers forwarded from the ACP client are a **project-level config layer**. They do not replace the global config — they merge on top of it. This means:

- Global MCP servers (from `~/.config/polytoken/config.yaml`) are still loaded.
- ACP-provided servers are added alongside them.
- The daemon's `/state` endpoint reports all servers — both global and project-level.
- The `mcp:<server_name>` config option appears in Paseo's config UI for every server, allowing the user to enable/disable any of them at runtime via `POST /mcp/{name}/enable` or `POST /mcp/{name}/disable`.

If a server with the same name exists in both the global config and the ACP-provided servers, the project-level entry takes precedence.

## What is not forwarded

The following polytoken `mcp_servers` config keys are not set from ACP (they are polytoken-specific and have no ACP equivalent):

- `block_list` — tool names to hide from the model
- `default_timeout_seconds` — per-server tool call timeout
- `auth_key_name` — OAuth credential directory name
- `default_enabled` — whether to connect at startup (defaults to `true`)
- `pass_env` — set automatically (see Stdio servers above), not from ACP
- `log_stderr` — polytoken default (`true`) is used
- `log_stdout` — set to `true` for stdio servers (see above)

To configure these, set them in your `~/.config/polytoken/config.yaml`.
