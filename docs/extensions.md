# Polytoken ACP Extension Methods

This document describes the ACP extension methods that `polytoken-acp` sends to the ACP client (e.g. Paseo), their JSON schemas, and how to implement handlers for them.

## ACP Extension Methods (Protocol)

ACP is an [extensible protocol](https://agentclientprotocol.com/protocol/extensibility). Any JSON-RPC method whose name **begins with an underscore** (`_`) is a vendor-specific extension:

- Names **without** `_` are reserved for ACP itself (including future versions).
- Names **with** `_` create a vendor namespace, e.g. `_polytoken/ask_user_question`.

Two message types exist:

| Type | Has `id`? | Expects response? | If unrecognized |
|------|-----------|-------------------|-----------------|
| **Extension request** (`extMethod`) | Yes | Yes | Receiver returns error `-32601` (method not found) |
| **Extension notification** (`extNotification`) | No | No (one-way) | Receiver silently ignores |

## Extension Methods Defined by polytoken-acp

All extension methods use the `_polytoken/` vendor namespace.

### 1. `_polytoken/ask_user_question` (Extension Request)

Sent when the Polytoken daemon emits an `ask_user_question` interrogative event. This is a **request** — it expects a response containing the user's answers. See the [Polytool CLI reference](https://docs.polytoken.dev/reference/cli/) for `polytoken event-schema`, which prints the full `DaemonEvent` JSON Schema including interrogative shapes.

**When:** The daemon agent calls `ask_user_question` (structured questions with options, modes, etc.). This cannot be mapped to ACP's standard `session/request_permission`, so it uses an extension request instead.

**Params:**

```json
{
  "interrogative_id": "string — the daemon's interrogative ID, needed for the response",
  "questions": [
    {
      "id": "string — question identifier",
      "question": "string — the question text",
      "context": "string (optional) — markdown explainer / background",
      "mode": "single_select | multi_select | text",
      "allow_free_text": true,
      "options": [
        {
          "id": "string — option identifier",
          "label": "string — display label",
          "description": "string (optional) — what this option means",
          "justification": "string (optional) — why to choose this",
          "preview": "string (optional) — ASCII/markdown preview (single_select only)"
        }
      ]
    }
  ]
}
```

**Expected response:**

```json
{
  "answers": [
    {
      "question_id": "string — matches the question's `id`",
      "selected_option_ids": ["string"]  — present if the user picked option(s),
      "free_text": "string"               — present if the user typed a custom answer
    }
  ]
}
```

If the client does not support this extension or returns no answers, polytoken-acp cancels the interrogative on the daemon so the agent can proceed.

**Implementation in polytoken-acp:** `src/agent.rs` — `handle_ask_user_question()`.

### 2. `_polytoken/system_reminder` (Extension Notification)

Sent when the Polytoken daemon emits a `system_reminder` event. This is a **notification** — one-way, no response expected. The daemon emits these based on its [permission rules and system hooks](https://docs.polytoken.dev/reference/configuration/).

**When:** The daemon injects system reminders (e.g. repository status, repository metadata changed) into the conversation. These are informational and don't require a response.

**Params:**

```json
{
  "slug": "string — machine-readable identifier (e.g. 'repo-status')",
  "display_name": "string — human-readable name (e.g. 'Repository status')",
  "body": "string — reminder content (may be markdown)",
  "reason": "string — why the reminder was sent (e.g. 'repository_status')"
}
```

**No response expected.** The client should render this as an informational notification or inline message.

**Implementation in polytoken-acp:** `src/agent.rs` — `EventTranslation::SystemReminder` arm. *(Available in PR #17.)*

## Handling Extensions in Paseo

Paseo's base ACP client (`packages/server/src/server/agent/providers/acp-agent.ts`) handles extension methods generically via the `ACPExtensionCommandsParser` type. See the [Paseo custom providers documentation](https://paseo.sh/docs/custom-providers) for how ACP agents are configured and launched:

```typescript
export type ACPExtensionCommandsParser = (
  method: string,
  params: Record<string, unknown>,
) => AgentSlashCommand[] | null;
```

- All extension notifications are logged at trace level.
- If a provider-specific parser is registered and returns a non-null result, the parsed commands are applied.
- If no parser is registered, or the parser returns `null`, the notification is silently ignored.

### Reference Implementation: Kiro

Kiro (`packages/server/src/server/agent/providers/kiro-acp-agent.ts`) registers a parser for `_kiro.dev/commands/available`. This is the reference implementation for provider-specific extension handling, similar to how Paseo's [providers overview](https://paseo.sh/docs/providers) describes native vs ACP-tier adapter patterns:

```typescript
const KIRO_COMMANDS_AVAILABLE_METHOD = "_kiro.dev/commands/available";

export const parseKiroExtensionCommands: ACPExtensionCommandsParser = (method, params) => {
  if (method !== KIRO_COMMANDS_AVAILABLE_METHOD) {
    return null;
  }
  return mapKiroAvailableCommands(params);
};
```

### Adding a Polytoken Provider Parser

For `_polytoken/*` methods to be handled, Paseo would need a provider that registers an `extensionCommandsParser`. Since `ask_user_question` returns structured answers (not slash commands), and `system_reminder` is pure notification (not commands at all), the parser pattern may need to be extended — or the methods handled directly in `extNotification` / `extMethod`:

```typescript
// packages/server/src/server/agent/providers/polytoken-acp-agent.ts

const POLYTOKEN_ASK_METHOD = "_polytoken/ask_user_question";
const POLYTOKEN_REMINDER_METHOD = "_polytoken/system_reminder";

// Option A: Extend the parser to handle these as side-effects
// (works for system_reminder which is one-way, but ask_user_question
// needs to return a response, not slash commands)

// Option B: Override extNotification/extMethod directly in a
// Polytoken-specific ACP agent subclass
```

**Key consideration:** `_polytoken/ask_user_question` is an extension **request** (expects a response), not a notification. The `ACPExtensionCommandsParser` pattern only handles notifications and returns slash commands. Handling `ask_user_question` properly would require either:

1. Rendering a question UI in Paseo (modal with options/free-text input) and returning the selected answers, or
2. Surfacing the question as a permission-like prompt and collecting the answer.

This is a product decision for the Paseo side.

## Session Modes

ACP `SessionMode` maps to the daemon's **permission monitor**, not facets. This matches how other ACP providers (e.g. Claude Code) expose permission tiers as modes, so Paseo renders them in its built-in mode picker automatically — no custom `configFeatureOption` needed.

| Mode | Label | Description |
|---|---|---|
| `standard` | Standard | Default permission prompts |
| `bypass` | Bypass | Skip permission prompts |
| `bypass_plus` | Bypass+ | Enhanced bypass mode |
| `autonomous` | Autonomous | Autonomous classifier-based permissions |

- **Source:** `GET /permission-monitor` (response field `monitor.type`)
- **Routing:** `set_session_mode("bypass")` → `POST /permission-monitor` with `{"mode": "bypass"}`
- **Live updates:** When the daemon emits `permission_monitor_switch` (e.g. from `/permissions` in the TUI), `polytoken-acp` sends a `CurrentModeUpdate` notification so the client's mode picker reflects the new mode.

### Facet switching

Polytoken facets (`execute`, `plan`) are **not** mapped to ACP modes. Instead, `/facet` is advertised as a slash command in `available_commands_update`, so the user can switch facets by typing `/facet plan` or `/facet execute` — matching the TUI experience. The daemon handles the command directly when it appears in a prompt.

## Session Config Options

In addition to extension methods, `polytoken-acp` exposes daemon settings as standard ACP `SessionConfigOption` entries in the `session/new` and `session/resume` responses. These are not extension methods — they use the ACP session configuration protocol directly.

The available config options are:

- **`model`** (select, category `model`) — model picker from `available_models`
- **`thought_level`** (select, category `thought_level`) — reasoning effort levels
- **`mcp:<server>`** (boolean) — one toggle per MCP server

Permissions are **not** a config option — they are a session mode (see above).

## Summary

| Method | Type | Direction | Expects response? | Status |
|--------|------|-----------|-------------------|--------|
| `_polytoken/ask_user_question` | ext request | daemon → client → daemon | Yes (answers array) | ✅ On main |
| `_polytoken/system_reminder` | ext notification | daemon → client | No | 🔀 PR #17 |
| Permission modes | SessionMode | bidirectional | No (standard ACP) | ✅ Done |
| `/facet` command | AvailableCommand | client → daemon | No (prompt passthrough) | ✅ Done |

## References

### ACP (Agent Client Protocol)

- [Protocol Extensibility](https://agentclientprotocol.com/protocol/extensibility) — official spec for underscore-prefixed extension methods, requests vs notifications, and capability advertisement
- [Protocol Overview](https://agentclientprotocol.com/protocol/v1/overview) — JSON-RPC envelope, built-in extensibility mechanisms
- [Rust SDK RFD](https://agentclientprotocol.com/rfds/rust-sdk-v1) — describes the `ext_method` / `ext_notification` approach in the current SDK and the planned first-class custom method support

### Polytoken

- [Introduction](https://docs.polytoken.dev/introduction) — what Polytoken is and how the daemon works
- [CLI Reference](https://docs.polytoken.dev/reference/cli/) — `polytoken event-schema` prints the full `DaemonEvent` JSON Schema; `polytoken print-tools` prints tool documentation
- [Daemon Authentication](https://docs.polytoken.dev/reference/daemon-auth) — bearer token and HTTP daemon API
- [Application Configuration](https://docs.polytoken.dev/reference/configuration/) — provider config, permission rules, MCP block lists

### Paseo

- [Providers](https://paseo.sh/docs/providers) — mental model for native vs ACP-tier agent adapters
- [Custom Providers](https://paseo.sh/docs/custom-providers) — how to add an ACP agent via `extends: "acp"` and `command` in `~/.paseo/config.json`
- [docs/custom-providers.md on GitHub](https://github.com/getpaseo/paseo/blob/main/docs/custom-providers.md) — full field reference (`extends`, `label`, `command`, `env`, `models`, `additionalModels`, etc.)
- `packages/server/src/server/agent/providers/acp-agent.ts` — base ACP client with `extNotification()` handler and `ACPExtensionCommandsParser` type
- `packages/server/src/server/agent/providers/kiro-acp-agent.ts` — reference implementation of a provider-specific extension method parser
