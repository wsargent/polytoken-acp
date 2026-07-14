# Polytoken ACP Extension Methods

This document describes the ACP extension methods that `polytoken-acp` sends to the ACP client (e.g. Paseo), their JSON schemas, and how to implement handlers for them.

## ACP Extension Methods (Protocol)

ACP is an [extensible protocol](https://agentclientprotocol.com/protocol/v1/extensibility). Any JSON-RPC method whose name **begins with an underscore** (`_`) is a vendor-specific extension:

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

Sent when the Polytoken daemon emits an `ask_user_question` interrogative event. This is a **request** — it expects a response containing the user's answers.

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

Sent when the Polytoken daemon emits a `system_reminder` event. This is a **notification** — one-way, no response expected.

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

Paseo's base ACP client (`packages/server/src/server/agent/providers/acp-agent.ts`) handles extension methods generically via the `ACPExtensionCommandsParser` type:

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

Kiro (`packages/server/src/server/agent/providers/kiro-acp-agent.ts`) registers a parser for `_kiro.dev/commands/available`:

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

## Summary

| Method | Type | Direction | Expects response? | Status |
|--------|------|-----------|-------------------|--------|
| `_polytoken/ask_user_question` | ext request | daemon → client → daemon | Yes (answers array) | ✅ On main |
| `_polytoken/system_reminder` | ext notification | daemon → client | No | 🔀 PR #17 |
