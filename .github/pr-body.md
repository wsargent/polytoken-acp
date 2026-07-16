## Summary

Fixes #51.

When polytoken-acp sends a `SessionUpdate::Plan`, Paseo renders it as a collapsible `TodoListCard`. The collapsed view shows the label `"Tasks"` and a secondary label derived from the **first incomplete task's title** (`items.find(i => !i.completed)?.text`). With only the first task title visible — no count, no badge — a user scanning the timeline sees what looks like a single task, not a multi-step plan.

This is a UX/discoverability issue, not a functional bug. The task list works correctly once expanded; the problem is that nothing signals there are more items hidden inside.

## Fix

Modify `build_plan_from_state` to append a position/count suffix `(N of M)` to each `PlanEntry`'s `content` when there are 2+ todos. For example:

| Before | After |
|--------|-------|
| `Investigate the bug` | `Investigate the bug (1 of 4)` |
| `Write the fix` | `Write the fix (2 of 4)` |
| `Add tests` | `Add tests (3 of 4)` |
| `Update docs` | `Update docs (4 of 4)` |

Single-task lists are unchanged (no suffix).

**Effect:** The collapsed secondary label now shows `"Investigate the bug (1 of 4)"` instead of just `"Investigate the bug"`, immediately signaling the total count and encouraging expansion.

## Constraint

Only `polytoken-acp` can be modified — Paseo and the Polytoken daemon are off-limits. The Paseo `TodoListCard` derives its collapsed secondary label from the first incomplete `PlanEntry.content`, so the only lever available is the entry content itself.

## Tests

- Updated `test_build_plan_from_state` assertions for the new format
- Added `test_build_plan_from_state_single` — verifies single tasks get no suffix
- All 135 unit tests pass
