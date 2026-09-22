# Offline API recipes

Run each recipe from the repository root with `uv run python -m examples.<module>`.
Every offline fixture uses the public `b24api.testing.ScriptedTransport`; an
unlisted request fails instead of returning an empty success. The examples use
the public client and a dummy `fixture.invalid` host, so they make no network
requests.

| Scenario | Recipe | Status | Offline oracle |
| --- | --- | --- | --- |
| 1 | `chat_bounded_mirror` | supported offline | Three chats, 11 exact IDs, two per-binding stops and one natural exhaustion |
| 7 | `chat_resume` | supported offline | SQLite page commit, global early close, one-row overlap, eight exact keyed IDs |
| 16 | `binary_download` | supported offline | Exact PDF/file bytes and media types; denied JSON remains `ApiResponseError` |
| 19 | `v3_task_results` | supported offline | Exact task result ID, object-valued validation error, unchanged failed-parent checkpoint |

The remaining scenarios in `spec/2026-09-22-b24api-issues-architecture/examples-contracts.md`
are still under implementation. An offline pass does not assert that a live
portal has the same method semantics; those claims require the specified
opt-in disposable-portal fixture.
