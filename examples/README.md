# Offline API recipes

Run each recipe from the repository root with `uv run python -m examples.<module>`.
Every offline fixture uses the public `b24api.testing.ScriptedTransport`; an
unlisted request fails instead of returning an empty success. The examples use
the public client and a dummy `fixture.invalid` host, so they make no network
requests.

| Scenario | Recipe | Status | Offline oracle |
| --- | --- | --- | --- |
| 1 | `chat_bounded_mirror` | supported offline | Three chats, 11 exact IDs, two per-binding stops and one natural exhaustion |
| 2 | `message_cursor_direction` | supported offline | FIRST_ID false clean end versus LAST_ID 5/5 at limits 1, 3 and 50 |
| 3 | `recent_dialogs` | supported offline | Envelope next, total=-1, five emitted rows, duplicate warning and four keyed chats |
| 4 | `search_chat_messages` | supported offline | Two-page chat, empty chat, typed ACCESS_ERROR and three correlated terminals |
| 5 | `task_role_union` | supported offline | Four role traversals, 932 exact IDs, 900/932 repeat hazard and aligned 950 empty page |
| 7 | `chat_resume` | supported offline | SQLite page commit, global early close, one-row overlap, eight exact keyed IDs |
| 8 | `crm_item_delta` | supported offline | Discovered smart-process type, edited old ID and new ID reconciled by time-window replay |
| 9 | `calendar_delta` | supported offline | Inclusive timezone border, recurring tombstones, atomic keyed deletion and checkpoint |
| 10 | `timeline_comments` | supported offline | Two-page deal, shared contact comment, denied lead, ignored >ID baseline |
| 11 | `sparse_user_search` | supported offline | Mapping values at 0/100 despite selected-empty 50; raw bound 150 |
| 12 | `page_index_members` | supported offline | Both one-based controls advance 1/2/3 and return 13 exact user IDs |
| 13 | `elapsed_task_items` | supported offline | Five exact JSON slots, task-scoped 50/3/empty pages and 53 IDs |
| 14 | `requisite_links` | supported offline | 582 composite keys, exact direct/batch windows, truncated 400-row tail rejected |
| 15 | `disk_mirror` | supported offline | Two roots, four counted folders, 53-row batch tail, link deduplication and cycle guard |
| 16 | `binary_download` | supported offline | Exact PDF/file bytes and media types; denied JSON remains `ApiResponseError` |
| 17 | `numerator_list` | supported offline | Fixed offsets 0/50/100; 53 IDs despite page-local totals |
| 18 | `content_viewers` | supported offline | Direct nested `params`, paged `items`, ordinary user mapping and guarded empty success |
| 19 | `v3_task_results` | supported offline | Exact task result ID, object-valued validation error, unchanged failed-parent checkpoint |

The remaining scenarios in `spec/2026-09-22-b24api-issues-architecture/examples-contracts.md`
are still under implementation. An offline pass does not assert that a live
portal has the same method semantics; those claims require the specified
opt-in disposable-portal fixture.
