# Offline API recipes

Run each recipe from the repository root with `uv run python -m examples.<module>`.
Every offline fixture uses the public `b24api.testing.ScriptedTransport`; an
unlisted request fails instead of returning an empty success. The examples use
the public client and a dummy `fixture.invalid` host, so they make no network
requests.

The keyset recipes keep `verify_keyset_capability()` immediately beside traversal when
`ENV != "PROD"`. Accepting an ID filter does not prove that a method honors strict bounds and
ordering. Run the default commands without `ENV`: scenarios 6 and 8 then also replay the
verifier canary exchanges from their frozen fixtures. `ENV=PROD` is the switch an application sets
only after it has qualified the method on a stable development/staging portal; it skips the canary.

Run the complete structured fixture matrix with
`uv run python -m examples.run --scenario all`. Each JSONL row contains the
scenario number, exact client SHA and external method-card SHA, measured report state and
assurance, independent expected and observed counts, physical and logical
request counts, bounded-resource high-water counters, and `provenance=fixture`.
To validate separately recorded disposable-portal results, pass
`--live-evidence path/to/evidence.jsonl`. Accepted rows must match the exact
client and method-card SHAs, independent oracle, report state, request counters,
and a non-empty disposable-fixture ID. They also carry an authenticated
`b24api-live-capture-v1` block with a timestamp, non-reversible portal
fingerprint, and one method/status/response-digest entry per physical request.
The independent recorder signs the canonical record with Ed25519; the verifier
uses the public trust anchor pinned in `examples.run`, with no caller-selectable
key. A fixture summary or caller-fabricated capture cannot be promoted by changing
labels or choosing its own key. Missing LIVE evidence exits with status 3 and a
reason; it is never reported as a passing LIVE claim. The method-card SHA is an external
reference: the commit of the separate apidocs repository whose method cards the recipes follow. It
does not resolve in this repository, and nothing here loads or verifies it.

The Offline column uses the specification's status vocabulary restricted to fixture evidence:
`passing` means the client mechanics are `supported` against the frozen fixture, and
`endpoint-limited` means the recipe deliberately ends fail-closed because the endpoint gives no
qualified closure. No scenario is `blocked by client gap`. Neither status is a LIVE claim.

| Scenario | Recipe | Offline | LIVE | Offline oracle |
| --- | --- | --- | --- | --- |
| 1 | [chat_bounded_mirror](chat_bounded_mirror.py) | passing | not recorded | Three chats, 11 exact IDs, two per-binding stops and one natural exhaustion |
| 2 | [message_cursor_direction](message_cursor_direction.py) | passing | not recorded | FIRST_ID false clean end versus LAST_ID 5/5 at limits 1, 3 and 50 |
| 3 | [recent_dialogs](recent_dialogs.py) | passing | not recorded | Envelope next, total=-1, five emitted rows, duplicate warning, `mechanics_only` assurance and four keyed chats |
| 4 | [search_chat_messages](search_chat_messages.py) | passing | not recorded | Two-page chat, empty chat, typed ACCESS_ERROR and three correlated terminals |
| 5 | [task_role_union](task_role_union.py) | passing | not recorded | Four role traversals, 932 exact IDs, qualified total closure and the 900/932 repeat hazard |
| 6 | [task_comments](task_comments.py) | passing | not recorded | Correlated TASKS_TASK batch, modern/legacy/empty/denied outcomes and exact three-slot legacy wire |
| 7 | [chat_resume](chat_resume.py) | passing | not recorded | SQLite page commit, global early close, one-row keyed overlap below the committed exclusive bound, eight exact keyed IDs |
| 8 | [crm_item_delta](crm_item_delta.py) | passing | not recorded | Discovered smart-process type, edited old ID and new ID reconciled by time-window replay |
| 9 | [calendar_delta](calendar_delta.py) | passing | not recorded | Inclusive timezone border, recurring tombstones, atomic keyed deletion and checkpoint |
| 10 | [timeline_comments](timeline_comments.py) | passing | not recorded | Two-page deal, shared contact comment, denied lead, ignored >ID baseline |
| 11 | [sparse_user_search](sparse_user_search.py) | passing | not recorded | Mapping values at 0/100 despite selected-empty 50; envelope raw total 150 |
| 12 | [page_index_members](page_index_members.py) | passing | not recorded | Both one-based controls advance 1/2/3 and return 13 exact user IDs |
| 13 | [elapsed_task_items](elapsed_task_items.py) | passing | not recorded | Five exact JSON slots, task-scoped 50/3/empty pages and 53 identity-checked IDs |
| 14 | [requisite_links](requisite_links.py) | passing | not recorded | 582 composite keys, exact direct/batch windows, truncated 400-row tail rejected |
| 15 | [disk_mirror](disk_mirror.py) | passing | not recorded | Two roots, four counted folders, 53-row batch tail, link deduplication and cycle guard |
| 16 | [binary_download](binary_download.py) | passing | not recorded | Exact PDF/file bytes and media types; denied JSON remains `ApiResponseError` |
| 17 | [numerator_list](numerator_list.py) | endpoint-limited | not recorded | Fixed offsets 0/50/100; 53 IDs observed, then fail-closed without qualified closure |
| 18 | [content_viewers](content_viewers.py) | passing | not recorded | Direct nested `params`, paged `items`, ordinary user mapping and guarded empty success |
| 19 | [v3_task_results](v3_task_results.py) | passing | not recorded | Exact task result ID, object-valued validation error, unchanged failed-parent checkpoint |

All 19 scenarios have offline recipes. An offline pass does not assert that a
live portal has the same method semantics; those claims require the specified
opt-in disposable-portal fixtures and have not been recorded here.
