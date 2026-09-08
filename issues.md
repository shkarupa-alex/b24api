# Client issues found while validating method examples

This file records client limitations discovered against real Bitrix24 endpoints. Reproduction evidence belongs here even when an immediate compatibility fix is available, so future releases retain the context.

## Resolved locally

### Terminal `next: -1` was rejected

`mobile.disk.folder.getchildren` returns HTTP 200 with an empty result and `next: -1` after the final `>ID` keyset boundary. The decoder rejected that response with `ProtocolError` because the canonical `Response` allowed only a non-negative `next`. This made exact empty-page confirmation impossible even though every preceding direct request succeeded.

The decoder now normalizes the server's `-1` terminal sentinel to canonical `None`; values below `-1` remain invalid. A regression test covers the observed envelope.

## Open design gaps

### HTTP 504 is not treated as ambiguous for unsafe requests

An owned `tasks.task.add` request with `ReplaySafety.UNSAFE` raised
`HTTPGatewayError: HTTP gateway error 504`. The verifier therefore never
received the create result and its cleanup sentinel remained `None`, but a
subsequent exact-title lookup found that the server had created task `3795`.
The task was removed manually and a second lookup confirmed no remaining
fixture.

This status has the same replay uncertainty as a transport interruption: an
unsafe caller cannot know whether the business effect occurred. The client
should surface an ambiguity-bearing exception (or otherwise expose the
possibly-executed state) for 504 responses instead of a definite
`HTTPGatewayError`, so callers do not mistake the failure for proof that no
effect happened. Automatic replay must remain forbidden.

### Method-specific form body encoding cannot be expressed by `Request`

`main.numerator.save` reads PHP-style nested fields from an
`application/x-www-form-urlencoded` request. Sending the same field names
through the public `Request`/`call()` path uses JSON; the handler does not
receive the form structure and returns an empty result instead of saving the
numerator. `Request` has no scoped body-encoding option, while reaching into
client-owned `_settings` and `_http` attributes would make the example depend
on private implementation details.

A future public transport option could allow an explicit form body for the
small set of handlers that require it while keeping authentication parameters
and endpoint construction client-owned. Until then the documentation uses a
safe empty headline call to prove route reachability and records a migration
gap rather than publishing a fabricated successful save response.

The set of affected handlers is not small. `Bitrix\Intranet\Controller\LeftMenu`
alone reads `$_POST` directly in fifteen actions (`addSelfItem`,
`addStandartItem`, `updateSelfItem`, `updateStandartItem`, `deleteStandartItem`,
`addItemToAll`, `deleteItemFromAll`, `deleteCustomItemFromAll`, `saveItemsSort`,
`setPreset`, `saveCustomPreset`, `setFirstPage`, `setGroupFilter`,
`addToFavorites`, `removeFromFavorites`), and the same pattern occurs in other
modules. PHP populates `$_POST` only for form bodies, so every one of these
actions is unreachable through `call()`.

The same request proves it end to end. With
`Content-Type: application/x-www-form-urlencoded` and `itemData[text]` /
`itemData[link]`, `intranet.leftmenu.addselfitem` answers
`{"result":{"itemId":3136565971}}`. With the client's
`Content-Type: application/json` and the identical parameter structure, the same
webhook answers HTTP 400 `{"error":"0","error_description":"..."}` — the handler
saw no fields at all. The failure is silent for some siblings:
`addStandartItemAction` has no phrase behind its missing-text error code, so a
JSON call stores a menu item with an empty text and an empty link and answers
`{"itemId": 0}`, which looks like a success. That item cannot be removed through
`deleteStandartItem` either, because the action derives the identifier as
`crc32($_POST['itemData']['link'])` and refuses the resulting `0`; the fixture
had to be cleared with `main.useroption.deleteoption` on
`intranet` / `left_menu_standard_items_s1`.

Both the observable failure and this silent-write variant argue for one explicit
public option on `Request` rather than per-card workarounds: without it a caller
cannot distinguish "the method rejected my data" from "the method never received
it", and a card cannot document the method's real contract at all.

### Traversal failures must distinguish client gaps from description defects

The 500-method migration exposed four initially identical-looking traversal
failures. Three were description defects, not client limitations:

- `sale.integration.statisticprovider.list` actually returns
  `statisticProviders` items with lowercase `id`; correcting the selector and
  identity made keyset traversal complete live.
- `landing.repo.getlist.json` and `landing.repowidget.getlist` require the
  keyset `filter` and `order` controls under `params`; correcting both
  `ParameterPath` values made traversal complete live.

`biconnector.table.list` could not be evaluated because the available portal
returned an embedded `ACCESS_DENIED` result before exposing a page. Its card
must not recommend keyset traversal until a permitted live run proves the
selector and advancing boundary. This is an access blocker, not evidence for
changing the client.

`task.getlist` is a genuine shape incompatibility for the generic traversal
selectors: its legacy `DATA` collection is an ID-keyed mapping (and may become
an empty list at termination), while list traversal expects a sequence. The
card therefore uses a guarded application-owned `>ID` loop. If mapping-backed
collections become a recurring contract, a future selector mode could yield
mapping values while preserving identity, duplicate, and completion proofs.

### Method-specific HTTP headers cannot be expressed by `Request`

`baas.serverport.lead.verificationack` reads the required
`X-Domain-Ack` and `X-Domain-Syn` values directly from HTTP headers. Neither
`Request` nor the public `call()`/`call_response()` operations provide a scoped
way to attach method-specific headers, so the client can reach the handler but
cannot make a successful request. Supplying these values as REST parameters is
not equivalent: the handler does not read them from the request body.

A future transport extension could accept an explicit, allowlisted header map
on a request without exposing or overriding client-owned authentication and
protocol headers. Until then this method remains a documented migration gap;
examples must not fabricate a successful payload or recommend an unrelated
REST method.

The same gap is reproduced by `crm.site.fileuploader.upload`. Its handler
rejects an otherwise valid REST call with
`invalid_content_name: X-Upload-Content-Name header is required`. Supplying a
similarly named body parameter does not satisfy the upload protocol. This is a
second, independent endpoint showing that scoped request headers are needed for
more than the acknowledgement API.

### Sequential offset cannot express a fixed server step

Some endpoints return fewer than their fixed server page size while requiring
the next `start` to advance by that fixed size. For example,
`booking.v1.resourcetype.list` returned 19 rows, ignored `start=19`, and
terminated only at `start=50`. `iter_list()` advances from the observed row
count when the server omits `next`, so it correctly detects the repeated page
but cannot complete this endpoint.

The documentation currently uses an explicit `call_response()` loop with the
proven fixed step. A future `OffsetSpec` could expose an explicit step distinct
from the local decoded-page cap; it must never infer that step from a method
name or one short fixture.

The same limitation is reproduced by `log.blogpost.get`: live pages at
`start=0` and `start=50` contained 50 and 19 rows with exact `total=69`.
The terminal page had `next=null`, but `iter_list()` advanced by its observed
19 rows to `start=69`; the endpoint repeated the terminal page and traversal
ended with `IncompleteTraversalError` after all 69 unique rows had already
been emitted. An explicit fixed-step loop completes and verifies the total.

`sale.businessvaluepersondomain.list` reproduces the short-first-page variant:
the portal returned one row with exact `total=1`, repeated that row for
`start=1`, `2`, and `49`, and returned the terminal empty page only at
`start=50`. Generic traversal therefore detects the repeat, while the
documented fixed-step loop completes and checks the composite identity.

The same short-first-page behavior is confirmed for six Sale controllers:
`sale.persontype.list`, `sale.property.list`, `sale.propertyrelation.list`,
`sale.status.list`, `sale.statuslang.list`, and `sale.tradeplatform.list`.
Their first pages contained 2–30 rows with an exact matching `total`; advancing
by that observed length repeated the page and made `iter_list()` terminate with
`IncompleteTraversalError`, while `start=50` returned the empty terminal page.
Each documented workaround advances by 50 and verifies both total and identity.

`socialnetwork.workgroup.list` reproduces the same fixed-step mismatch through
its `workgroups` wrapper. The live first page contained 20 rows with
`total=20` and `next=null`; `start=20` repeated that page, while `start=50`
returned an empty terminal page. Generic `iter_list()` therefore raises
`IncompleteTraversalError` after the repeated fingerprint instead of using
the exact total to recognize that the first page was already complete.

`timeman.worktimerecord.list` confirms the short-terminal-page form through
its `WORKTIME_RECORDS` wrapper: starts `0` and `50` returned 50 and 16 distinct
rows with exact `total=66`, but generic `iter_list()` advanced by 16 and ended
with `IncompleteTraversalError`. The documented direct loop advances by the
proven fixed step and verifies IDs and the exact total.

`userfieldconfig.list` reproduces the short-first-page form: the CRM module
returned 17 rows with exact `total=17`, generic `iter_list()` advanced to
`start=17` and detected the repeated page, and the fixed-step direct loop
completed at the server step of 50.

`socialnetwork.usertogroup.list` adds an inconsistent-total variant. With a
live group filter, `start=0` returned one relation and `total=1`; `start=1`
and `start=2` repeated the same relation, while `start=50` returned an empty
page but changed `total` to `50`. Generic `iter_list()` emitted the unique row
and ended with `PaginationError`. The documented custom loop preserves the
head total and advances by the proven server step of 50.

Five legacy Tasks list routes reproduce the same fixed-step contract at a much
larger scale: `task.ctaskitem.getlist`, `task.ctaskitem.list`,
`task.item.getlist`, `task.item.list`, and `tasks.task.list`. Their final pages
contained 30/32 rows at `start=900`; advancing by the observed length to
`start=930`/`932` repeated that page, while `start=950` returned the empty
terminal page. `iter_list()` emitted every unique row and then ended with
`IncompleteTraversalError` / `PaginationError`. The four flat legacy aliases
also reported `total=932` while exposing only 930 unique rows, so counted
traversal cannot repair them; the documented loops advance by the proven fixed
step and reject repeated IDs.

### Offset traversal cannot normalize a relative `next`

`mobile.intranet.departments.get` accepts absolute `start` offsets and an
explicit `LIMIT`, and returns an exact `total`, but its transport `next` is the
size of the current page rather than the next absolute offset. Live calls with
`LIMIT=10` returned page lengths `10, 10, 10, 5` at starts `0, 10, 20, 40`,
all with `total=45`, while their `next` values were `10, 10, 10, 5`.
`start=50` returned an empty page with `next=0`.

`iter_list()` therefore repeats the second page and terminates with
`IncompleteTraversalError`. `iter_list_counted()` also rejects the batch tail
with `CapabilityError: parallel counted continuation contradicts the planned
exact range`, including when `OffsetSpec.limit_path` points to `LIMIT`.

The method documentation uses an explicit `call_response()` loop that advances
by the decoded page length and verifies unique IDs and the final count against
`total`. A future offset contract could accept an explicit, opt-in continuation
normalizer (or an option to ignore a known-bad `next` while exact-total
invariants are enforced). It must remain endpoint-specific: interpreting every
small `next` as a relative delta would corrupt normal REST pagination.

### Keyset cannot express split flat sort controls

`KeysetSpec` models ordering as a path to a mapping and appends `IdentitySpec.order_key`, producing shapes such as `order[ID]=ASC`. Some endpoints, including `voximplant.sip.get`, instead require two flat controls: `SORT=CONFIG_ID` and `ORDER=ASC`. Their `>CONFIG_ID` filter is usable, but the current generic keyset operation cannot express their ordering contract.

Until the contract supports separate sort-field and sort-direction paths, these endpoints must use stable sequential offset traversal or an application-owned keyset loop.

### Method-level error payloads are indistinguishable from business results

Some REST handlers return application errors inside a successful transport result, for example `{"error":{"error":"ACCESS_DENIED",...}}`; newer controller actions may return a list of `{message, code, customData}` error objects. `call()` correctly preserves `Response.result`, but the generic client has no endpoint-specific knowledge with which to classify those values as failures.

Documentation verification therefore needs an explicit payload-error gate. A future client extension could offer an opt-in result validator on `Request` without inferring error shapes globally.

### Counted batch tails cannot fall back to direct offset requests

`crm.requisitelink.list` returned an exact filtered total of 582 and every
direct offset page through the end succeeded. The same request through
`iter_list_counted()` consistently stopped after 400 emitted rows, with
`PaginationError`, for physical batch sizes 5, 8, 10, and 50. This indicates an
endpoint/batch interaction at offsets starting around 400 rather than an
incorrect total or unstable direct pagination.

The documentation uses an exact-total direct offset loop so it cannot silently
truncate. A future opt-in counted strategy could fall back to direct requests
for a failed tail only when no command outcome is ambiguous and the identity/
total invariants remain provable; automatic fallback must not risk duplicate
effects for unreviewed requests.

The recovery order should be explicit and observable: retry the failed tail
with a smaller physical batch, then reduce the logical page size when the REST
method actually supports a page-size control, and finally continue with direct
sequential requests. `crm.requisitelink.list` is evidence that reducing only
the physical batch is not sufficient: sizes 50, 10, 8, and 5 all stopped at
the same 400-row boundary. The traversal report should expose which fallback
was used and retain the exact-total/identity proof; otherwise documentation
must prefer the explicit sequential loop.

### No public transport for non-JSON (binary) response bodies

Every response is decoded with `json.loads` in
`b24api/execution/executor.py` before a `Response` is constructed, and a body
that is not JSON is turned into `ProtocolError`. The public surface exported
from `b24api/__init__.py` has no counterpart that returns the raw
`WireResponse.body`, so there is no supported way to call a REST method whose
successful contract is a file stream rather than a REST envelope.

Reproduction — any of these methods answers with a file body and never reaches
a decodable envelope:

- `catalog.product.download`, `catalog.product.offer.download`, and
  `catalog.product.service.download` (the stored file's own content type)
- `crm.documentgenerator.document.download` (DOCX, `Content-Type` of the source file)
- `crm.documentgenerator.document.getpdf` (`application/pdf`)
- `crm.documentgenerator.template.download` (DOCX)
- `crm.item.import.downloadexample` (CSV)
- `crm.item.getfile` (the stored file's own content type)
- `documentgenerator.document.getpdf` (`application/pdf`)
- `user.userfield.file.get` (handler streams the stored user-field file through
  `BFile` rather than returning a JSON envelope)

For `documentgenerator.template.download`, the handler-provided signed
`downloadMachine` URL returns the expected ZIP/DOCX bytes, confirming that the
REST call reached a successful binary response rather than an application
error.

Reproduced directly against the test portal:

```python
settings = json.dumps({"entityTypeId": 2, "encoding": "UTF-8", "delimiter": "semicolon"})
await api.call(Request("crm.item.import.downloadexample",
                       {"entityTypeId": 2, "importSettingsJson": settings},
                       replay_safety=ReplaySafety.UNKNOWN))
# b24api.errors.ProtocolError: Malformed JSON response
```

The request itself is accepted — an incomplete parameter set answers with a
normal `ApiResponseError [100]`, so the failure above is the decoder meeting a
CSV body, not a rejected call.

Documentation for these methods therefore cannot show a real `call()` result.
They are recorded as `example_response_status=migration_gap` with
`verification_blocker=binary_response_transport_unsupported` and point the
reader at the `downloadUrlMachine` / `pdfUrlMachine` link from the
corresponding `.get`/`.list` method, fetched with a plain HTTP client.

An opt-in binary path — for example a `call_bytes(Request)` that skips JSON
decoding and returns the body together with the response content type — would
let these endpoints be documented against the client instead of around it. It
must stay explicit: silently returning bytes whenever decoding fails would hide
genuine `ProtocolError` cases behind an unparsed payload.

### No public request encoding for form/query-only REST actions

`Request` parameters are sent through the ordinary JSON REST transport. The
public client API has no explicit form-encoded or query-parameter transport for
registered actions that read raw HTTP request fields instead of declaring
REST-bindable action parameters.

`socialnetwork.workgroup.creategroup` reproduces this mismatch. Its controller
declares no action parameters and reads `groupName`, `viewMode`, and
`avatarColor` through `getRequest()->get()`. A current-client call with all
three documented parameters therefore reaches the action with `null` values and
fails before creating a group:

```python
await api.call(
    Request(
        "socialnetwork.workgroup.creategroup",
        {
            "groupName": "B24API transport probe",
            "viewMode": "closed",
            "avatarColor": "29AD49",
        },
        ReplaySafety.UNSAFE,
    )
)
# ApiResponseError: getColoredDefaultAvatar(): Argument #1 ($color)
# must be of type string, null given
```

The same diagnostic was reproduced twice on 2026-09-02. PHP inspection shows
that the failure occurs before `CSocNetGroup` creation. A separate form-encoded
HTTP request can populate these raw fields, but using another HTTP library in a
b24api example would bypass the client rather than migrate the example.

A future explicit request-encoding option could support this narrow endpoint
class. It must be opt-in and visible in the request contract; silently retrying
failed JSON calls as form requests would be unsafe for mutations and could
duplicate business effects after ambiguous failures.

### No public transport for signed-URL multipart uploads

The registered `upload` route is the second half of the Disk upload protocol,
not an ordinary method call. A preceding `disk.folder.uploadfile` or equivalent
operation issues a one-time URL, query token, and multipart field name; the
caller must then POST the binary body to that arbitrary signed URL.

`Request` can target only the configured REST endpoint and express JSON
parameters. It cannot direct one call to the issued URL or attach a multipart
file part, so the signed upload route cannot be completed through the public
client API. A plain current-client probe reproducibly reaches the route but is
rejected before upload:

```python
await api.call(Request("upload", replay_safety=ReplaySafety.UNSAFE))
# b24api.errors.ApiResponseError:
# API error [access_denied]: Access denied! Link check failed
```

This is distinct from the binary-response issue: here the missing capability is
request-side URL and multipart encoding. It is also narrower than ordinary
form/query actions because the destination and token are returned dynamically
by the portal. A future explicit signed-upload operation could accept the
issued URL, server-provided field name, filename, media type, and bytes while
keeping redirects, authentication forwarding, size limits, and replay safety
under client control.

### An identity coercion mismatch is reported without the row that broke it

`documentgenerator.document.list` returns `id` as the string `"395"`. A keyset
traversal declaring `IdentityCoercion.EXACT_INTEGER` therefore admits nothing,
and the caller receives:

```
b24api.errors.IncompleteTraversalError: Traversal did not complete
```

The chained cause carries the rule — `PaginationError: identity must be an
exact integer` — but neither message names the offending value, the
`item_path` it was read from, nor the method, so a fifty-row page gives no clue
which field is at fault. Any tool that records only the last traceback line
(the example verifier does) keeps the generic sentence and loses the reason
entirely.

`OperationReport` is also silent about the rejection: `admitted=0` with
`violations=()`, although a row was refused by an identity rule. A report that
counts nothing admitted and no violation cannot be distinguished from a page
that legitimately contained no rows.

The coercion failure should name the value, its path and the mode
(`identity at ('id',) must be an exact integer, got '395'`), and the rejection
should appear in `report.violations`.

### The rendered message hides the error code the portal actually sent

`ApiResponseError.__init__` keeps the wire value in `original_code` but renders
the message from the lowercased `code`:

```python
self.original_code = code
self.code = str(code).lower()
message = f"API error [{self.code}]: {safe_description}"
```

The REST module never lowercases: `RestException::output()` returns
`getErrorCode()` unchanged, so `Bitrix\Rest\AccessException` answers
`ACCESS_DENIED` and `Bitrix\Rest\AuthTypeException` answers `WRONG_AUTH_TYPE`.
A caller therefore reads

```
b24api.errors.ApiResponseError:
# API error [access_denied]: Access denied! Application context required
```

for a portal answer of `ACCESS_DENIED`, and the documented spelling is the one
that disappeared. `original_code` is reachable from `to_safe_dict()`, but the
string form is what people copy into an issue, a log line or a document — this
corpus recorded `access_denied` in `verified_error` for dozens of methods and
carried the lowercase spelling into the published error tables.

Lowercasing is right for matching (`normalized_code` already exists for that);
the rendered message should quote what arrived, for example
`API error [ACCESS_DENIED]: ...`, and mention the normalized form only when the
two differ.
