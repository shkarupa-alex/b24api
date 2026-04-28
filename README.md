# API client for Bitrix24

Low-level API client with multiple strategies for lists gathering.
All methods support retries.

## Regular call (any method)

```python
import asyncio

from b24api import Bitrix24


async def main():
    async with Bitrix24() as b24:
        result = await b24.call({"method": "user.access", "parameters": {"ACCESS": ["G2", "AU"]}})
        print(result)


asyncio.run(main())
```

## Batch call (any method, no limit)

```python
import asyncio

from b24api import Bitrix24


async def main():
    async with Bitrix24() as b24:
        requests = ({"method": "user.update", "parameters": {"ID": u, "UF_SKYPE": ""}} for u in range(1000))
        async for result in b24.batch(requests):
            print(result)


asyncio.run(main())
```

## Gathering full list (slow)
Applicable to list methods with `start=<offset>` support. 
Fetches list chunks one by one.

```python
import asyncio

from b24api import Bitrix24


async def main():
    async with Bitrix24() as b24:
        async for item in b24.list_sequential({"method": "user.get"}):
            print(item)


asyncio.run(main())
```

## Gathering full list (faster)
Applicable to list methods with `start=<offset>` support. 
Fetches first list chunk with regular call, then fetches other chunks with `batch`.
Approximately 3 times faster then `list_sequential`.

```python
import asyncio

from b24api import Bitrix24


async def main():
    async with Bitrix24() as b24:
        async for item in b24.list_batched({"method": "user.get"}):
            print(item)


asyncio.run(main())
```

## Gathering full list (fastest)
Applicable to list methods with `filter={<parameters>}` support. 
Fetches first and last list chunk with batch call, then fetches other chunks with `batch`. Doesn't use counting (`start=-1`).
Approximately 2 times faster then `list_batched`.

```python
import asyncio

from b24api import Bitrix24


async def main():
    async with Bitrix24() as b24:
        async for item in b24.list_batched_no_count({"method": "user.get"}):
            print(item)


asyncio.run(main())
```

## Gathering full list with required reference
Applicable to list methods with `select=[<fields>]` and `filter={<parameters>}` support and required filter parameters. 
Fetches first and last list chunk with batch call, then fetches other chunks with `batch`. Doesn't use counting (`start=-1`).

```python
import asyncio

from b24api import Bitrix24


async def main():
    async with Bitrix24() as b24:
        deal_ids = [1, 2, 3]  # deals IDs (e.g. from "crm.deal.list" call)
        filter_updates = ({"=ENTITY_ID": i} for i in deal_ids)
        items = b24.reference_batched_no_count(
            {"method": "crm.timeline.comment.list", "parameters": {"filter": {"ENTITY_TYPE": "deal"}}},
            filter_updates,
        )
        async for item in items:
            print(item)


asyncio.run(main())
```

## Gathering full list with cursor pagination per reference
Applicable to list methods that paginate via a dedicated top-level cursor parameter (`LAST_ID`, `FIRST_ID`) per reference instead of the standard `>ID` filter — e.g. most `im.*` list methods. Pagination within a single reference is strictly sequential; each round-trip fires up to `batch_size` requests in parallel, mixing new first-page requests with in-flight continuations as the FIFO queue rotates (so continuations may be served before later-arriving first-pages).

```python
import asyncio

from b24api import Bitrix24


async def main():
    async with Bitrix24() as b24:
        dialog_ids = ["chat1", "chat2", "chat3"]
        references = ({"DIALOG_ID": d} for d in dialog_ids)
        items = b24.reference_cursor_no_count(
            {"method": "im.dialog.messages.get", "parameters": {}},
            references,
            cursor_param="LAST_ID",   # name of the cursor parameter the API accepts
            cursor_field="id",         # field in each item that holds its id
            cursor_take="min",         # "min" = older messages direction; "max" = newer
            result_key="messages",     # required for multi-key responses
        )
        async for item in items:
            print(item)


asyncio.run(main())
```
