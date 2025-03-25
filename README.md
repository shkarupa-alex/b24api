# API client for Bitrix24

Low-level API client with multiple strategies for lists gathering.
All methods support retries.

## Regular call (any method)
```python
from b24api import Bitrix24

b24 = Bitrix24()
result = b24.call({"method": "user.access", "parameters": {"ACCESS": ["G2", "AU"]}})
print(result)
```

## Batch call (any method, no limit)
```python
from b24api import Bitrix24

b24 = Bitrix24()

requests = ({"method": "user.update", "parameters": {"ID": u, "UF_SKYPE": ""}} for u in range(1000))
result = b24.batch(requests)
print(result)
```

## Gathering full list (slow)
Applicable to list methods with `start=<offset>` support. 
Fetches list chunks one by one.

```python
from b24api import Bitrix24

b24 = Bitrix24()
result = b24.list_sequential({"method": "user.get"})
print(list(result))
```

## Gathering full list (faster)
Applicable to list methods with `start=<offset>` support. 
Fetches first list chunk with regular call, then fetches other chunks with `batch`.
Approximately 3 times faster then `list_sequential`. 

```python
from b24api import Bitrix24

b24 = Bitrix24()
result = b24.list_batched({"method": "user.get"})
print(list(result))
```

## Gathering full list (fastest)
Applicable to list methods with `filter={<parameters>}` support. 
Fetches first and last list chunk with batch call, then fetches other chunks with `batch`. Doesn't use counting (`start=-1`).
Approximately 2 times faster then `list_batched`. 

```python
from b24api import Bitrix24

b24 = Bitrix24()
result = b24.list_batched_no_count({"method": "user.get"})
print(list(result))
```

## Gathering full list with required reference
Applicable to list methods with `select=[<fields>]` and `filter={<parameters>}` support and required filter parameters. 
Fetches first and last list chunk with batch call, then fetches other chunks with `batch`. Doesn't use counting (`start=-1`).

```python
from b24api import Bitrix24

b24 = Bitrix24()
deal_ids = [1, 2, 3]  # deals IDs (e.g. from "crm.deal.list" call)
filter_updates = ({"=ENTITY_ID": i} for i in deal_ids)
result = b24.reference_batched_no_count(
    {"method": "crm.timeline.comment.list", "parameters": {"ENTITY_TYPE": "deal"}},
    filter_updates,
)
print(list(result))
```
