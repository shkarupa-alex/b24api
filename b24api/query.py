from datetime import datetime
from typing import Any
from urllib.parse import quote_plus


def build_query(parameters: dict[Any, Any], path: str = "%s") -> str:
    """Serialize parameters the way PHP's `http_build_query` does.

    The Bitrix24 backend is PHP and expects nested arrays as `key[0]=...&key[sub][0]=...` rather than the flat
    or JSON-style encodings Python libraries default to. This mirrors PHP's nested-bracket layout (with lists keyed
    by index) so the server parses requests identically to PHP-originated calls.
    """
    query: list[str] = []

    for key, value in parameters.items():
        if value is None:
            continue

        value_: Any = value

        if isinstance(value_, list | tuple):
            value_ = dict(enumerate(value_))

        if isinstance(value_, dict):
            subquery = build_query(value_, path % key + "[%s]")
        else:
            key_ = quote_plus(path % key)

            if isinstance(value_, datetime):
                value_ = value_.isoformat()
            value_ = quote_plus(str(value_))

            subquery = f"{key_}={value_}"

        if subquery:
            query.append(subquery)

    return "&".join(query)
