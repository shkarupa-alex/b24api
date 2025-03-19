from datetime import datetime
from urllib.parse import quote_plus

from b24api.type import ApiTypes


def build_query(parameters: dict[int | str, ApiTypes], path: str = "%s") -> str:
    query = []

    if parameters is None:
        return ""

    for key, value in parameters.items():
        if value is None:
            continue

        value_ = value

        if isinstance(value_, list | tuple):
            value_ = dict(enumerate(value))

        if isinstance(value_, dict):
            subquery = build_query(value_, path % key + "[%s]")
        else:
            key_ = quote_plus(path % key)

            if isinstance(value_, datetime):
                value_ = value.astimezone().isoformat()
            value_ = quote_plus(str(value_))

            subquery = f"{key_}={value_}"

        if subquery:
            query.append(subquery)

    return "&".join(query)
