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

        if isinstance(value, list | tuple):
            value = dict(enumerate(value))  # noqa: PLW2901

        if isinstance(value, dict):
            subquery = build_query(value, path % key + "[%s]")
        else:
            key_ = quote_plus(path % key)
            value_ = value.isoformat() if isinstance(value, datetime) else value
            # TODO: check date filtering [with .replace(microsecond=0).astimezone()]
            value_ = quote_plus(str(value_))
            subquery = f"{key_}={value_}"

        if subquery:
            query.append(subquery)

    return "&".join(query)
