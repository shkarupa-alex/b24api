from datetime import datetime
from typing import Any

# Types allowed in response and request
type ApiTypes = bool | str | int | float | dict[str, Any] | list[Any] | datetime | None
