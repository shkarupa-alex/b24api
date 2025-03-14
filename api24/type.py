from datetime import datetime
from typing import TypeAlias

# Types allowed in response and request
BaseType: TypeAlias = bool | str | int | float | dict | list | datetime | None
