"""A consumer module type-checked against the installed wheel, not the source tree.

The ``wheel-typing`` CI job copies this file into an empty directory, installs the built wheel
into a clean virtual environment and runs ``mypy --strict`` on it. The job requires the revealed
type below to be exactly ``b24api.contracts.request.Request`` and refuses any ``import-untyped``
report, which is what a wheel without ``b24api/py.typed`` produces. pytest never collects this
module: its name does not match the test file pattern.
"""

from typing import reveal_type

from b24api import Request, RouteKind

reveal_type(Request("user.get", route=RouteKind.BARE))
