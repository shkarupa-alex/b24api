import logging
from collections.abc import Generator
from typing import Annotated

from fast_depends import Depends
from httpx import Client


def httpx_client() -> Generator[Client, None, None]:
    httpx_logger = logging.getLogger("httpx")
    httpx_logger.setLevel(logging.WARNING)

    client = Client(http2=True, timeout=30)

    yield client


HttpxClient = Annotated[Client, Depends(httpx_client)]
