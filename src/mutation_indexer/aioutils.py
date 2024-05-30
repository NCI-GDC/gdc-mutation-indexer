import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from typing_extensions import ParamSpec

TParams = ParamSpec("TParams")
TReturn = TypeVar("TReturn")


def to_thread(
    callable: Callable[TParams, TReturn]
) -> Callable[TParams, Awaitable[TReturn]]:
    def wrapper(*args: TParams.args, **kwargs: TParams.kwargs) -> Awaitable[TReturn]:
        return asyncio.to_thread(callable, *args, **kwargs)

    return wrapper
