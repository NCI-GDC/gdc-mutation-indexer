import asyncio
import functools
from collections.abc import Awaitable, Callable
from typing import TypeVar

from typing_extensions import ParamSpec

__all__ = ("to_thread",)


TParams = ParamSpec("TParams")
TReturn = TypeVar("TReturn")


def to_thread(
    func: Callable[TParams, TReturn]
) -> Callable[TParams, Awaitable[TReturn]]:
    @functools.wraps(func)
    def wrapper(*args: TParams.args, **kwargs: TParams.kwargs) -> Awaitable[TReturn]:
        return asyncio.to_thread(func, *args, **kwargs)

    return wrapper
