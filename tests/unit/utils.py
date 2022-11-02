import contextlib
import decimal
import json
import threading
import uuid
from os import path
from typing import Any, Iterator, Optional, TypeVar
from unittest import mock

from pyspark.sql import types

TNew = TypeVar("TNew")

DECIMAL_CONTEXT = decimal.Context(prec=10)

_patch_lock = threading.RLock()


def load_schema(schema_dir: str, file_name: str) -> types.StructType:
    file_name = path.join(schema_dir, file_name)

    with open(file_name, "r") as f:
        return types.StructType.fromJson(json.load(f))


def generate_uuid5(*args: Any) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, "\t".join(str(arg) for arg in args)))


@contextlib.contextmanager
def patch(
    target: str,
    new: TNew = mock.DEFAULT,
    spec: Optional[Any] = None,
    create: bool = False,
    spec_set: Optional[Any] = None,
    autospec: Optional[Any] = None,
    new_callable: Optional[Any] = None,
    **kwargs: Any
) -> Iterator[TNew]:
    with _patch_lock, mock.patch(
        target,
        new=new,
        spec=spec,
        create=create,
        spec_set=spec_set,
        autospec=autospec,
        new_callable=new_callable,
        **kwargs
    ) as m:
        yield m


def assert_float_equal(a: Optional[float], b: Optional[float]) -> None:
    decimal_a = a if a is None else DECIMAL_CONTEXT.create_decimal_from_float(a)
    decimal_b = b if b is None else DECIMAL_CONTEXT.create_decimal_from_float(b)

    assert decimal_a == decimal_b
