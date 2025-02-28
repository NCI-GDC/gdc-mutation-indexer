import decimal
import uuid
from collections.abc import Callable, Iterable
from typing import Any, ClassVar, Protocol
from unittest import mock

from pyspark import sql
from pyspark.sql import types

from mutation_indexer import es_utils

DECIMAL_CONTEXT = decimal.Context(prec=6)  # 32 bit float has 6 to 7 significant digits.


class DataClass(Protocol):
    __dataclass_fields__: ClassVar[dict[str, Any]]


CreateDataFrame = Callable[[Iterable[DataClass], types.StructType], sql.DataFrame]


def generate_uuid5(*args: Any) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, "\t".join(str(arg) for arg in args)))


def _get_decimal(x: float | decimal.Decimal | None) -> decimal.Decimal | None:
    if x is None:
        return x
    elif isinstance(x, decimal.Decimal):
        return DECIMAL_CONTEXT.create_decimal(x)
    else:
        return DECIMAL_CONTEXT.create_decimal_from_float(x)


def assert_float_equal(
    a: float | decimal.Decimal | None,
    b: float | decimal.Decimal | None,
) -> None:
    a = _get_decimal(a)
    b = _get_decimal(b)

    assert a == b


def assert_float_not_equal(
    a: float | decimal.Decimal | None,
    b: float | decimal.Decimal | None,
) -> None:
    a = _get_decimal(a)
    b = _get_decimal(b)

    assert a != b


def arrange_empty_mappings_loader() -> es_utils.MappingsLoader:
    loader = mock.MagicMock(spec=es_utils.MappingsLoader)
    loader.load_mapper.return_value = mock.MagicMock(mappings={}, settings={})

    return loader


def convert_lists(data: dict) -> dict:
    """Converts all lists in the dict into tuples for comparing with models.

    Args:
        data: The dictionary containing the lists to convert.

    Returns:
        The dictionary that was passed into the function.
    """
    for key, item in data.items():
        if isinstance(item, list) and isinstance(next(iter(item), None), dict):
            for subitem in item:
                convert_lists(subitem)

        if isinstance(item, dict):
            convert_lists(item)

        if isinstance(item, list):
            data[key] = tuple(item)

    return data
