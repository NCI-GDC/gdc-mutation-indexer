import dataclasses
import decimal
import functools
import unittest
import uuid
from collections.abc import Callable, Iterable
from typing import Any, ClassVar, Generic, Optional, Protocol, TypeVar, Union, overload
from unittest import mock

import pyspark
from pyspark import sql
from pyspark.sql import types
from typing_extensions import TypeVarTuple, Unpack

from mutation_indexer import es_utils
from tests.unit import fixtures

DECIMAL_CONTEXT = decimal.Context(prec=6)  # 32 bit float has 6 to 7 significant digits.


TTestCase = TypeVar("TTestCase", bound=unittest.TestCase)
TParams = TypeVarTuple("TParams")


class DataClass(Protocol):
    __dataclass_fields__: ClassVar[dict[str, Any]]


CreateDataFrame = Callable[[Iterable[DataClass], types.StructType], sql.DataFrame]


def create_dataframe(
    data: Iterable[DataClass], schema: types.StructType
) -> sql.DataFrame:
    rdd: pyspark.RDD = fixtures.SPARK_SESSION.sparkContext.parallelize(
        map(dataclasses.asdict, data)
    )

    return fixtures.SPARK_SESSION.createDataFrame(rdd, schema)


class parametrize(Generic[Unpack[TParams]]):
    @overload
    def __init__(self, *subtests: tuple[Unpack[TParams]]) -> None:
        pass

    @overload
    def __init__(self, **subtests: tuple[Unpack[TParams]]) -> None:
        pass

    def __init__(
        self, *unnamed: tuple[Unpack[TParams]], **subtests: tuple[Unpack[TParams]]
    ) -> None:
        if subtests and unnamed:
            raise ValueError("Cannot use both unnamed and named subtests.")
        elif subtests:
            self._subtests = subtests
        elif unnamed:
            self._subtests = {self._convert_to_name(s): s for s in unnamed}
        else:
            raise ValueError("Test must have at lease one subtest.")

    @staticmethod
    def _convert_to_name(params: tuple[Any]) -> str:
        strings: Iterable[str] = map(str, params)

        return "-".join(strings)

    def __call__(
        self, func: Callable[[TTestCase, Unpack[TParams]], None]
    ) -> Callable[[TTestCase], None]:
        @functools.wraps(func)
        def wrapper(test_case: TTestCase) -> None:
            for subtest, parameters in self._subtests.items():
                with test_case.subTest(subtest):
                    func(test_case, *parameters)

        return wrapper


def generate_uuid5(*args: Any) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, "\t".join(str(arg) for arg in args)))


def _get_decimal(
    x: Optional[Union[float, decimal.Decimal]]
) -> Optional[decimal.Decimal]:
    if x is None:
        return x
    elif isinstance(x, decimal.Decimal):
        return DECIMAL_CONTEXT.create_decimal(x)
    else:
        return DECIMAL_CONTEXT.create_decimal_from_float(x)


def assert_float_equal(
    a: Optional[Union[float, decimal.Decimal]],
    b: Optional[Union[float, decimal.Decimal]],
) -> None:
    a = _get_decimal(a)
    b = _get_decimal(b)

    assert a == b


def assert_float_not_equal(
    a: Optional[Union[float, decimal.Decimal]],
    b: Optional[Union[float, decimal.Decimal]],
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
