import decimal
import uuid
from typing import Any, Optional, Union
from unittest import mock

from exports import es_utils

DECIMAL_CONTEXT = decimal.Context(prec=10)


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
    loader.load_mappings.return_value = {}

    return loader
