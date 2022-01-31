from typing import Any, Iterable

import attr
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types


@attr.s(frozen=True)
class DefaultColumn:
    name = attr.ib(type=str)
    type = attr.ib(type=types.DataType, default=types.StringType())
    value = attr.ib(type=Any, default=None)

    def col(self) -> sql.Column:
        return F.lit(self.value).cast(self.type)


def default_columns(df: sql.DataFrame, defaults: Iterable[DefaultColumn]):
    """
    Defaults any columns from the list of default columns NOT found in the
    dataframe to the value indicated in the default column object.

    Args:
        df: the dataframe being defaulted.
        defaults: an iterable of columns to be defaulted to in case they
            are not found in the dataframe.

    Returns:
        A dataframe with all default column present (either they were added
        or they already existed.)
    """
    columns = frozenset(df.columns)
    defaults = filter(lambda d: d.name not in columns, defaults)

    for default in defaults:
        df = df.withColumn(default.name, default.col())

    return df
