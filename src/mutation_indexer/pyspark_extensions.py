import dataclasses
from typing import Any
from collections.abc import Iterable

from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types


@dataclasses.dataclass(frozen=True)
class DefaultColumn:
    name: str
    type: types.DataType = types.StringType()
    value: Any = None

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


def explode_nested_doc(col: sql.Column | str) -> sql.Column:
    """
    This explodes a list from Elasicsearch which may or may not have all documents w/ a
    singleton item. This is current causing and issue with the native explode
    functionality of spark when used with the es plugin as spark is getting thinking it
    has a struct obj vs an array of such.

    Args:
        col: The name of the column which will be exploded.

    Returns:
        The exploded column.
    """
    col = col if isinstance(col, sql.Column) else F.col(col)

    return F.explode(F.when(F.size(col) == 1, F.array(col[0])).otherwise(col))
