import dataclasses
from typing import Any, Iterable, Union

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


def explode_safe(col: Union[sql.Column, str]) -> sql.Column:
    col = col if isinstance(col, sql.Column) else F.col(col)

    return F.explode(F.when(F.size(col) == 1, F.array(col[0])).otherwise(col))
