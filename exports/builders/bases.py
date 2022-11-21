import abc
import inspect
import logging
from typing import Generic, Iterable, Optional, TypeVar

from pyspark import sql
from typing_extensions import Protocol

from exports.configuration.builders import common
from exports.constants import build

TConfig = TypeVar("TConfig", bound=common.Builder)

logger = logging.getLogger(__name__)

INPUT_PARAM_KINDS = frozenset(
    (inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
)


class Builder(Protocol):
    @property
    def output(self) -> build.DataFrame:  # type: ignore
        pass

    @property
    def inputs(self) -> Iterable[build.DataFrame]:  # type: ignore
        pass

    def build(self, *arg_dfs: sql.DataFrame, **kwarg_dfs: sql.DataFrame) -> sql.DataFrame:  # type: ignore
        pass


class InputBuilder(Generic[TConfig], Builder, abc.ABC):
    def __init__(
        self, config: TConfig, spark_session: sql.SparkSession, output: build.DataFrame
    ) -> None:
        self._config = config
        self._spark_session = spark_session
        self._output = output

    @property
    def output(self) -> build.DataFrame:
        return self._output

    @property
    def inputs(self) -> Iterable[build.DataFrame]:
        params = inspect.signature(self._build_from_scratch).parameters.values()
        param_names = (p.name for p in params if p.kind in INPUT_PARAM_KINDS)

        return (build.DataFrame.from_param(n) for n in param_names)

    @abc.abstractmethod
    def _build_from_scratch(
        self, *arg_dfs: sql.DataFrame, **kwarg_dfs: sql.DataFrame
    ) -> sql.DataFrame:
        pass

    def _safe_read(self) -> sql.DataFrame:
        logger.info(f"Reading: {self.output.name}")

        return self._spark_session.read.parquet(self._config.backup.path)

    def _read(self) -> Optional[sql.DataFrame]:
        if self._config.backup.mode == build.BackupMode.READ:
            return self._safe_read()

        return None

    def _write(self, df: sql.DataFrame) -> sql.DataFrame:
        if self._config.backup.mode.is_write():
            logger.info(f"Writing: {self.output.name}")
            df.write.parquet(self._config.backup.path, mode="overwrite")
            self._has_written_backup = True

        if self._config.backup.mode == build.BackupMode.BOTH:
            return self._safe_read()

        return df

    def build(self, **inputs: sql.DataFrame) -> sql.DataFrame:
        df = self._read()

        if not df:
            logger.info(f"Building: {self.output.name}")

            df = self._build_from_scratch(**inputs)

        return self._write(df)
