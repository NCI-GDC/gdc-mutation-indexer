import abc
import logging
from typing import (
    AbstractSet,
    Generic,
    Iterable,
    Mapping,
    Optional,
    Type,
    TypeVar,
    get_type_hints,
)

from pyspark import sql
from typing_extensions import Protocol, TypeGuard

from exports.configuration.builders import common
from exports.constants import build

TConfig = TypeVar("TConfig", bound=common.Builder)
TInputDFs = TypeVar("TInputDFs", bound=Mapping[str, object])

logger = logging.getLogger(__name__)


class Builder(Protocol):
    """A class which can build its defined output dataframe from its required inputs."""

    @property
    def output(self) -> build.DataFrame:  # type: ignore
        """The data frame which will be produced by this builder."""
        pass

    @property
    def inputs(self) -> Iterable[build.DataFrame]:  # type: ignore
        """The required data frames to build this builder's output."""
        pass

    def build(self, **inputs: sql.DataFrame) -> sql.DataFrame:  # type: ignore
        """
        From the given inputs builds the defined output data frame.

        Args:
            inputs: A set of input data frames that MUST include the data frames
                defined in the implementing class's inputs property.

        Returns:
            A data frame which contains the expected data of the defined output
            property.
        """
        pass


class InputDataFrameManger(Generic[TInputDFs]):
    __slots__ = ("_required_dfs", "_required_params")

    def __init__(self, input_type: Type[TInputDFs]) -> None:
        type_hints = get_type_hints(input_type)

        assert all(
            issubclass(t, sql.DataFrame) for t in type_hints.values()
        ), "Input mapping type must contain only sql.DataFrames"

        self._required_params: AbstractSet[str] = type_hints.keys()
        self._required_dfs = tuple(
            build.DataFrame.from_param(p) for p in self._required_params
        )

    @property
    def required_dataframes(self) -> Iterable[build.DataFrame]:
        """
        All required data frames needed as inputs for the given input's TypedDict.
        """
        return self._required_dfs

    def check(self, inputs: Mapping[str, sql.DataFrame]) -> TypeGuard[TInputDFs]:
        """
        Checks if all required keys for the input's TypedDict are present in the input
        mapping.

        Returns:
            True if the inputs mapping is an instance of the desired input_dfs.
        """
        return self._required_params <= inputs.keys()


class InputBuilder(Generic[TConfig, TInputDFs], Builder, abc.ABC):
    __slots__ = ("_config", "_spark_session", "_input_manager", "_output")

    def __init__(
        self,
        config: TConfig,
        spark_session: sql.SparkSession,
        input_type: Type[TInputDFs],
        output: build.DataFrame,
    ) -> None:
        self._config = config
        self._spark_session = spark_session
        self._input_manager = InputDataFrameManger(input_type)
        self._output = output

    @property
    def output(self) -> build.DataFrame:
        return self._output

    @property
    def inputs(self) -> Iterable[build.DataFrame]:
        return self._input_manager.required_dataframes

    @abc.abstractmethod
    def _build_from_scratch(self, input_dfs: TInputDFs) -> sql.DataFrame:
        """
        The functionality to build a new data frame from the required inputs.

        Args:
            inputs: The required data frames to construct the output data frame.

        Retruns:
            A data frame which contains the expected data of the defined output.
        """
        pass

    def _safe_read(self) -> sql.DataFrame:
        """
        Safely reads a backed up parquet file into a data frame. If the file does not
        exist then an error will be raised.

        Returns:
            A data frame with data from the file at the configured backup path
        """
        logger.info(f"Reading: {self.output.name}")

        return self._spark_session.read.parquet(self._config.backup.path)

    def _read(self) -> Optional[sql.DataFrame]:
        """
        Reads the data frame, if configured to READ, from the configure parqet file. If
        the builder is not configured to read then None is returned.

        Returns:
            An optional data frame based on the configured backup mode.
        """
        if self._config.backup.mode == build.BackupMode.READ:
            return self._safe_read()

        return None

    def _write(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Writes the data frame to any configured or required data store. I.e. memory,
        disk, or elasticsearch. If the df is written to disk and the backup mode is
        BOTH than the written dataframe will be returned.

        Returns:
            The original, cached, or written data frame depending on the builders
            configuration.
        """
        if self._config.backup.mode.is_write():
            logger.info(f"Writing: {self.output.name}")
            df.repartition(self._config.backup.partition_size).write.parquet(
                self._config.backup.path, mode="overwrite"
            )

        if self._config.backup.mode == build.BackupMode.BOTH:
            return self._safe_read()

        return df.cache() if self._config.is_cached else df

    def build(self, **inputs: sql.DataFrame) -> sql.DataFrame:
        assert self._input_manager.check(inputs), "Missing required inputs."

        df = self._read()

        if not df:
            logger.info(f"Building: {self.output.name}")

            df = self._build_from_scratch(inputs)

        return self._write(df)
