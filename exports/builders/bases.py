import abc
import copy
import functools
import logging
from typing import (
    AbstractSet,
    Any,
    Dict,
    Generic,
    Iterable,
    Mapping,
    Optional,
    Type,
    TypeVar,
    Union,
    get_type_hints,
)

import more_itertools
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types
from typing_extensions import Literal, Protocol, TypeGuard

from exports import es_utils
from exports.configuration.builders import common
from exports.constants import build

TConfig = TypeVar("TConfig", bound=common.Builder)
TIndexConfig = TypeVar("TIndexConfig", bound=common.IndexBuilder)
TInputDFs = TypeVar("TInputDFs", bound=Mapping[str, object])

logger = logging.getLogger(__name__)

BASE_PRIMARY_ALIQUOT_FIELDS = frozenset(
    (
        "file_id",
        "created_datetime",
        "cases.case_id",
        "cases.samples.sample_id",
        "cases.samples.sample_type",
    )
)


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
            input_dfs: The required data frames to construct the output data frame.

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
            df.write.parquet(self._config.backup.path, mode="overwrite")

        if self._config.backup.mode == build.BackupMode.BOTH:
            return self._safe_read()

        return df.cache() if self._config.is_cached else df
    
    def _build(self, input_dfs: TInputDFs) -> sql.DataFrame:
        """
        A wrapper method whoes base functionality is to call the `_build_from_scratch`
        method. Override this method in a derived base class to apply any post 
        transformations that have be applied to all builders inherriting from this base.

        Args:
            input_dfs: The required data frames to construct the output data frame.

        Returns:
            An data frame constructed from the given inputs based on the logic defined
            in the `_build_from_scratch` with all universal transformations from the 
            base builder applied.
        """
        return self._build_from_scratch(input_dfs)

    def build(self, **inputs: sql.DataFrame) -> sql.DataFrame:
        assert self._input_manager.check(inputs), "Missing required inputs."

        df = self._read()

        if not df:
            logger.info(f"Building: {self.output.name}")

            df = self._build(inputs)

        return self._write(df)


def _sample_weight_col() -> sql.Column:
    """
    Builds the sample weight column based on the sample type.

    Returns:
        Weighted sample column
    """
    weights = (
        ("Primary Tumor", 1),
        ("Primary Blood Derived Cancer - Bone Marrow", 2),
        ("Primary Blood Derived Cancer - Peripheral Blood", 3),
        ("Metastatic", 4),
        ("Additional Metastatic", 5),
        ("Recurrent Tumor", 6),
        ("Recurrent Blood Derived Cancer - Bone Marrow", 7),
        ("Recurrent Blood Derived Cancer - Peripheral Blood", 8),
        ("Additional - New Primary", 9),
    )
    when_clause = F.when(F.lit(1) != F.lit(1), 0)

    for sample_type, weight in weights:
        when_clause = when_clause.when(F.col("sample_type") == sample_type, weight)

    return when_clause.otherwise(len(weights) + 1).alias("sample_weight")


def _combine_weighted_entity_dfs(
    weighted_file_df: Optional[sql.DataFrame],
    weighted_case_df: Optional[sql.DataFrame],
) -> sql.DataFrame:
    if weighted_case_df and weighted_file_df:
        return weighted_case_df.union(weighted_file_df)

    elif weighted_case_df:
        return weighted_case_df

    elif weighted_file_df:
        return weighted_file_df

    else:
        raise ValueError("At least one valid entity must be provided.")


def _add_required_include_fields(
    include_fields: Union[Iterable[str], Literal[True]]
) -> Union[Iterable[str], Literal[True]]:
    if include_fields is not True:
        return BASE_PRIMARY_ALIQUOT_FIELDS.union(include_fields)

    return include_fields


class PrimaryAliquotBuilder(
    Generic[TConfig, TInputDFs], InputBuilder[TConfig, TInputDFs]
):
    __slots__ = ("_es_dataframe_util", "_additional_selections")

    def __init__(
        self,
        config: TConfig,
        spark_session: sql.SparkSession,
        input_type: Type[TInputDFs],
        output: build.DataFrame,
        es_dataframe_util: es_utils.DataFrameUtil,
        additional_selections: Iterable[str] = (),
    ) -> None:
        """
        Args:
            config: The configuration for the given builder.
            sqlContext: The sql session object for the current pyspark run.
            es_dataframe_util: The util for creating dataframes from data in
                elasticsearch.
            output: The DataFrame which is the resulting output of this builder.
            additional_selections: An additional set of fields to include when selecting
                data from the newly created primary aliquot data frame.
        """
        super().__init__(config, spark_session, input_type, output)

        self._es_dataframe_util = es_dataframe_util
        self._additional_selections = additional_selections

    def _get_weighted_entity_df(
        self,
        weighted_df: sql.DataFrame,
        entity_id: str,
        entity: str,
    ) -> sql.DataFrame:
        return weighted_df.select(
            F.col(entity_id).alias("entity_id"),
            F.lit(entity).alias("entity"),
            "file_id",
            "created_datetime",
            "case_id",
            "sample_id",
            "case",
            "sample_weight",
            *self._additional_selections,
        )

    def _get_initial_weighted_df(
        self,
        query: dict,
        include_fields: Union[Iterable[str], Literal[True]],
    ) -> sql.DataFrame:
        """
        Gets the initial data from elasticsearch. This is the data meeting the
        criteria in the query and includes the fields given in include_fields.

        NOTE: Override this method if any manipulation of the data frame needs to
        happen before the standard primary aliquot selection begins. E.g. use it to
        alias fields that have special characters that cannot be utilized in
        additional_selections

        Args:
            query: The query to be run in elasticsearch to determine the data loaded.
            include_fields: The fields that will be included/returned in the dataframe.
                If set to True, all fields are returned.

        Returns:
            The data frame created in the above process.
        """
        return self._es_dataframe_util.read(
            build.IndexType.FILE,
            include_fields=include_fields,
            query=query,
        )

    def _get_weighted_df(
        self,
        query: dict,
        include_fields: Union[Iterable[str], Literal[True]],
    ) -> sql.DataFrame:
        return (
            self._get_initial_weighted_df(query, include_fields)
            .select(
                "file_id",
                F.col("created_datetime").cast("timestamp"),
                F.explode("cases").alias("case"),
                *self._additional_selections,
            )
            .select(
                "file_id",
                "created_datetime",
                F.col("case.case_id").alias("case_id"),
                "case",
                F.explode("case.samples").alias("sample"),
                *self._additional_selections,
            )
            .select(
                "file_id",
                "created_datetime",
                "case_id",
                "case",
                "sample.sample_id",
                "sample.sample_type",
                *self._additional_selections,
            )
            .select(
                "file_id",
                "created_datetime",
                "case_id",
                "sample_id",
                "case",
                _sample_weight_col(),
                *self._additional_selections,
            )
        )

    def _get_primary_aliquot_df(
        self,
        filters: Iterable[dict],
        entities: AbstractSet[str] = frozenset(("case", "file")),
        include_fields: Union[Iterable[str], Literal[True]] = True,
    ) -> sql.DataFrame:
        """
        Args:
            filters: The filters used to query es files with
            include_fields: An optional field used to tell spark which fields to read from
                spark. Use to include extra fields in the returned case mapping.

        Returns:
            a dataframe with the file data associated with the most relevant sample for
            each case.

            file_id
            created_datetime
            experimental_strategy
            case_id
            case
                case_id
                samples
                (other case fields can be included in the include fields param)
        """
        query = {"query": {"bool": {"must": filters}}}
        include_fields = _add_required_include_fields(include_fields)
        weighted_df = self._get_weighted_df(query, include_fields)
        weighted_file_df = None
        weighted_case_df = None

        if "file" in entities:
            weighted_file_df = self._get_weighted_entity_df(
                weighted_df, "file_id", "file"
            )

        if "case" in entities:
            weighted_case_df = self._get_weighted_entity_df(
                weighted_df, "case_id", "case"
            )

        weighted_entity_df = _combine_weighted_entity_dfs(
            weighted_file_df, weighted_case_df
        )
        entity_window = (
            sql.Window()
            .partitionBy("entity", "entity_id")
            .orderBy(
                F.col("sample_weight"),
                F.col("created_datetime"),
                F.col("file_id"),
            )
        )

        return (
            weighted_entity_df.withColumn(
                "row_number", F.row_number().over(entity_window)
            )
            .where(F.col("row_number") == 1)
            .select(
                "entity_id",
                "entity",
                "file_id",
                "created_datetime",
                "case_id",
                "sample_id",
                "case",
                *self._additional_selections,
            )
        )


def _walk_schema(field: types.StructField, child_name: str) -> types.StructField:
    """
    Walks the inputs fields data type field in order to find the child field with the
    input name. 

    Args:
        field: the field found in a parent schema/struct type.
        child_name: the name of the desired child field.

    Returns:
        The child field with the given child_name.

    Raises:
        ValueError: this is raised if the input field is NOT a struct type, an array 
            with an struct type for an element type, or a map type with a value type
            which is a struct type.
    """
    datatype = field.dataType

    while isinstance(datatype, (types.ArrayType, types.MapType)):
        if isinstance(datatype, types.ArrayType):
            datatype = datatype.elementType
        if isinstance(datatype, types.MapType):
            datatype = datatype.valueType

    if isinstance(datatype, types.StructType):
        return datatype[child_name]
    else:
        raise ValueError(
            f"Unexpected data type encountered while walking. DataType: {type(datatype)}"
        )


class IndexBuilder(
    Generic[TIndexConfig, TInputDFs], InputBuilder[TIndexConfig, TInputDFs], abc.ABC
):
    """A builder base class for constructing data to be inserted into an elasticsearch index."""

    __slots__ = ("_es_dataframe_util", "_mappings_loader", "_index_type", "_index_name")

    def __init__(
        self,
        config: TIndexConfig,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        mappings_loader: es_utils.MappingsLoader,
        input_type: Type[TInputDFs],
        output: build.DataFrame,
    ) -> None:
        super().__init__(config, spark_session, input_type, output)

        self._es_dataframe_util = es_dataframe_util
        self._mappings_loader = mappings_loader
        self._index_type = build.IndexType[self._output.name]
        self._index_name, _ = self._index_type.get_mappings_details()

    def _write(self, df: sql.DataFrame) -> sql.DataFrame:
        df = super()._write(df)
        df = df.repartition(self._config.partition_size, self._config.id_field)

        logger.info(f"Writing to ES: {self.output.name}")
        self._es_dataframe_util.write(df, self._index_type, self._config.id_field)

        return df
    
    def _get_boolean_paths(self) -> Iterable[str]:
        """
        Find all the boolean field in mapping and return the paths

        Returns:
            An iterable of each path to a boolean field represented as a series of
            field names seperated by a '.'.
        """
        def get_boolean_paths(node: Dict[str, Dict[str, Any]], path: str = "") -> Iterable[str]:
            for key, value in node.items():
                path = f"{path}{key}"

                if value.get("type") == "boolean":
                    yield path
                elif "properties" in value:
                    yield from get_boolean_paths(value["properties"], f"{path}.")

        mappings = self._mappings_loader.load_mappings(self._index_type).get("mappings", {})

        return get_boolean_paths(mappings.get("properties", {}))

    def _cast_booleans(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Ensure all the boolean fields in data frame are booleans before save to ES

        Args:
            df: pyspark dataframe to cast boolean

        Returns:
            the input dataframe with all boolean fields cast to such type.
        """
        paths = self._get_boolean_paths()
        schema = copy.deepcopy(df.schema)

        for raw_path in paths:
            path = iter(raw_path.split("."))
            fieldname = more_itertools.first(path)
            field = functools.reduce(_walk_schema, path, schema[fieldname])
            field.dataType = types.BooleanType()

        return df.select(*(F.col(f.name).cast(f.dataType) for f in schema.fields))

    def _build(self, input_dfs: TInputDFs) -> sql.DataFrame:
        df = super()._build(input_dfs)

        return self._cast_booleans(df)
