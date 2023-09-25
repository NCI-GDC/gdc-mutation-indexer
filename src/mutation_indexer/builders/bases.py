import abc
import copy
import functools
import logging
from collections.abc import Iterable, Iterator, Mapping, Set
from importlib import resources
from typing import (
    Any,
    Dict,
    Generic,
    Optional,
    Protocol,
    TypeVar,
    get_type_hints,
    runtime_checkable,
)

import more_itertools
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types
from typing_extensions import TypeGuard

from mutation_indexer import es_utils, schemas
from mutation_indexer.configuration.builders import common
from mutation_indexer.constants import build

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


@runtime_checkable
class Builder(Protocol):
    """A class which can build its defined output dataframe from its required inputs."""

    def __hash__(self) -> int:
        return hash(self.output)

    def __eq__(self, value: object) -> bool:
        if value is self:
            return True

        if isinstance(value, Builder):
            return self.output == value.output

        if isinstance(value, build.DataFrame):
            return self.output == value

        return False

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

    def __init__(self, input_type: type[TInputDFs]) -> None:
        type_hints = get_type_hints(input_type)

        assert all(
            issubclass(t, sql.DataFrame) for t in type_hints.values()
        ), "Input mapping type must contain only sql.DataFrames"

        self._required_params: Set[str] = type_hints.keys()
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


class InputBuilder(Builder, Generic[TConfig, TInputDFs], abc.ABC):
    __slots__ = ("_config", "_spark_session", "_input_manager", "_output")

    def __init__(
        self,
        config: TConfig,
        spark_session: sql.SparkSession,
        input_type: type[TInputDFs],
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

        Returns:
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
        Reads the data frame, if configured to READ, from the configure parquet file. If
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

    def build(self, **inputs: sql.DataFrame) -> sql.DataFrame:
        assert self._input_manager.check(inputs), "Missing required inputs."

        df = self._read()

        if not df:
            logger.info(f"Building: {self.output.name}")

            df = self._build_from_scratch(inputs)

        return self._write(df)


def _sample_weight_col(sample_type: sql.Column) -> sql.Column:
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

    for sample_type_value, weight in weights:
        when_clause = when_clause.when(sample_type == sample_type_value, weight)

    return when_clause.otherwise(len(weights) + 1)


def _struct_fields_to_es_fields(
    fields: Iterable[types.StructField], path: str = ""
) -> Iterable[str]:
    for field in fields:
        data_type = field.dataType

        if isinstance(data_type, types.ArrayType) and isinstance(
            data_type.elementType, types.StructType
        ):
            data_type = data_type.elementType

        if isinstance(data_type, types.StructType):
            yield from _struct_fields_to_es_fields(
                data_type.fields, f"{path}{field.name}."
            )
        else:
            yield f"{path}{field.name}"


class PrimaryAliquotBuilder(
    Generic[TConfig, TInputDFs], InputBuilder[TConfig, TInputDFs]
):
    __slots__ = ("_es_rdd_util",)

    def __init__(
        self,
        config: TConfig,
        spark_session: sql.SparkSession,
        es_rdd_util: es_utils.RDDUtil,
        input_type: type[TInputDFs],
        output: build.DataFrame,
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

        self._es_rdd_util = es_rdd_util

    def _load_file_schema(self) -> types.StructType:
        """
        This loads the schema which will drive what data will be selected from ES. This
        schema should ultimately reflect on a subset of data from the graph's file
        index. Overload this method and load the core data using `super()` and add any
        additional fields that may be needed in an implementing class (via
        types.StructTypes.add).

        Returns:
            A struct type which will be used to determine the data that is loaded from
            elasticsearch as well as validate the data loaded.
        """
        return schemas.load_schema("builders/bases/primary_aliquot.yaml")

    def _load_aliquot_df(self, query: dict, schema: types.StructType) -> sql.DataFrame:
        """
        Loads the file data from ES and explodes the data out to an aliquot level such
        that each row contains an aliquots info including the calculated sample weight.

        Args:
            query: The query with which to limit the ES results.
            schema: The structure of the expected result from ES.

        Returns:
            A data frame of aliquot data including the sample weight associated with the
            aliquot's sample_type.
        """
        include_fields = _struct_fields_to_es_fields(schema.fields)
        schema = types.StructType(
            [
                types.StructField("_id", types.StringType()),
                types.StructField("_source", schema),
            ]
        )

        return (
            self._es_rdd_util.get_rdd(
                build.IndexType.FILE,
                include_fields=include_fields,
                query=query,
            )
            .toDF(schema)
            .select("_source.*")
            .withColumns(
                {
                    "created_datetime": F.col("created_datetime").cast("timestamp"),
                    "case": F.explode("cases"),
                }
            )
            .withColumns(
                {"case_id": F.col("case.case_id"), "sample": F.explode("case.samples")}
            )
            .where(F.col("sample.sample_type").isNotNull())
            .withColumns(
                {
                    "sample_id": F.col("sample.sample_id"),
                    "sample_type": F.col("sample.sample_type"),
                    "sample_weight": _sample_weight_col(F.col("sample.sample_type")),
                    "portion": F.explode_outer("sample.portions"),
                }
            )
            .withColumn("analyte", F.explode_outer("portion.analytes"))
            .withColumn("aliquot", F.explode_outer("analyte.aliquots"))
            .withColumns(
                {
                    "aliquot_id": F.col("aliquot.aliquot_id"),
                    "aliquot_created_datetime": F.col("aliquot.created_datetime").cast(
                        "timestamp"
                    ),
                }
            )
            .drop("cases", "sample", "portion", "analyte", "aliquot")
        )

    def _get_primary_aliquot_df(self, query: dict) -> sql.DataFrame:
        """
        Loads the primary aliquot data for the given subset of files loaded by the given
        query. The primary aliquot is a determined on a per case basis by selecting the
        file associated with the oldest most "tumor like" sample type and from within
        that sample the oldest aliquot is selected as the primary aliquot. This data
        frame contains the case_id, file_id, sample_id & aliquot_id of all the
        forementioned.

        Args:
            query: The *body* of the elasticsearch search API which will be used to load
                the files from elasticsearch.

        Returns:
            primary_aliquot {}
            |---aliquot_created_datetime
            |---aliquot_id
            |---case_id
            |---file_id
            |---sample_id
            |---sample_type
            |---sample_weight
            |---case {}                <- Contains the data from the file's cases 
            |   +--- ...                  property.
            +---*additional selections <- Any additional fields added to the schema in
                                          an overload of the _load_file_schema method.
        """
        schema = self._load_file_schema()
        final_fields = (
            "aliquot_created_datetime",
            "aliquot_id",
            "case_id",
            "sample_id",
            "sample_type",
            "sample_weight",
            *map(lambda f: "case" if f.name == "cases" else f.name, schema.fields),
        )
        file_df = self._load_aliquot_df(query, schema).select(*final_fields)
        case_window = (
            sql.Window()
            .partitionBy("case_id")
            .orderBy(
                F.col("sample_weight"),
                F.col("created_datetime"),
                F.col("file_id"),
                F.col("aliquot_created_datetime"),
                F.col("aliquot_id"),
            )
        )

        return (
            file_df.withColumn("row_number", F.row_number().over(case_window))
            .where(F.col("row_number") == 1)
            .select(*final_fields)
        )


TResourceConfig = TypeVar("TResourceConfig", bound=common.ResourceBuilder)


class ResourceBuilder(
    Generic[TResourceConfig, TInputDFs], InputBuilder[TResourceConfig, TInputDFs]
):
    def _schema(self) -> types.StructType:
        return schemas.load_schema(self._config.schema)

    def _load_resource_data(self) -> sql.DataFrame:
        with resources.as_file(
            resources.files(self._config.package).joinpath(self._config.resource)
        ) as p:
            df = self._spark_session.read.csv(
                str(p), schema=self._schema(), header=True, sep="\t", comment="#"
            )

        return df


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
        input_type: type[TInputDFs],
        output: build.DataFrame,
    ) -> None:
        super().__init__(config, spark_session, input_type, output)

        self._es_dataframe_util = es_dataframe_util
        self._mappings_loader = mappings_loader
        self._index_type = build.IndexType[self._output.name]
        self._index_name, _ = self._index_type.get_mappings_details()

    def _get_boolean_paths(self) -> Iterator[str]:
        """
        Find all the boolean field in mapping and return the paths

        Returns:
            An iterable of each path to a boolean field represented as a series of
            field names separated by a '.'.
        """

        def get_boolean_paths(
            node: Dict[str, Dict[str, Any]], path: str = ""
        ) -> Iterator[str]:
            for key, value in node.items():
                subpath = f"{path}{key}"

                if value.get("type") == "boolean":
                    yield subpath
                elif "properties" in value:
                    yield from get_boolean_paths(value["properties"], f"{subpath}.")

        mappings = self._mappings_loader.load_mappings(self._index_type).get(
            "mappings", {}
        )

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

    def _write(self, df: sql.DataFrame) -> sql.DataFrame:
        df = self._cast_booleans(df)
        df = super()._write(df)
        df = df.repartition(self._config.partition_size, self._config.id_field)

        logger.info(f"Writing to ES: {self.output.name}")
        self._es_dataframe_util.write(df, self._index_type, self._config.id_field)

        return df
