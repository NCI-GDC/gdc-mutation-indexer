import logging
from typing import AbstractSet, Iterable, List, Optional, Union

from pyspark import sql
from pyspark.sql import functions as F
from typing_extensions import Literal

import config
from exports import es_utils, schemas
from exports.builders import base_input_builder
from exports.constants import build


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


BASE_PRIMARY_ALIQUOT_FIELDS = frozenset(
    (
        "file_id",
        "created_datetime",
        "cases.case_id",
        "cases.samples.sample_id",
        "cases.samples.sample_type",
    )
)


def _add_required_include_fields(
    include_fields: Union[Iterable[str], Literal[True]]
) -> Union[Iterable[str], Literal[True]]:
    if include_fields is not True:
        return BASE_PRIMARY_ALIQUOT_FIELDS.union(include_fields)  # type: ignore

    return include_fields


def _expand_aliquots(aliquot_df: sql.DataFrame) -> sql.DataFrame:
    return (
        aliquot_df.select("file_id", F.explode_outer("cases").alias("case"))
        .select(
            "file_id",
            F.col("case.case_id").alias("case_id"),
            F.explode_outer("case.samples").alias("sample"),
        )
        .select(
            "file_id",
            "case_id",
            F.col("sample.sample_id").alias("sample_id"),
            F.explode_outer("sample.portions").alias("portion"),
        )
        .select(
            "file_id",
            "case_id",
            "sample_id",
            F.explode_outer("portion.analytes").alias("analyte"),
        )
        .select(
            "file_id",
            "case_id",
            "sample_id",
            F.explode_outer("analyte.aliquots").alias("aliquot"),
        )
    )


class BasePrimaryAliquotBuilder(base_input_builder.BaseInputBuilder):
    def __init__(
        self,
        config: config.BaseConfig,
        sqlContext: sql.SQLContext,
        es_dataframe_util: es_utils.DataFrameUtil,
        input_type: str,
        additional_selections: Iterable[str] = (),
    ) -> None:
        """
        Args:
            config: The app configuration object
            sqlContext: The sql context object for the current pyspark run
            es_dataframe_util: The util for creating dataframes from data in elasticsearch
            input_type: The name of the input type that this builder represents
            additional_selections: An additional set of fields to include when selecting
                data from the newly created primary aliquot data frame.
        """
        super().__init__(config, sqlContext, input_type)

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
            *self._additional_selections
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
        return self._es_dataframe_util.get_dataframe(
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
                *self._additional_selections
            )
            .select(
                "file_id",
                "created_datetime",
                F.col("case.case_id").alias("case_id"),
                "case",
                F.explode("case.samples").alias("sample"),
                *self._additional_selections
            )
            .select(
                "file_id",
                "created_datetime",
                "case_id",
                "case",
                "sample.sample_id",
                "sample.sample_type",
                *self._additional_selections
            )
            .select(
                "file_id",
                "created_datetime",
                "case_id",
                "sample_id",
                "case",
                _sample_weight_col(),
                *self._additional_selections
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
            a dataframe with the file data associated with the most relevant sample for each case.

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
                *self._additional_selections
            )
        )


def _get_gene_expression_filters(projects: Optional[List[str]]) -> List[dict]:
    filters = [
        {"terms": {"data_type": ["Gene Expression Quantification"]}},
        {"terms": {"acl": ["open"]}},
        {"term": {"analysis.workflow_type": "STAR - Counts"}},
    ]  # type: List[dict]

    if projects:
        filters.append(
            {
                "nested": {
                    "path": "cases",
                    "query": {"terms": {"cases.project.project_id": projects}},
                }
            }
        )

    return filters


class GeneExpressionPrimaryAliquotBuilder(BasePrimaryAliquotBuilder):
    def __init__(
        self,
        config: config.BaseConfig,
        sql_context: sql.SQLContext,
        es_dataframe_util: es_utils.DataFrameUtil,
    ) -> None:
        """
        Args:
            config: The app configuration object
            sqlContext: The sql context object for the current pyspark run
            es_dataframe_util: The util for creating dataframes from data in elasticsearch
        """
        super().__init__(
            config, sql_context, es_dataframe_util, "gene_expression_primary_aliquot"
        )

    def build_from_scratch(self, **kwargs: sql.DataFrame) -> sql.DataFrame:
        """
        Gets the case and it's associated file data for the mutation index.

        Args:
            workflow_types: A collection of analysis workflow types to
                filter files on.

        Returns:
            (GeneExpressionPrimaryAliquotData): An object containing the primary aliquot
            dataframe as well as a list of all the file urls associated with the primary
            aliquots.

            primary_aliquot{}
            |---file_id
            |---file_url
            |---case_id
            |---submitter_id
            |---demographic{}
            |   |---days_to_death
            |   |---ethnicity
            |   |---gender
            |   |---race
            |   |---vital_status
            |
            |---project{}
            |   |---project_id
            |
            |---diagnoses[]
            |   |---age_at_diagnosis
            |
            |---samples[]
                |---sample_type
        """
        filters = _get_gene_expression_filters(self.config.projects)
        case_fields = [
            "cases.submitter_id",
            "cases.demographic.days_to_death",
            "cases.demographic.ethnicity",
            "cases.demographic.gender",
            "cases.demographic.race",
            "cases.demographic.vital_status",
            "cases.project.project_id",
            "cases.diagnoses.age_at_diagnosis",
        ]

        return self._get_primary_aliquot_df(
            filters,
            entities=frozenset(("case",)),
            include_fields=case_fields,
        ).select(
            "file_id",
            "case_id",
            "case.submitter_id",
            "case.demographic",
            "case.project",
            "case.diagnoses",
            "case.samples",
        )


class PrimaryAliquotBuilder(BasePrimaryAliquotBuilder):
    FILE_URL_BATCH_SIZE = 1000

    def __init__(
        self,
        config: config.BaseConfig,
        sql_context: sql.SQLContext,
        es_dataframe_util: es_utils.DataFrameUtil,
        es_rdd_util: es_utils.RDDUtil,
    ) -> None:
        """
        Args:
            config: The app configuration object
            sql_context: The sql context object for the current pyspark run
            es_dataframe_util: The util for creating dataframes from data in elasticsearch
            es_rdd_util: The util for creating RDD objects from data in elasticsearch
        """
        super().__init__(
            config,
            sql_context,
            es_dataframe_util,
            "primary_aliquot",
            additional_selections=("experimental_strategy",),
        )
        self._sql_context = sql_context
        self._es_rdd_util = es_rdd_util
        self._logger = logging.getLogger(self.__class__.__name__)

    def _get_aliquot_level_df(self) -> sql.DataFrame:
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "nested": {
                                "path": "cases.samples.portions.analytes.aliquots",
                                "query": {
                                    "exists": {
                                        "field": "cases.samples.portions.analytes.aliquots"
                                    }
                                },
                            }
                        },
                    ]
                }
            }
        }
        included_fields = (
            "file_id",
            "cases.case_id",
            "cases.samples.sample_id",
            "cases.samples.portions.analytes.aliquots.aliquot_id",
            "cases.samples.portions.analytes.aliquots.created_datetime",
        )
        aliquot_data_schema = schemas.load_schema(
            "builders/primary_aliquot/aliquot_data.json"
        )

        if self.config.projects:
            project_clause = {
                "nested": {
                    "path": "cases",
                    "query": {
                        "terms": {"cases.project.project_id": self.config.projects}
                    },
                }
            }

            query["query"]["bool"]["must"].append(project_clause)

        aliquot_df = _expand_aliquots(
            self._es_rdd_util.get_rdd(
                build.IndexType.FILE, include_fields=included_fields, query=query
            )
            .toDF(aliquot_data_schema)
            .select("_source.*")
        ).select(
            "file_id",
            "case_id",
            "sample_id",
            "aliquot.aliquot_id",
            F.col("aliquot.created_datetime")
            .cast("timestamp")
            .alias("aliquot_created_datetime"),
        )

        return aliquot_df

    def build_from_scratch(self, **kwargs: sql.DataFrame) -> sql.DataFrame:
        """
        Gets the file data associated with the best match sample for every
        case in the current processes configured project(s)

        Return:
            A data frame with the file data

            primary_aliquot{}
            |---aliquot_id
            |---case_id
            |---entity
            |---entity_id
            |---experimental_strategy
            +---file_id
        """
        filters = (
            [
                {
                    "nested": {
                        "path": "cases",
                        "query": {
                            "terms": {"cases.project.project_id": self.config.projects}
                        },
                    }
                }
            ]
            if self.config.projects
            else [{"match_all": {}}]
        )

        include_fields = (
            "file_id",
            "created_datetime",
            "experimental_strategy",
            "cases.case_id",
            "cases.samples.sample_id",
            "cases.samples.sample_type",
        )
        primary_aliquot_df = self._get_primary_aliquot_df(
            filters, include_fields=include_fields
        ).select(
            "entity_id",
            "entity",
            "case_id",
            "file_id",
            "experimental_strategy",
            "sample_id",
        )
        aliquot_data_df = self._get_aliquot_level_df()
        aliquot_df = primary_aliquot_df.join(
            aliquot_data_df, on=["file_id", "case_id", "sample_id"], how="left"
        )

        aliquot_window = (
            sql.Window()
            .partitionBy("entity", "entity_id")
            .orderBy("aliquot_created_datetime", "aliquot_id")
        )

        return (
            aliquot_df.withColumn("row_number", F.row_number().over(aliquot_window))
            .where(F.col("row_number") == 1)
            .select(
                "aliquot_id",
                "case_id",
                "entity",
                "entity_id",
                "experimental_strategy",
                "file_id",
            )
        )
