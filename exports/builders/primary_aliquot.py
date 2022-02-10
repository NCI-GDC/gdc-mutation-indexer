import itertools
import logging
from typing import AbstractSet, Iterable, List, NamedTuple, Optional, Union

import more_itertools
from indexclient import client
from pyspark import sql
from pyspark.sql import functions as F

import config
from exports import es_utils
from exports.builders import base_input_builder


def _is_main_url(metadata: dict):
    """
    Check if given metadata corresponds to main IndexD URL:
        * type == cleversafe
        * state == validated

    Returns:
        bool: True if main URL, False otherwise
    """
    return metadata.get("type") == "cleversafe" and metadata.get("state") == "validated"


def _get_and_format_url(doc: client.Document) -> Optional[str]:
    """
    Select main IndexD url if one exist and format it to something that Spark
    understands

    Args:
        doc: IndexD document to extract URL from

    Returns:
        str: formatted main URL
    """
    for url, meta in doc.urls_metadata.items():
        if _is_main_url(meta):
            url = url.replace("s3://", "s3a://").replace(
                "cleversafe.service.consul/", ""
            )
            return url

    return None


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


def _get_weighted_entity_df(
    weighted_df: sql.DataFrame, entity_id: str, entity: str
) -> sql.DataFrame:
    return weighted_df.select(
        F.col(entity_id).alias("entity_id"),
        F.lit(entity).alias("entity"),
        "file_id",
        "created_datetime",
        "experimental_strategy",
        "case_id",
        "sample_id",
        "case",
        "sample_weight",
    )


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
    include_fields: Union[Iterable[str], bool]
) -> Union[Iterable[str], bool]:
    if include_fields is not True:
        return frozenset(
            {
                "file_id",
                "created_datetime",
                "experimental_strategy",
                "cases.case_id",
                "cases.samples.sample_id",
                "cases.samples.sample_type",
            }
        ).union(
            include_fields  # type: ignore
        )

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


GeneExpressionPrimaryAliquotData = NamedTuple(
    "GeneExpressionPrimaryAliquotData",
    [("primary_aliquot_df", sql.DataFrame), ("file_urls", Iterable[str])],
)


class PrimaryAliquotBuilder(base_input_builder.BaseInputBuilder):
    FILE_URL_BATCH_SIZE = 1000

    def __init__(
        self,
        config: config.BaseConfig,
        sql_context: sql.SQLContext,
        indexd: client.IndexClient,
        es_dataframe_util: es_utils.DataFrameUtil,
    ) -> None:
        super().__init__(config, sql_context, "primary_aliquot")
        self._sql_context = sql_context
        self._indexd = indexd
        self._es_dataframe_util = es_dataframe_util
        self._logger = logging.getLogger(self.__class__.__name__)

    def _get_weighted_df(
        self, query: dict, include_fields: Union[Iterable[str], bool]
    ) -> sql.DataFrame:
        return (
            self._es_dataframe_util.get_dataframe(
                es_utils.Index.File,
                include_fields=include_fields,
                query=query,
            )
            .select(
                "file_id",
                F.col("created_datetime").cast("timestamp"),
                "experimental_strategy",
                F.explode("cases").alias("case"),
            )
            .select(
                "file_id",
                "created_datetime",
                "experimental_strategy",
                F.col("case.case_id").alias("case_id"),
                "case",
                F.explode("case.samples").alias("sample"),
            )
            .select(
                "file_id",
                "created_datetime",
                "experimental_strategy",
                "case_id",
                "case",
                "sample.sample_id",
                "sample.sample_type",
            )
            .select(
                "file_id",
                "created_datetime",
                "experimental_strategy",
                "case_id",
                "sample_id",
                "case",
                _sample_weight_col(),
            )
        )

    def _get_primary_aliquot_df(
        self,
        filters: Iterable[dict],
        entities: AbstractSet[str] = frozenset(("case", "file")),
        include_fields: Union[Iterable[str], bool] = True,
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
            weighted_file_df = _get_weighted_entity_df(weighted_df, "file_id", "file")

        if "case" in entities:
            weighted_case_df = _get_weighted_entity_df(weighted_df, "case_id", "case")

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
                "experimental_strategy",
                "case_id",
                "sample_id",
                "case",
            )
        )

    def _get_main_urls(self, file_ids: Iterable[str]) -> Iterable[sql.Row]:
        """
        Construct iterable of {file_id, url}, where url is a main URL associated with the given file_id

        Args:
            file_ids: a list of file_ids

        Returns:
            A generator where each mapping is
            {file_id: x, file_url: u}
        """
        batches = more_itertools.ichunked(file_ids, self.FILE_URL_BATCH_SIZE)
        docs = itertools.chain.from_iterable(
            self._indexd.bulk_request(list(bids)) for bids in batches
        )

        for doc in docs:
            url = _get_and_format_url(doc)

            if url is None:
                self.logger.warning("File is missing: '{}'".format(doc.did))

            else:
                yield sql.Row(file_id=doc.did, file_url=url)

    def build_gene_expression_primary_aliquot_data(
        self,
        workflow_types: Iterable[str],
    ):
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
        filters = [
            {"terms": {"data_type": ["Gene Expression Quantification"]}},
            {"terms": {"acl": ["open"]}},
            {"terms": {"analysis.workflow_type": workflow_types}},
        ]  # type: List[dict]
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

        if self.config.projects:
            filters.append(
                {
                    "nested": {
                        "path": "cases",
                        "query": {
                            "terms": {"cases.project.project_id": self.config.projects}
                        },
                    }
                }
            )

        primary_aliquot_df = self._get_primary_aliquot_df(
            filters,
            entities=frozenset(("case",)),
            include_fields=case_fields,
        )
        file_ids = (
            r.file_id for r in primary_aliquot_df.select("file_id").distinct().collect()
        )

        urls = tuple(self._get_main_urls(file_ids))
        urls_df = self._sql_context.createDataFrame(urls)

        primary_aliquot_df = primary_aliquot_df.select(
            "file_id",
            "case_id",
            "case.submitter_id",
            "case.demographic",
            "case.project",
            "case.diagnoses",
            "case.samples",
        )
        primary_aliquot_df = primary_aliquot_df.join(
            urls_df,
            ["file_id"],
            how="left",
        )
        file_urls = list(url.file_url for url in urls)

        return GeneExpressionPrimaryAliquotData(
            primary_aliquot_df=primary_aliquot_df, file_urls=file_urls
        )

    def _get_aliquot_level_df(self) -> sql.DataFrame:
        dated_query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "nested": {
                                "path": "cases.samples.portions.analytes.aliquots",
                                "query": {
                                    "exists": {
                                        "field": "cases.samples.portions.analytes.aliquots.created_datetime"
                                    }
                                },
                            }
                        },
                    ]
                }
            }
        }
        undated_query = {
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
                    ],
                    "must_not": [
                        {
                            "nested": {
                                "path": "cases.samples.portions.analytes.aliquots",
                                "query": {
                                    "exists": {
                                        "field": "cases.samples.portions.analytes.aliquots.created_datetime"
                                    }
                                },
                            }
                        },
                    ],
                }
            }
        }
        undated_included_fields = (
            "file_id",
            "cases.case_id",
            "cases.samples.sample_id",
            "cases.samples.portions.analytes.aliquots.aliquot_id",
        )
        dated_included_fields = tuple(itertools.chain(
            undated_included_fields,
            ("cases.samples.portions.analytes.aliquots.created_datetime",),
        ))

        if self.config.projects:
            project_clause = {
                "nested": {
                    "path": "cases",
                    "query": {
                        "terms": {"cases.project.project_id": self.config.projects}
                    },
                }
            }

            dated_query["query"]["bool"]["must"].append(project_clause)
            undated_query["query"]["bool"]["must"].append(project_clause)

        dated_df = _expand_aliquots(
            self._es_dataframe_util.get_dataframe(
                es_utils.Index.File,
                include_fields=dated_included_fields,
                query=dated_query,
            )
        ).select(
            "file_id",
            "case_id",
            "sample_id",
            "aliquot.aliquot_id",
            F.col("aliquot.created_datetime")
            .cast("timestamp")
            .alias("aliquot_created_datetime"),
        )
        undated_df = _expand_aliquots(
            self._es_dataframe_util.get_dataframe(
                es_utils.Index.File,
                include_fields=undated_included_fields,
                query=undated_query,
            )
        ).select(
            "file_id",
            "case_id",
            "sample_id",
            "aliquot.aliquot_id",
            F.lit(None).cast("timestamp").alias("aliquot_created_datetime"),
        )

        return dated_df.union(undated_df)

    def build_from_scratch(self, **kwargs: sql.DataFrame) -> sql.DataFrame:
        """
        Gets the file data associated with the best match sample for every
        case in the current processes configured project(s)

        Return:
            A data frame with the file data

            primary_aliquot{}
            |---case_id
            |---file_id
            |---experimental_strategy
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
                "entity_id",
                "entity",
                "case_id",
                "file_id",
                "experimental_strategy",
                "aliquot_id",
            )
        )
