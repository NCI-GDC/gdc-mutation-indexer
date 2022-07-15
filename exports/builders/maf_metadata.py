import logging
from typing import Any, Dict, Iterable, List, NamedTuple, Sequence, Tuple, Union

import elasticsearch
import more_itertools
from pyspark import sql
from pyspark.sql import functions as F
from typing_extensions import Literal, TypedDict

import config
from exports import es_utils
from exports.builders import primary_aliquot

logger = logging.getLogger(__name__)


class _ExperimentalStrategiesBucket(TypedDict):
    key: str
    doc_count: int


class _ExperimentalStrategies(TypedDict):
    doc_count_error_upper_bound: int
    sum_other_doc_count: int
    buckets: Iterable[_ExperimentalStrategiesBucket]


class _Files(TypedDict):
    doc_count: int
    experimental_strategies: _ExperimentalStrategies


class _ProjectBucket(TypedDict):
    key: str
    doc_count: int
    files: _Files


class MAFMetadataBuilder(primary_aliquot.BasePrimaryAliquotBuilder):
    """
    An input builder for collecting the metadata associated with the MAF
    data that will be loaded as part of the build process.
    """

    def __init__(
        self,
        config: config.BaseConfig,
        sqlContext: sql.SQLContext,
        es_dataframe_util: es_utils.DataFrameUtil,
        es_client: elasticsearch.Elasticsearch,
    ):
        """
        Args:
            config: The app configuration object
            sqlContext: The sql context object for the current pyspark run
            es_dataframe_util: The util for creating dataframes from data in
                elasticsearch
        """
        super().__init__(
            config,
            sqlContext,
            es_dataframe_util,
            input_type="maf_metadata",
            additional_selections=("data_type", "workflow_type"),
        )

        self._es_client = es_client

    def _get_project_strategy_aggregations(
        self, filters: Sequence[dict]
    ) -> Sequence[_ProjectBucket]:
        size = len(self.config.projects) if self.config.projects else 10_000
        aggs = {
            "cases": {
                "nested": {"path": "cases"},
                "aggs": {
                    "projects": {
                        "terms": {
                            "field": "cases.project.project_id",
                            "size": size,
                        },
                        "aggs": {
                            "files": {
                                "reverse_nested": {},
                                "aggs": {
                                    "experimental_strategies": {
                                        "terms": {"field": "experimental_strategy"}
                                    }
                                },
                            }
                        },
                    }
                },
            }
        }

        if self.config.projects:
            filters = list(filters)
            projects_filter = {
                "nested": {
                    "path": "cases",
                    "query": {
                        "terms": {"cases.project.project_id": self.config.projects}
                    },
                }
            }

            filters.append(projects_filter)

        return self._es_client.search(
            index=self.config.graph_file_index,
            size=0,
            aggs=aggs,
            query={"bool": {"must": filters}},
        )["aggregations"]["cases"]["projects"]["buckets"]

    def _build_experimental_strategy_filter(self, filters: Sequence[dict]) -> dict:
        def get_strategy(project: _ProjectBucket) -> str:
            strategies = frozenset(
                strategy["key"]
                for strategy in project["files"]["experimental_strategies"]["buckets"]
            )

            if "WXS" in strategies:
                return "WXS"

            elif "Targeted Sequencing" in strategies:
                return "Targeted Sequencing"

            logger.warning(
                f"Project: {project['key']} has MAFs that are associated with unknown experimental strategies: {', '.join(strategies)}"
            )
            return "UNKNOWN"

        projects = self._get_project_strategy_aggregations(filters)
        projects_by_strategy = more_itertools.map_reduce(
            projects, keyfunc=get_strategy, valuefunc=lambda project: project["key"]
        )

        return {
            "bool": {
                "should": [
                    {
                        "bool": {
                            "must": [
                                {"term": {"experimental_strategy": strategy}},
                                {
                                    "nested": {
                                        "path": "cases",
                                        "query": {
                                            "terms": {
                                                "cases.project.project_id": projects
                                            }
                                        },
                                    }
                                },
                            ]
                        },
                    }
                    for strategy, projects in projects_by_strategy.items()
                    if strategy in ("WXS", "Targeted Sequencing")
                ]
            }
        }

    def _build_file_filters(self) -> List[Dict[str, Any]]:
        aesvmm_workflow = {
            "bool": {
                "must": [
                    {"term": {"data_format": "MAF"}},
                    {"term": {"data_type": "Masked Somatic Mutation"}},
                    {
                        "term": {
                            "analysis.workflow_type": "Aliquot Ensemble Somatic Variant Merging and Masking"
                        }
                    },
                ]
            }
        }
        fvam_workflow = {
            "bool": {
                "must": [
                    {"term": {"data_format": "MAF"}},
                    {"term": {"data_type": "Aggregated Somatic Mutation"}},
                    {
                        "term": {
                            "analysis.workflow_type": "FoundationOne Variant Aggregation and Masking"
                        }
                    },
                ]
            }
        }
        filters: List[dict] = [{"bool": {"should": [aesvmm_workflow, fvam_workflow]}}]
        strategy_filter = self._build_experimental_strategy_filter(filters)

        filters.append(strategy_filter)
        logger.info(f"MAF Metadata ran filters: {filters}")

        return filters

    def _get_initial_weighted_df(
        self, query: dict, include_fields: Union[Iterable[str], Literal[True]]
    ) -> sql.DataFrame:
        return (
            super()
            ._get_initial_weighted_df(query, include_fields)
            .select("*", F.col("analysis.workflow_type").alias("workflow_type"))
        )

    def build_from_scratch(self, **kwargs: sql.DataFrame) -> sql.DataFrame:
        """
        Gets the maf file data (file_id, workflow_type, and data_type) and its
        associated case id.

        Returns:
            The below data frame

            maf_metadata{}
            |---case_id
            |---data_type
            |---file_id
            +---workflow_type
        """
        filters = self._build_file_filters()

        return self._get_primary_aliquot_df(
            filters,
            frozenset(("case",)),
            include_fields=("data_type", "analysis.workflow_type"),
        ).select(
            "case_id",
            "data_type",
            "file_id",
            "workflow_type",
        )
