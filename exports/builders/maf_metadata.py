import functools
import logging
from typing import Iterable, List, Sequence, Union

import elasticsearch
import more_itertools
from pyspark import sql
from pyspark.sql import functions as F
from typing_extensions import Literal, TypedDict

import config
from exports import es_utils
from exports.builders import bases, primary_aliquot
from exports.configuration import elasticsearch as es_config
from exports.configuration.builders import viz
from exports.constants import build

logger = logging.getLogger(__name__)

_UNPRIORITIZED_STRATEGY = "__UNPRIORITIZED_STRATEGY__"


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


class MAFFileFilterFactory:
    def __init__(
        self, config: es_config.Read, es_client: elasticsearch.Elasticsearch
    ) -> None:
        self._config = config
        self._es_client = es_client

    def _get_project_strategy_aggregations(
        self, filters: Sequence[dict], projects: Sequence[str]
    ) -> Sequence[_ProjectBucket]:
        """
        Performs an aggregation query in elasticsearch which returns all experimental
        strategies associated with the MAFs of each project the build is configured to
        run for.

        Args:
            filters: The filters being used to select the MAF files.
            projects: A subset of projects which should be the only ones included in
                the aggregations. If empty, all projects are included.

        Returns:
            A sequence of the aggregation buckets at the project level.
        """
        size = len(projects) if projects else 10_000
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

        if projects:
            filters = list(filters)
            projects_filter = {
                "nested": {
                    "path": "cases",
                    "query": {"terms": {"cases.project.project_id": projects}},
                }
            }

            filters.append(projects_filter)

        return self._es_client.search(
            index=self._config.file_index,
            size=0,
            aggs=aggs,
            query={"bool": {"must": filters}},
        )["aggregations"]["cases"]["projects"]["buckets"]

    def _select_experimental_strategy(
        self,
        prioritized_experimental_strategies: Iterable[str],
        project: _ProjectBucket,
    ) -> str:
        """
        Finds the highest priority experimental strategy associated with the project.

        Args:
            project: The project bucket from the doc_type aggregation by project.

        Returns:
            the experimental strategy that should be used for selecting the MAFs for a
            given project
        """
        strategies = frozenset(
            strategy["key"]
            for strategy in project["files"]["experimental_strategies"]["buckets"]
        )
        # selects the first and thus highest priority experimental stategy
        strategy = more_itertools.first_true(
            prioritized_experimental_strategies,
            pred=lambda s: s in strategies,
            default=_UNPRIORITIZED_STRATEGY,
        )

        if strategy == _UNPRIORITIZED_STRATEGY:
            logger.warning(
                f"Project: {project['key']} only has MAFs that are associated with unprioritized experimental strategies: {', '.join(strategies)}"
            )

        return strategy

    def _build_experimental_strategy_filter(
        self,
        filters: Sequence[dict],
        projects: Sequence[str],
        prioritized_experimental_strategies: Iterable[str],
    ) -> dict:
        """
        Creates an elasticsearch filter for selecting the correct MAFs for each project
        vis-a-vis experimental stragegy.

        Args:
            filters: all other filters that will be used to select the MAFs for all
                projects

        Returns:
            An elasticsearch query
        """
        strategy_selector = functools.partial(
            self._select_experimental_strategy, prioritized_experimental_strategies
        )
        project_buckets = self._get_project_strategy_aggregations(filters, projects)
        projects_by_strategy = more_itertools.map_reduce(
            project_buckets,
            keyfunc=strategy_selector,
            valuefunc=lambda project: project["key"],
        )
        _ = projects_by_strategy.pop(_UNPRIORITIZED_STRATEGY, None)

        if not projects_by_strategy:
            raise RuntimeError(
                "Invalid Data: No projects associated with any MAF files."
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
                ]
            }
        }

    def get_filters(
        self,
        projects: Sequence[str],
        prioritized_experimental_strategies: Iterable[str],
    ) -> List[dict]:
        """
        Builds the elasticsearch query filters to be used to select the MAF documents
        from the file index.

        Returns:
            A list of elasticsearch queries.
        """
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
        strategy_filter = self._build_experimental_strategy_filter(
            filters, projects, prioritized_experimental_strategies
        )

        filters.append(strategy_filter)

        return filters


class MAFMetadataInputs:
    pass


class MAFMetadataBuilder(
    bases.PrimaryAliquotBuilder[viz.MAFMetadataBuilder, MAFMetadataInputs]
):
    """
    An input builder for collecting the metadata associated with the MAF
    data that will be loaded as part of the build process.
    """

    __slots__ = ("_file_filter_factory",)

    def __init__(
        self,
        config: viz.MAFMetadataBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        file_filter_factory: MAFFileFilterFactory,
    ):
        """
        Args:
            config: The app configuration object
            spark_session: The spark session object for the current pyspark run
            es_dataframe_util: The util for creating dataframes from data in
                elasticsearch
            es_client: The elasticsearch client for accessing the file index
        """
        super().__init__(
            config,
            spark_session,
            es_dataframe_util=es_dataframe_util,
            additional_selections=("data_type", "workflow_type"),
            input_type=MAFMetadataInputs,
            output=build.DataFrame.MAF_METADATA,
        )

        self._file_filter_factory = file_filter_factory

    def _get_initial_weighted_df(
        self, query: dict, include_fields: Union[Iterable[str], Literal[True]]
    ) -> sql.DataFrame:
        return (
            super()
            ._get_initial_weighted_df(query, include_fields)
            .select("*", F.col("analysis.workflow_type").alias("workflow_type"))
        )

    def _build_from_scratch(self, input_dfs: MAFMetadataInputs) -> sql.DataFrame:
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
        filters = self._file_filter_factory.get_filters(
            self._config.projects, self._config.prioritized_experimental_strategies
        )

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
