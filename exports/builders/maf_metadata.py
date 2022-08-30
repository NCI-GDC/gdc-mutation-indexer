from typing import Iterable, List, Sequence, Union

import elasticsearch
import more_itertools
from pyspark import sql
from pyspark.sql import functions as F
from typing_extensions import Literal, TypedDict

import config
from exports import es_utils
from exports.builders import primary_aliquot


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


_UNPRIORITIZED_STRATEGY = "__UNPRIORITIZED_STRATEGY__"


class MAFFileFilterFactory:
    def __init__(
        self, config: config.BaseConfig, es_client: elasticsearch.Elasticsearch
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
            index=self._config.graph_file_index,
            size=0,
            aggs=aggs,
            query={"bool": {"must": filters}},
        )["aggregations"]["cases"]["projects"]["buckets"]

    def _select_experimental_strategy(self, project: _ProjectBucket) -> str:
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
            self._config.maf_prioritized_experimental_strategies,
            pred=lambda s: s in strategies,
            default=_UNPRIORITIZED_STRATEGY,
        )

        if strategy == _UNPRIORITIZED_STRATEGY:
            self.logger.warning(
                f"Project: {project['key']} only has MAFs that are associated with unprioritized experimental strategies: {', '.join(strategies)}"
            )

        return strategy

    def _build_experimental_strategy_filter(
        self, filters: Sequence[dict], projects: Sequence[str]
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
        projects = self._get_project_strategy_aggregations(filters, projects)
        projects_by_strategy = more_itertools.map_reduce(
            projects,
            keyfunc=self._select_experimental_strategy,
            valuefunc=lambda project: project["key"],
        )
        _ = projects_by_strategy.pop(_UNPRIORITIZED_STRATEGY, None)

        if not projects_by_strategy:
            raise RuntimeError("Invalid Data: No projects associated with any MAF files.")

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

    def get_filters(self, projects: Sequence[str]) -> List[dict]:
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
        strategy_filter = self._build_experimental_strategy_filter(filters, projects)

        filters.append(strategy_filter)

        return filters


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
        file_filter_factory: MAFFileFilterFactory,
    ):
        """
        Args:
            config: The app configuration object
            sqlContext: The sql context object for the current pyspark run
            es_dataframe_util: The util for creating dataframes from data in
                elasticsearch
            es_client: The elasticsearch client for accessing the file index
        """
        super().__init__(
            config,
            sqlContext,
            es_dataframe_util,
            input_type="maf_metadata",
            additional_selections=("data_type", "workflow_type"),
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
        filters = self._file_filter_factory.get_filters(self.config.projects)

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
