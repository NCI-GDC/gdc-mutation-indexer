import logging

from collections import namedtuple
from itertools import chain
from typing import Iterable, Mapping, Optional, TypeVar

from pyspark import sql
from pyspark.sql import functions as f

from config import BaseConfig
from exports import es_utils
from indexclient import client as indexclient


GeneExpressionPrimaryAliquotData = namedtuple(
    "GeneExpressionPrimaryAliquotData", ["primary_aliquot_df", "file_urls"]
)


T = TypeVar('T')


class PrimaryAliquotBuilder:

    FILE_URL_BATCH_SIZE = 1000

    def __init__(self, config: BaseConfig, sql_context: sql.SQLContext):
        self.sql_context = sql_context
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def _sample_weight():
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
        when_clause = f.when(f.lit(1) != f.lit(1), 0)

        for sample_type, weight in weights:
            when_clause = when_clause.when(
                f.col("sample_type") == sample_type, weight)

        return when_clause.otherwise(len(weights) + 1).alias("sample_weight")

    def _get_primary_aliquots(self, filters: Iterable[dict], include_fields: Iterable[str] = None):
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
        query = {
            "query": {
                "bool": {
                    "must": filters
                }
            }
        }
        include_fields = {
            "file_id",
            "created_datetime",
            "experimental_strategy",
            "cases.case_id",
            "cases.samples.sample_type"
        }.union(include_fields or [])
        files_df = es_utils.get_dataframe_from_es(
            self.sql_context,
            self.config,
            self.config.graph_file_index,
            include_fields=include_fields,
            query=query
        )

        weighted_files_df = files_df.select(
            "file_id",
            f.col("created_datetime").cast("timestamp"),
            "experimental_strategy",
            f.explode("cases").alias("case")
        ).select(
            "file_id",
            "created_datetime",
            "experimental_strategy",
            f.col("case.case_id").alias("case_id"),
            "case",
            f.explode("case.samples").alias("sample")
        ).select(
            "file_id",
            "created_datetime",
            "experimental_strategy",
            "case_id",
            "case",
            "sample.sample_type",
        ).select(
            "file_id",
            "created_datetime",
            "experimental_strategy",
            "case_id",
            "case",
            self._sample_weight(),
        )

        case_window = sql.Window().partitionBy("case_id").orderBy(
            f.col("sample_weight"),
            f.col("created_datetime"),
            f.col("file_id"),
        )

        return weighted_files_df.withColumn(
            "row_number", f.row_number().over(case_window)
        ).where(
            f.col("row_number") == 1
        ).select(
            "file_id",
            "created_datetime",
            "experimental_strategy",
            "case_id",
            "case",
        )

    def _is_main_url(self, metadata: dict):
        """
        Check if given metadata corresponds to main IndexD URL:
            * type == cleversafe
            * state == validated

        Returns:
            bool: True if main URL, False otherwise
        """
        return (
            metadata.get("type") == "cleversafe" and
            metadata.get("state") == "validated"
        )

    def _get_and_format_url(self, doc: indexclient.Document) -> Optional[str]:
        """
        Select main IndexD url if one exist and format it to something that Spark
        understands

        Args:
            doc: IndexD document to extract URL from

        Returns:
            str: formatted main URL
        """
        for url, meta in doc.urls_metadata.items():
            if self._is_main_url(meta):
                url = url.replace(
                    "s3://", "s3a://").replace("cleversafe.service.consul/", "")
                return url
        return None

    @staticmethod
    def _batch(iterable: Iterable[T], n: int) -> Iterable[Iterable[T]]:
        """
        Groups the iterable into batches of n.

        Args:
            iterable: the iterable to be batched
            n: the number of elements in each batch

        Returns:
            An iterable of batches where each batch is an iterable itself
            with no more than n items.

        """
        iterator = iter(iterable)

        def batch(first):
            try:
                yield first

                for _ in range(1, n):
                    yield next(iterator)

            except StopIteration:
                return

        while True:
            try:
                first = next(iterator)

                yield batch(first)

            except StopIteration:
                return

    def _get_main_urls(self, file_ids: Iterable[str]) -> Iterable[Mapping[str, str]]:
        """
        Construct iterable of {file_id, url}, where url is a main URL associated with the given file_id

        Args:
            file_ids: a list of file_ids

        Returns:
            A generator where each mapping is
            {file_id: x, file_url: u}
        """
        batches = self._batch(file_ids, self.FILE_URL_BATCH_SIZE)
        docs = chain.from_iterable(
            self.config.indexd.bulk_request(list(bids)) for bids in batches
        )

        for doc in docs:
            url = self._get_and_format_url(doc)

            if url is None:
                self.logger.warning("File is missing: '{}'".format(doc.did))

            else:
                yield {"file_id": doc.did, "file_url": url}

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
            {"terms": {"analysis.workflow_type": workflow_types}}
        ]
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
            filters.append({
                "nested": {
                    "path": "cases",
                    "query": {"terms": {"cases.project.project_id": self.config.projects}},
                }
            })

        primary_aliquot_df = self._get_primary_aliquots(
            filters,
            include_fields=case_fields,
        )
        file_ids = (r.file_id for r in primary_aliquot_df.select(
            "file_id").distinct().collect())

        urls = tuple(self._get_main_urls(file_ids))
        urls_df = self.sql_context.createDataFrame(urls)

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
        file_urls = list(u["file_url"] for u in urls)

        return GeneExpressionPrimaryAliquotData(
            primary_aliquot_df=primary_aliquot_df,
            file_urls=file_urls
        )

    def build_primary_aliquots_for_project(self):
        """
        Gets the file data associated with the best match sample for every
        case in the current processes configured project(s)

        Args:
            sql_context(pyspark.sql.SQLContext): The spark sql context
            config: The configuration for the current process

        Return:
            A data frame with the file data

            primary_aliquot{}
            |---case_id
            |---file_id
            |---experimental_strategy
        """
        filters = [
            {
                "nested": {
                    "path": "cases",
                    "query": {"terms": {"cases.project.project_id": self.config.projects}},
                }
            },
        ]

        return self._get_primary_aliquots(filters).select(
            "case_id",
            "file_id",
            "experimental_strategy"
        )
