import collections
import logging

from pyspark import sql
from pyspark.sql import types

from mutation_indexer import config
from mutation_indexer.builders import bases

logging.basicConfig(format=config.LOG_FORMAT)


# Information on an aliquot that was tested for mutations.
#
# Attributes:
#     submitter_id (str): The submitter ID of the aliquot.
#     maf_url (str): The URL of the MAF in which the aliquot was referenced.
#     project_id (optional(str)): The project ID associated with the aliquot, if given.
TestedAliquot = collections.namedtuple(
    "TestedAliquot", ["submitter_id", "maf_url", "project_id"]
)


class AliquotBuilder(bases.BaseInputBuilder):
    """
    Creates a list of aliquots from maf headers.
    Represents cases that have been tested for mutations.

    Aliquots go hand-in-hand with Maf:
    -If we build maf from scratch, we build aliquots from scratch.
    -If we read maf from cache, we read aliquots from cache. Etc.
    """

    PRAGMA_N_SAMPLES = "#n.analyzed.samples"
    PRAGMA_PROJECT_ID = "#project_id"
    PRAGMA_TUMOR_SUB_IDS = "#tumor.aliquots.submitter_id"

    def __init__(self, config, sqlContext):
        super().__init__(config, sqlContext, "aliquot")

    def build_from_scratch(self, **kwargs: sql.DataFrame) -> sql.DataFrame:
        return self.get_aliquots_from_headers()

    def get_aliquots_from_headers(self):
        """Read information on tested aliquots from the configured MAF headers.

        Returns a dataframe of `TestedAliquot`s.
        """
        self.logger.info("Building aliquot df from scratch")

        aliquots = []
        for url in self.config.maf_urls:
            header = self.read_maf_header(url, n_lines=10).collect()

            n_aliquots = -1
            submitter_ids = None
            project_id = None
            for row in header:
                row_text = row[0]

                if row_text.startswith(self.PRAGMA_N_SAMPLES):
                    n_aliquots = int(row_text.split()[1])
                    continue

                if row_text.startswith(self.PRAGMA_TUMOR_SUB_IDS):
                    submitter_ids = row_text.split()[1].split(",")
                    continue

                if row_text.startswith(self.PRAGMA_PROJECT_ID):
                    project_id = row_text.split()[1].strip()
                    continue

            if n_aliquots < 0 or submitter_ids is None:
                raise RuntimeError(
                    "Invalid MAF file. Missing required pragma comments: '{}' and '{}'".format(
                        self.PRAGMA_N_SAMPLES, self.PRAGMA_TUMOR_SUB_IDS
                    )
                )

            assert (
                len(submitter_ids) == n_aliquots
            ), "{} has inconsistent aliquot data in header".format(url)

            for submitter_id in submitter_ids:
                aliquot = TestedAliquot(
                    submitter_id=submitter_id, maf_url=url, project_id=project_id
                )
                aliquots.append(aliquot)

        schema_fields = [
            types.StructField(field, types.StringType())
            for field in TestedAliquot._fields
        ]
        schema = types.StructType(schema_fields)
        return self.sqlContext.createDataFrame(aliquots, schema)

    def read_maf_header(self, url, n_lines=10):
        """
        Reads only maf header
        """
        self.logger.debug(url)
        return self.sqlContext.read.text(url).limit(n_lines)
