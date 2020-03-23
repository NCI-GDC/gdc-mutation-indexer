import logging

from exports.builders.base_input_builder import BaseInputBuilder

from config import LOG_FORMAT

logging.basicConfig(format=LOG_FORMAT)


class AliquotBuilder(BaseInputBuilder):
    """
    Creates a list of aliquots from maf headers.
    Represents cases that have been tested for mutations.

    Aliquots go hand-in-hand with Maf:
    -If we build maf from scratch, we build aliquots from scratch.
    -If we read maf from cache, we read aliquots from cache. Etc.
    """

    PRAGMA_N_SAMPLES = '#n.analyzed.samples'
    PRAGMA_TUMOR_SUB_IDS = '#tumor.aliquots.submitter_id'

    def __init__(self, config, sqlContext):
        super(AliquotBuilder, self).__init__(config, sqlContext, 'aliquot')

    def build_from_scratch(self):
        return self.get_aliquots_from_headers()

    def get_aliquots_from_headers(self):
        """
        Reads a set of tuples of (unique aliquots, maf headers)
        """
        self.logger.info('Building aliquot df from scratch')

        unique_aliquots = set()
        aliquot_to_url = {}
        for url in self.config.maf_urls:
            header = self.read_maf_header(url, n_lines=5).collect()

            n_aliquots = -1
            aliquots = None
            for row in header:
                if row[0].startswith(self.PRAGMA_N_SAMPLES):
                    n_aliquots = int(row[0].split()[1])
                    continue

                if row[0].startswith(self.PRAGMA_TUMOR_SUB_IDS):
                    aliquots = row[0].split()[1].split(',')
                    break

            if n_aliquots < 0 or aliquots is None:
                raise RuntimeError(
                    "Invalid MAF file. Missing required pragma comments: "
                    "'{}' and '{}'".format(self.PRAGMA_N_SAMPLES, self.PRAGMA_TUMOR_SUB_IDS)
                )

            assert len(aliquots) == n_aliquots, \
                '{} has inconsistent aliquot data in header'.format(url)
            unique_aliquots.update(aliquots)
            for aliquot in aliquots:
                aliquot_to_url[aliquot] = url

        # Create a dataframe from aliquot_ids set
        aliquot_df = self.sqlContext.createDataFrame(
            ((x, y) for x, y in aliquot_to_url.items()), ['aliquot_id', 'url']
        )
        return aliquot_df

    def read_maf_header(self, url, n_lines=5):
        """
        Reads only maf header
        """
        self.logger.debug(url)
        return self.sqlContext.read.format('com.databricks.spark.csv')\
                              .options(delimiter='\t')\
                              .load(url).limit(n_lines)
