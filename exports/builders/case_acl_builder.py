import logging

from exports.builders.base_input_builder import BaseInputBuilder
from exports.es_utils import iterate_es_results

from config import LOG_FORMAT

logging.basicConfig(format=LOG_FORMAT)


class CaseACLBuilder(BaseInputBuilder):
    """
    TODO: doc string
    """

    def __init__(self, config, sqlContext):
        super(CaseACLBuilder, self).__init__(config, sqlContext, 'case_acl')

    def build_from_scratch(self):
        """
        TODO:
        """
        return self.get_case_ids_from_source_es()

    def get_case_ids_from_source_es(self):
        """
        Reads aliquots from headers of mafs and queries source es
        for corresponding case_ids.

        This function _also_ returns the acls associated with the
        case_id -> aliquot -> maf_url -> maf_filename.

        1) if any of the observations is open then case level is open;
        2) if all observations are controlled
        and populated with the same dbgap study code,
        then case level will be the same dbgap study code;
        3) if study code in 2) have different values from observation,
        then there is something wrong.
        """
        self.logger.info('Building case_acl df from scratch')

        # Read unique aliquots from maf headers
        unique_aliquots, aliquot_to_url = self.get_aliquots_from_headers()

        query = {
            "_source": ["_id", "samples.portions.analytes.aliquots.submitter_id"],
            "query": {
                "nested": {
                    "path": "samples.portions.analytes.aliquots",
                    "query": {
                        "constant_score": {
                            "filter": {
                                "terms": {
                                    "samples.portions.analytes.aliquots.submitter_id": list(unique_aliquots)
                                }
                            }
                        }
                    }
                }
            }
        }

        results = iterate_es_results(
            self.config.es,
            self.config.graph_index,
            self.config.graph_document,
            query=query
        )

        cases_urls = {}
        case_ids = set()

        filenames_to_acls = self.config.acls

        # walk to the aliquot
        for hit in results:
            case_id = hit["_id"]
            case_ids.add(case_id)
            samples = hit['_source']['samples']
            aliquots_to_lookup = []
            for sample in samples:
                portions = sample['portions']
                for portion in portions:
                    analytes = portion['analytes']
                    for analyte in analytes:
                        aliquots = analyte['aliquots']
                        for aliquot in aliquots:
                            submitter_id = aliquot['submitter_id']
                            if submitter_id in unique_aliquots:
                                aliquots_to_lookup.append(submitter_id)

            # go from aliquots to url to maf_name to acl
            aliquot_acls = []
            for aliquot in aliquots_to_lookup:
                url = aliquot_to_url[aliquot]
                filename = self.config.maf_url_to_file_name(url)
                acl = filenames_to_acls[filename]
                aliquot_acls.append(acl)

            # dedupe (annoying because acls are lists)
            aliquot_acls = list(set(x for l in aliquot_acls for x in l))

            assert 0 < len(aliquot_acls) <= 2, 'Invalid acls ' \
                'for case {}, aliquot(s) {}, phsids {}' \
                ''.format(case_id, aliquots_to_lookup, aliquot_acls)

            # If only one acl across aliquots, use that
            if len(aliquot_acls) == 1:
                cases_urls[case_id] = aliquot_acls
            else:
                # If we find more than one acl, we must have
                # the scenario [open, phsid000x]
                # ([phsid000x, phsid000y] means something is wrong)
                assert u'open' in aliquot_acls, 'Multiple phsids ' \
                    'found for case {}, aliquots {}, phsids {}' \
                    ''.format(case_id, aliquots_to_lookup, aliquot_acls)

                cases_urls[case_id] = [u'open']

        # We found a url for each case
        assert len(case_ids) == len(cases_urls)

        # There may be more than one aliquot per case
        # I.e., the following example is valid:
        #
        # case 1: aliquot x, aliquot y
        # case 2: aliquot z
        #
        # (or)
        #
        # aliquot | case
        # --------------
        #    x    | 1
        #    y    | 1
        #    z    | 2
        assert len(unique_aliquots) >= len(case_ids)

        # Create a dataframe from case_ids set
        cases_df = self.sqlContext.createDataFrame(
            ((x, y) for x, y in cases_urls.items()), ['case_id', 'case_acl']
        )

        self.logger.info('Case df complete')

        return cases_df

    def get_aliquots_from_headers(self):
        """
        Reads a set of tuples of (unique aliquots, maf headers)
        """
        unique_aliquots = set()
        aliquot_to_url = {}
        for url in self.config.maf_urls:
            if str(url).endswith('maf') or str(url).endswith('maf.gz'):
                header = self.read_maf_header(url, n_lines=5).collect()
                header = map(lambda r: r.asDict().values()[0].split(), header)
                assert header[-2][0] == '#n.analyzed.samples'
                assert header[-1][0] == '#tumor.aliquots.submitter_id'
                aliquots = header[-1][1].split(',')
                n_aliquots = int(header[-2][1])

                assert len(aliquots) == n_aliquots, \
                    '{} has inconsistent aliquot data in header'.format(url)
                unique_aliquots.update(aliquots)
                for aliquot in aliquots:
                    aliquot_to_url[aliquot] = url

        return unique_aliquots, aliquot_to_url

    def read_maf_header(self, url, n_lines=5):
        """
        Reads only maf header
        TODO: needs to be reading from the same place as config. Is it?
        """
        print url
        return self.sqlContext.read.format('com.databricks.spark.csv')\
                              .options(delimiter='\t')\
                              .load(url).limit(n_lines)
