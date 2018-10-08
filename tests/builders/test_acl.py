import pytest
from exports.es_utils import (
    get_nested_field_by_value_query,
    get_es_doc_count,
)

from utils.true_stats import TestDataStats
from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('sqlContext', 'es_client',
                         'maf_df', 'gistic_df', 'test_data',
                         'gene_centric_df',
                         'case_centric_df',
                         'ssm_centric_df',
                         'ssm_occurrence_centric_df')
class TestACL:
    """
    Some of the test maf acls are open and some are closed (['phs000218']).
    Verify that 1) all the relevant indices have acls present,
    that 2) they are either open or phs000218,
            i.e. that # of non-open == # of phs000218
            except for case_centric.
    TODO: read in path to acl from mapper
    """

    @pytest.mark.parametrize('doc_type,nested_path,field', [
        ('gene_centric', 'case.ssm.observation', 'case.ssm.observation.acl'),
        ('case_centric', 'gene.ssm.observation', 'gene.ssm.observation.acl'),
        ('ssm_centric', 'occurrence.case.observation', 'occurrence.case.observation.acl'),
        ('ssm_occurrence_centric', 'case.observation', 'case.observation.acl')
    ])
    def test_acl_counts(self, es_client, doc_type, nested_path, field,
            maf_df, gistic_df, test_data):

        index_name = conf.indices[doc_type]

        open_docs = get_es_doc_count(
            es_client, index_name, doc_type,
            query=get_nested_field_by_value_query(
                 field, nested_path, ['open']
            )
        )
        phs000218_docs = get_es_doc_count(
            es_client, index_name, doc_type,
            query=get_nested_field_by_value_query(
                 field, nested_path, ['phs000218']
            )
        )
        total_docs = get_es_doc_count(es_client, index_name, doc_type)

        if doc_type == 'gene_centric':
            # Test data contains gene[s] that have no ssms hence no acls
            stats = TestDataStats.get_stats(maf_df, gistic_df, test_data,
                                            doc_type)
            cnv_only_genes = stats['cnv_genes'] - stats['ssm_genes']
            total_docs = total_docs - len(cnv_only_genes)

        elif doc_type == 'case_centric':
            # Test data contains case[s] that have no ssms hence no acls
            stats = TestDataStats.get_stats(maf_df, gistic_df, test_data,
                                            doc_type)
            cnv_only_cases = stats['cnv_cases'] - stats['ssm_cases']
            total_docs = total_docs - len(cnv_only_cases)

        assert open_docs + phs000218_docs == total_docs, \
            "{} open docs + {} phs000218 docs != {} total docs".format(
                open_docs, phs000218_docs, total_docs
            )

