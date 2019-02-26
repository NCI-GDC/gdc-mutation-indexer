import pytest
from exports.es_utils import (
    get_nested_field_by_value_query,
    get_es_doc_count,
)

from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('sqlContext', 'es_client',
                         'acl_maf_df', 'gistic_df', 'test_data',
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
            (except case and gene centric, which may have both acls)
    TODO: read in path to acl from mapper
    """

    @pytest.mark.parametrize('doc_type,nested_path,field', [
        ('gene_centric', 'case.ssm.observation', 'case.ssm.observation.acl'),
        ('case_centric', 'gene.ssm.observation', 'gene.ssm.observation.acl'),
        ('ssm_centric', 'occurrence.case.observation', 'occurrence.case.observation.acl'),
        ('ssm_occurrence_centric', 'case.observation', 'case.observation.acl')
    ])
    def test_acl_counts(self, es_client, doc_type, nested_path, field,
            acl_maf_df, gistic_df, test_data):

        index_name = conf.indices[doc_type]

        open_query = get_nested_field_by_value_query(
            field, nested_path, ['open']
        )
        open_docs = get_es_doc_count(
            es_client, index_name, doc_type, query=open_query
        )

        phs000218_query = get_nested_field_by_value_query(
            field, nested_path, ['phs000218']
        )
        phs000218_docs = get_es_doc_count(
            es_client, index_name, doc_type, query=phs000218_query
        )

        # case and genes may have multiple ssms with different acls;
        # check for docs where both acls are present
        overlap_query = {'query': {'bool': {'must': [
            open_query['query'], phs000218_query['query']
        ]}}}
        overlap_docs = get_es_doc_count(
            es_client, index_name, doc_type, query=overlap_query
        )

        if doc_type == 'case_centric':
            # Only consider cases or genes with ssms, as we use ssms as our
            # source of acls
            index_query = {'query': {'nested': {
                'path': 'gene.ssm',
                'query': {'exists': {'field': 'gene.ssm'}}
            }}}

        elif doc_type == 'gene_centric':
            index_query = {'query': {'nested': {
                'path': 'case.ssm',
                'query': {'exists': {'field': 'case.ssm'}}
            }}}

        else:
            # All ssms should have acls based on maf data
            index_query = None

            # It is rare for ssms to be read from multiple mafs, and our test
            # data does not have any ssms with multiple mafs. Therefore, no
            # ssms should have both acls present.
            #
            # NOTE: As we apply more granular acls in the future, this
            # assumption of one acl per ssm may not apply, but it should be
            # valid for now with our particular set of test data.
            assert not overlap_docs, (
                "{} docs have overlapping acls"
                .format(overlap_docs, index_name)
            )

        total_docs = get_es_doc_count(
            es_client, index_name, doc_type, query=index_query
        )

        assert open_docs + phs000218_docs - overlap_docs == total_docs, (
            "{} open docs + {} phs000218 docs - {} overlap != {} total docs"
            .format(open_docs, phs000218_docs, overlap_docs, total_docs)
        )
