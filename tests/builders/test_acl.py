import pytest
from exports.es_utils import (
    get_nested_field_by_value_query,
    get_es_doc_count,
)

from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('sqlContext', 'es_client',
                         'maf_df', 'gistic_df',
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
            maf_df, gistic_df):

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
            maf_genes = {r.gene_id for r in maf_df.collect()}
            cnv_genes = {r.gene_id for r in gistic_df.collect()}
            cnv_only_genes = {g for g in cnv_genes if g not in maf_genes}
            total_docs = total_docs - len(cnv_only_genes)

        assert open_docs + phs000218_docs == total_docs, \
            "{} open docs + {} phs000218 docs != {} total docs".format(
                open_docs, phs000218_docs, total_docs
            )

