import pytest
from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('sqlContext', 'es_client',
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

    @pytest.mark.parametrize('doc_type,index_name,path,field', [
        ('gene_centric', conf.indices['gene_centric'],
         'case.ssm.observation', 'case.ssm.observation.acl'),
        ('case_centric', conf.indices['case_centric'],
         'gene.ssm.observation', 'gene.ssm.observation.acl'),
        ('ssm_centric', conf.indices['ssm_centric'],
         'occurrence.case.observation', 'occurrence.case.observation.acl'),
        ('ssm_occurrence_centric', conf.indices['ssm_occurrence_centric'],
         'case.observation', 'case.observation.acl')
    ])
    def test_acl_counts(self, es_client, doc_type, index_name, field, path):

        def count_results(query):
            results = es_client.search(index=index_name,
                                       doc_type=doc_type,
                                       body=query)
            assert results['hits']['hits'], 'no results found'
            return results['hits']['total']

        def count_acl_value(value):
            query = {
                'size': 1,
                '_source': [field],
                'query': {
                    'bool': {
                        'should': [{
                            'bool': {
                                'must': {
                                    'nested': {
                                        'path': path,
                                        'query': {
                                            'terms': {
                                                field: value}}}}}}]
                            }
                        }
                    }
            return count_results(query)

        def count_not_value(value):
            query = {
                'size': 1,
                '_source': [field],
                'query': {
                    'bool': {
                        'should': [{
                            'bool': {
                                'must_not': {
                                    'nested': {
                                        'path': path,
                                        'query': {
                                            'terms': {
                                                field: value}}}}}}]
                            }
                        }
                    }
            return count_results(query)

        def count_total_docs():
            result = es_client.count(index=index_name, doc_type=doc_type)
            return result['count']

        open_docs = count_acl_value(['open'])
        phs000218_docs = count_acl_value(['phs000218'])
        not_open_docs = count_not_value(['open'])
        total_docs = count_total_docs()

        if doc_type != 'case_centric':
            # for case centric it is possible that this is not equal
            # if case has no ssms associated with it, then case.gene.ssm
            # (and case.gene) is null.
            # if so, then these docs are not_open but they do not have a value.
            assert not_open_docs == phs000218_docs, \
                   "{} non-open docs and {} " \
                   "phs000218 docs".format(not_open_docs, phs000218_docs)

        assert open_docs + phs000218_docs == total_docs, \
            "{} open docs + {} phs000218 docs != {} total docs".format(
                   open_docs, phs000218_docs, total_docs
               )
