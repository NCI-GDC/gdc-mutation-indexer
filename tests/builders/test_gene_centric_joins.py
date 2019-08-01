import pytest
import json

from exports.builders import ConsequenceBuilder, ObservationBuilder
from tests_config import TestConfig
from base_joins_test import BaseJoinsTest


conf = TestConfig()


@pytest.mark.usefixtures('maf_df', 'gistic_df', 'gene_centric_df', 'gene_ssm_subtree')
class TestGeneCentricJoins(BaseJoinsTest):
    """
    Test case_centric index joins

        gene{}
             |___ case[]
                     |___ ssm[]
                           |___ consequence[]
                           |             |_____ transcript{}
                           |                          |_____ annotation{}
                           |___ observation[]

    """

    def test_cases_per_gene(self, maf_df, gistic_df, gene_centric_df):
        # Cases per gene built:
        df = self.unpack_df_list(gene_centric_df, 'gene_id', 'case', 'case_id')
        cpg = self.get_relationship_map(df, 'gene_id', 'case_id')

        # Cases per gene expected:
        df = (
            maf_df.select('case_id', 'gene_id')
                  .union(
                    gistic_df.select('case_id', 'gene_id')
                  )
        ).distinct()
        true_cpg = self.get_relationship_map(df, 'gene_id', 'case_id')

        assert cpg == true_cpg

    def test_ssm_per_case(self, maf_df, gene_centric_df):
        # SSMs per case built:
        df = self.unpack_df_list(gene_centric_df, 'gene_id', 'case',
                                 ['case_id', 'ssm'])
        df = self.unpack_df_list(df, ['gene_id', 'case_id'], 'ssm', 'ssm_id')

        es_spc = {}
        for row in df.toJSON().collect():
            row = json.loads(row)
            es_spc.setdefault(row['gene_id'], {})
            es_spc[row['gene_id']].setdefault(row['case_id'], set())
            es_spc[row['gene_id']][row['case_id']].update({row['ssm_id']})

        # SSMs per case expected:
        spc = {}
        df = maf_df.select('gene_id', 'case_id', 'ssm_id')
        for row in df.toJSON().collect():
            row = json.loads(row)
            spc.setdefault(row['gene_id'], {})
            spc[row['gene_id']].setdefault(row['case_id'], set())
            spc[row['gene_id']][row['case_id']].update({row['ssm_id']})

        assert es_spc == spc

    @pytest.mark.gene_centric_ssm_subtree
    def test_ssm_subtree(self, sqlContext, maf_df, gene_centric_df, gene_ssm_subtree):
        def get_stats(dataframe):
            """
            Extracts ssm, consequence, transcript relationships from a flat dataframe
            """
            res = {}
            for row in dataframe.toJSON().collect():
                row = json.loads(row)
                sid = row['ssm_id']
                oid = row['observation_id']
                cid = row['consequence_id']

                res.setdefault(sid, {'consequences': set(), 'observations': set()})
                res[sid]['consequences'].update([cid])
                res[sid]['observations'].update([oid])

            return res

        # ssm_subtree stats expected:
        cons_df = (ConsequenceBuilder(conf, sqlContext)
                   .build_for_ssm(maf_df, 'gene_centric'))
        obs_df = ObservationBuilder().build_for_ssm(maf_df, 'gene_centric',
                                                    selector='ssm')

        df = cons_df.join(obs_df, on=['ssm_id'], how='left')

        df = self.unpack_df_list(df, ['ssm_id', 'observation'],
                                 'consequence', 'consequence_id')
        df = self.unpack_df_list(df, ['ssm_id', 'consequence_id'],
                                 'observation', 'observation_id')
        true_stats = get_stats(df)

        # ssm_subtree stats built:
        df = self.unpack_df_list(gene_ssm_subtree, [], 'ssm',
                                 ['ssm_id', 'consequence', 'observation'])
        df = self.unpack_df_list(df, ['ssm_id', 'observation'], 'consequence',
                                 ['consequence_id'])
        df = self.unpack_df_list(df, ['ssm_id', 'consequence_id'], 'observation',
                                 ['observation_id'])
        stats = get_stats(df)

        assert stats == true_stats

