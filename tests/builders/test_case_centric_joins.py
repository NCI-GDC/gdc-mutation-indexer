import pytest
import json
from pyspark.sql.functions import explode

from exports.builders import ConsequenceBuilder, ObservationBuilder
from tests_config import TestConfig
from base_joins_test import BaseJoinsTest

conf = TestConfig()


@pytest.mark.usefixtures('sqlContext', 'maf_df', 'gistic_df', 'case_centric_df', 'ssm_transcript_df')
class TestCaseCentricJoins(BaseJoinsTest):
    """
    Test case_centric index joins

        case{}
             |___ gene[]
                     |___ ssm[]
                           |___ consequence[]
                           |             |_____ transcript{}
                           |                          |_____ annotation{}
                           |___ observation[]

    """

    def test_genes_per_case(self, maf_df, gistic_df, case_centric_df):
        # Genes per case built:
        df = self.unpack_df_list(case_centric_df, 'case_id', 'gene', 'gene_id')
        gpc = self.get_relationship_map(df, 'case_id', 'gene_id')

        # Genes per case expected:
        df = (
            maf_df.select('case_id', 'gene_id')
            .union(gistic_df.select('case_id', 'gene_id'))
        ).distinct()
        true_gpc = self.get_relationship_map(df, 'case_id', 'gene_id')

        assert gpc == true_gpc

    def test_ssm_per_gene(self, maf_df, case_centric_df):
        # SSMs per gene built:
        df = self.unpack_df_list(case_centric_df, 'case_id', 'gene',
                                 ['gene_id', 'ssm'])
        df = self.unpack_df_list(df, ['case_id', 'gene_id'], 'ssm', 'ssm_id')

        es_spg = {}
        for row in df.toJSON().collect():
            row = json.loads(row)
            es_spg.setdefault(row['case_id'], {})
            es_spg[row['case_id']].setdefault(row['gene_id'], set())
            es_spg[row['case_id']][row['gene_id']].update({row['ssm_id']})

        # SSMs per gene expected:
        spg = {}
        df = maf_df.select('gene_id', 'case_id', 'ssm_id')
        for row in df.toJSON().collect():
            row = json.loads(row)
            spg.setdefault(row['case_id'], {})
            spg[row['case_id']].setdefault(row['gene_id'], set())
            spg[row['case_id']][row['gene_id']].update({row['ssm_id']})

        assert es_spg == spg

    @pytest.mark.case_centric_ssm_subtree
    def test_ssm_subtree(self, sqlContext, maf_df, case_centric_df, case_ssm_subtree):
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
                   .build_for_ssm(maf_df, 'case_centric'))
        obs_df = ObservationBuilder().build_for_ssm(maf_df, 'case_centric',
                                                    selector='ssm')

        df = cons_df.join(obs_df, on=['ssm_id'], how='left')

        df = self.unpack_df_list(df, ['ssm_id', 'observation'],
                                 'consequence', 'consequence_id')
        df = self.unpack_df_list(df, ['ssm_id', 'consequence_id'],
                                 'observation', 'observation_id')
        stats = get_stats(df)

        # ssm_subtree stats built:
        df = self.unpack_df_list(case_ssm_subtree, [], 'ssm',
                                 ['ssm_id', 'consequence', 'observation'])
        df = self.unpack_df_list(df, ['ssm_id', 'observation'], 'consequence',
                                 ['consequence_id'])
        df = self.unpack_df_list(df, ['ssm_id', 'consequence_id'], 'observation',
                                 ['observation_id'])
        es_stats = get_stats(df)

        assert stats == es_stats
