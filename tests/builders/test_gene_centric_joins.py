import pytest
import json
from pyspark.sql.functions import explode

from exports.builders import ConsequenceBuilder, ObservationBuilder
from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('maf_df', 'gene_centric_df', 'gene_ssm_subtree')
class TestGeneCentricJoins:
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

    def test_cases_per_gene(self, maf_df, gene_centric_df):
        # Cases per gene built:
        df = (gene_centric_df.select('gene_id', explode('case').alias('case'))
                             .select('gene_id', 'case.case_id'))
        es_cpg = {}
        for row in df.toJSON().collect():
            row = json.loads(row)
            es_cpg.setdefault(row['gene_id'], set())
            es_cpg[row['gene_id']].update({row['case_id']})

        # Cases per gene expected:
        cpg = {}
        df = maf_df.select('case_id', 'gene_id').distinct()
        for row in df.toJSON().collect():
            row = json.loads(row)
            cpg.setdefault(row['gene_id'], set())
            cpg[row['gene_id']].update({row['case_id']})

        assert es_cpg == cpg

    def test_ssm_per_case(self, maf_df, gene_centric_df):
        # SSMs per case built:
        df = (gene_centric_df.select('gene_id', explode('case').alias('case'))
                             .select('gene_id', 'case.case_id',
                                     explode('case.ssm').alias('ssm'))
                             .select('gene_id', 'case_id', 'ssm.ssm_id'))
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
                   .build(maf_df, 'gene_centric'))
        obs_df = ObservationBuilder().build(maf_df, 'gene_centric')

        df = cons_df.join(obs_df, on=['ssm_id'], how='left')

        df = (df.select('ssm_id', 'observation',
                        explode('consequence').alias('c'))
                .select('ssm_id', 'c.consequence_id',
                        explode('observation').alias('o'))
                .select('ssm_id', 'consequence_id', 'o.observation_id'))

        stats = get_stats(df)

        # ssm_subtree stats built:
        df = (gene_ssm_subtree.select(explode('ssm').alias('s'))
                              .select('s.ssm_id', 's',
                                      explode('s.consequence').alias('c'))
                              .select('ssm_id', 'c.consequence_id',
                                      explode('s.observation').alias('o'))
                              .select('ssm_id', 'consequence_id', 'o.observation_id'))

        es_stats = get_stats(df)

        assert stats == es_stats

