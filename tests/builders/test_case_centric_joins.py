import pytest
import json
from pyspark.sql.functions import explode

from exports.builders import ConsequenceBuilder, ObservationBuilder
from tests_config import TestConfig

conf = TestConfig()


@pytest.mark.usefixtures('sqlContext', 'maf_df', 'case_centric_df', 'ssm_transcript_df')
class TestCaseCentricJoins:
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

    def test_genes_per_case(self, maf_df, case_centric_df):
        # Genes per case built:
        df = (case_centric_df.select('case_id', explode('gene').alias('gene'))
                             .select('case_id', 'gene.gene_id'))
        es_gpc = {}
        for row in df.toJSON().collect():
            row = json.loads(row)
            es_gpc.setdefault(row['case_id'], set([]))
            es_gpc[row['case_id']].update({row['gene_id']})

        # Genes per case expected:
        gpc = {}
        df = maf_df.select('case_id', 'gene_id').distinct()
        for row in df.toJSON().collect():
            row = json.loads(row)
            gpc.setdefault(row['case_id'], set([]))
            gpc[row['case_id']].update({row['gene_id']})

        assert es_gpc == gpc

    def test_ssm_per_gene(self, maf_df, case_centric_df):
        # SSMs per gene built:
        df = (case_centric_df.select('case_id', explode('gene').alias('gene'))
                             .select('case_id', 'gene.gene_id',
                                     explode('gene.ssm').alias('ssm'))
                             .select('gene_id', 'case_id', 'ssm.ssm_id'))
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
                   .build(maf_df, 'case_centric'))
        obs_df = ObservationBuilder().build(maf_df, 'case_centric')

        df = cons_df.join(obs_df, on=['ssm_id'], how='left')

        df = (df.select('ssm_id', 'observation',
                        explode('consequence').alias('c'))
                .select('ssm_id', 'c.consequence_id',
                        explode('observation').alias('o'))
                .select('ssm_id', 'consequence_id', 'o.observation_id'))

        stats = get_stats(df)

        # ssm_subtree stats built:
        df = (case_ssm_subtree.select(explode('ssm').alias('s'))
                              .select('s.ssm_id', 's',
                                      explode('s.consequence').alias('c'))
                              .select('ssm_id', 'c.consequence_id',
                                      explode('s.observation').alias('o'))
                              .select('ssm_id', 'consequence_id', 'o.observation_id'))

        es_stats = get_stats(df)

        assert stats == es_stats

