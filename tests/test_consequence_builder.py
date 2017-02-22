
from tests_config import TestConfig
from utils import SparkTestCase

from pyspark.sql.functions import size, sum

from exports.builders import ConsequenceBuilder, MAFBuilder
conf = TestConfig()


class TestConsequenceBuilder(SparkTestCase):
    ''' Test intermediate result from the transcript builder '''

    @classmethod
    def setUpClass(cls):
        # TODO this should be setUpClass so we only build the maf once
        # Need to modify SparkTestCase to use setUpClass
        super(TestConsequenceBuilder, cls).setUpClass()
        cls.maf_df = MAFBuilder(conf, cls.sqlContext).build()
        cls.cons_builder = ConsequenceBuilder(conf, cls.sqlContext)

    def test_transcript_without_gene(self):
        tran_df = ConsequenceBuilder(conf, self.sqlContext)\
                                .build(self.maf_df, join_gene=False)

        tran_df = tran_df.where(tran_df.ssm_id == '1a191926-2c54-539a-817d-6196d105bb38')
        self.assertEqual(tran_df.count(), 1)
        cons = tran_df.collect()[0]['consequence']
        self.assertEqual(len(cons), 7)
        cons = [ r['transcript'] for r in cons ]
        self.assertTrue(all(['gene' not in t for t in cons]))
    
    def test_transcript_values(self):
        tran_df = ConsequenceBuilder(conf, self.sqlContext)\
                                .build(self.maf_df, join_gene=True)

        tran_df = tran_df.where(tran_df.ssm_id == '1a191926-2c54-539a-817d-6196d105bb38')
        self.assertEqual(tran_df.count(), 1)
        cons = tran_df.collect()[0]['consequence']
        self.assertEqual(len(cons), 7)
        cytobands = [ r['transcript']['gene']['cytoband'] for r in cons ]
        assert all([type(c) is list for c in cytobands ])
        

    def test_all_effects_cols(self):
        fields = [ 'do_not_use', 'consequence_type', 'aa_change',
                   'transcript_id', 'ref_seq_accession' ]
        ssm_trans = self.cons_builder._build_all_effects_cols(self.maf_df)

        for f in fields:
            assert f in ssm_trans.columns

        # this gene has 20 transcripts overall
        assert ssm_trans.filter(
            ssm_trans.gene_id == 'ENSG00000079841').count() == 16
