from config import TestConfig
from utils import SparkTestCase

from pyspark.sql.functions import size, sum

from exports.builders import TranscriptBuilder, MAFBuilder
conf = TestConfig()


class TestTranscriptBuilder(SparkTestCase):
    ''' Test intermediate result from the transcript builder '''

    @classmethod
    def setUpClass(cls):
        super(TestTranscriptBuilder, cls).setUpClass()
        cls.maf_df = MAFBuilder(conf, cls.sqlContext).build()
        cls.trans_builder = TranscriptBuilder(conf, cls.sqlContext)

    def test_transcript_without_gene(self):
        tran_df = TranscriptBuilder(conf, self.sqlContext)\
                                .build(self.maf_df, join_gene=False)

        tran_df = tran_df.where(tran_df.ssm_id == '1a191926-2c54-539a-817d-6196d105bb38')
        self.assertEqual(tran_df.count(), 1)
        cons = tran_df.collect()[0]['consequence']
        self.assertEqual(len(cons), 7)
        trans = [ r['transcript'] for r in cons ]
        self.assertTrue(all(['gene' not in t for t in trans]))
    
    def test_transcript_values(self):
        tran_df = TranscriptBuilder(conf, self.sqlContext)\
                                .build(self.maf_df, join_gene=True)

        tran_df = tran_df.where(tran_df.ssm_id == '1a191926-2c54-539a-817d-6196d105bb38')
        self.assertEqual(tran_df.count(), 1)
        cons = tran_df.collect()[0]['consequence']
        self.assertEqual(len(cons), 7)
        trans = [ r['transcript'] for r in cons ]

    def test_exploded_ssm_tran(self):
        ssm_trans = self.trans_builder._build_ssm_tran(self.maf_df)
        # this gene has 20 transcripts overall
        assert ssm_trans.filter(
            ssm_trans.gene_id == 'ENSG00000079841').count() == 16
