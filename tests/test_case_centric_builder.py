from config import TestConfig
from pyspark.sql.functions import col, size
from utils import SparkTestCase


from exports.builders import CaseCentricBuilder, MAFBuilder
conf = TestConfig()


class TestCaseCentricBuilder(SparkTestCase):
    ''' Test intermediate result from the case centric builder '''

    @classmethod
    def setUpClass(cls):
        super(TestCaseCentricBuilder, cls).setUpClass()
        cls.maf_df = MAFBuilder(conf, cls.sqlContext).build()
        cls.builder = CaseCentricBuilder(conf, cls.sqlContext)

    def test_ssm(self):
        ssm = self.builder.build_ssm(self.maf_df)
        assert ssm.count() == 18
        assert ssm.filter(size(col('observation')) == 1).count() == 18
        assert (
            ssm.filter(size(col('consequence')) == 17)
            .filter(ssm.gene_id == 'ENSG00000029363').count()) == 1
        assert (
            ssm.filter(size(col('consequence')) == 16)
            .filter(ssm.gene_id == 'ENSG00000079841').count()) == 1

    def test_gene(self):
        # one gene per ssm for our test mafs
        gene_df = self.builder.build_gene(self.maf_df)
        assert (
            gene_df.filter(size('gene.ssm') == 1).count()) == 18

    def test_case(self):
        case_df = self.builder.build().case_centric
        assert (
            case_df.filter(size('gene') == 1).count()) == 6
        assert (
            case_df.filter(size('gene') == 9)
            .filter(case_df.submitter_id == 'TCGA-A4-A6HP').count()) == 1
