from tests_config import TestConfig
from pyspark.sql.functions import col, size
from utils import SparkTestCase


from exports.builders import GeneCentricBuilder, MAFBuilder, CaseBuilder
conf = TestConfig()


class TestGeneCentricBuilder(SparkTestCase):
    ''' Test intermediate result from the case centric builder '''

    @classmethod
    def setUpClass(cls):
        super(TestGeneCentricBuilder, cls).setUpClass()
        cls.maf_df = MAFBuilder(conf, cls.sqlContext).build()
        cls.builder = GeneCentricBuilder(conf, cls.sqlContext)

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

    def test_case_with_gene_id(self):
        case_df = CaseBuilder(conf, self.sqlContext).build()
        case_gene_id = self.builder.build_case_with_gene_id(self.maf_df)
        assert set(['gene_id'] + case_df.columns) == set(case_gene_id.columns)
        assert case_gene_id.count() == 18

    def test_case_ssm(self):
        # one ssm per case
        case_ssm = self.builder.build_case_ssm(self.maf_df)
        assert case_ssm.filter(size('case.ssm') == 1).count() == 18

    def test_gene_case(self):
        # one case per gene
        gene_centric = self.builder.build().gene_centric
        assert gene_centric.filter(size('case') == 1).count() == 18
