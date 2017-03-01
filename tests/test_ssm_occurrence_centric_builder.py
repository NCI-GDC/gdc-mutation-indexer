from tests_config import TestConfig
from pyspark.sql.functions import col, size
from utils import SparkTestCase


from exports.builders import SSMOccurrenceCentricBuilder, MAFBuilder
conf = TestConfig()


class TestSSMOccurrenceCentricBuilder(SparkTestCase):
    ''' Test intermediate result from the ssm occurrence centric builder

    ssm_occurrence{}
          |____ ssm{}
          |        |____ consequence[]
          |                     |_____ transcript{}
          |                                   |_____ gene{}
          |                                   |_____ annotation{}
          |____ case{}
                   |____ observation[]
    '''


    @classmethod
    def setUpClass(cls):
        super(TestSSMOccurrenceCentricBuilder, cls).setUpClass()
        cls.maf_df = MAFBuilder(conf, cls.sqlContext).build()
        cls.builder = SSMOccurrenceCentricBuilder(conf, cls.sqlContext)

    def test_ssm(self):
        ssm = self.builder.build_ssm(self.maf_df)
        assert ssm.count() == 18
        assert (
            ssm.filter(size(col('consequence')) == 17)
            .filter(ssm.ssm_id == 'ed0bbbd9-4f0d-5697-bd6c-0cbd72fc6bd0').count()) == 1
        assert (
            ssm.filter(size(col('consequence')) == 16)
            .filter(ssm.ssm_id == '0d33430c-c896-53c6-9b5f-c1ac68dcc132').count()) == 1

    def test_case(self):
        # one gene per ssm for our test mafs
        case_df = self.builder.build_case(self.maf_df)
        assert case_df.count() == 18
        assert (
            case_df.filter(size('observation') == 1).count()) == 18

    def test_ssm_occurrence(self):
        ssm_occurrence_df = self.builder.build().ssm_occurrence_centric
        assert ssm_occurrence_df.count() == 18
        assert (
            ssm_occurrence_df.filter(size(col('case.observation')) == 1).count()) == 18
        assert (
            ssm_occurrence_df
            .filter(size(col('case.observation')) == 1)
            .filter(col('ssm_occurrence_id') == '4d96cd1a-d679-52ba-bfc3-fe14cceb6d24').count()) == 1
