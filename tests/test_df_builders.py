from pyspark.sql.functions import UserDefinedFunction, col
from exports.builders.df_builders import (
    get_annotation_df,
    get_gene_df,
    get_ssm_df
)

from utils import SparkTestCase
from exports.builders import MAFBuilder
from exports.builders.utils import struct_select
from pyspark.sql.types import BooleanType
from config import TestConfig
conf = TestConfig()


class TestDFBuilders(SparkTestCase):
    @classmethod
    def setUpClass(cls):
        super(TestDFBuilders, cls).setUpClass()
        cls.maf_df = MAFBuilder(conf, cls.sqlContext).build()

    def is_sub(self, subset, superset):
        result = True
        for item in subset.items():
            if type(item[1]) is dict:
                result = result and self.is_sub(item[1], superset)
            elif item not in superset:
                print 'item not in superset:', item
                if item[1] is not None:
                    result = False
        return result

    def assert_from_maf(self, maf_df, row, join_by):
        item = row.asDict(recursive=True)
        maf = maf_df.filter(
            col(join_by) == item[join_by]).first().asDict(recursive=True)
        assert self.is_sub(item, maf.items())

    def test_gene_df(self):
        # convert is_cancer_gene_census to True as our test self.mafs aren't
        # in the census list
        udf = UserDefinedFunction(lambda x: True, BooleanType())
        new_df = self.maf_df.withColumn(
                'is_cancer_gene_census',
                udf(self.maf_df.is_cancer_gene_census))
        gene_df = get_gene_df(new_df)
        self.assert_from_maf(new_df, gene_df.first(), 'gene_id')

    def test_df_drop_fields(self):
        gene_df = get_gene_df(self.maf_df, drop_fields=['cytoband', 'name'])
        assert 'cytoband' not in gene_df.columns
        assert 'name' not in gene_df.columns

    def test_unique_fields(self):
        ann_df = get_annotation_df(self.maf_df, unique_fields=['impact'])
        assert ann_df.count() < self.maf_df.count()

    def test_df_add_fields(self):
        gene_df = get_gene_df(self.maf_df, add_fields=['_case_submitter_id'])
        assert '_case_submitter_id' in gene_df.columns

    def test_annotation_df(self):
        ann_df = get_annotation_df(self.maf_df)
        annotation = ann_df.first()
        self.assert_from_maf(
            self.maf_df, annotation, 'transcript_id')

    def test_ssm_df(self):
        ssm_df = get_ssm_df(self.maf_df)
        self.assert_from_maf(
            self.maf_df, ssm_df.first(), 'ssm_id')
