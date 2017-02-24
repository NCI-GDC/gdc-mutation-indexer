from pyspark.sql.functions import UserDefinedFunction

from utils import SparkTestCase
from exports.builders import MAFBuilder
from exports.builders.utils import struct_select
from pyspark.sql.types import BooleanType
from config import TestConfig


class TestGeneDataFrame(SparkTestCase):
    def is_sub(self, subset, superset):
        result = True
        for item in subset.items():
            if type(item[1]) is dict:
                result = result and self.is_sub(item[1], superset)
            elif item not in superset:
                print item
                result = False
        return result

    def test_gene_mapping(self):
        # test all values are mapped
        builder = MAFBuilder(TestConfig(), self.sqlContext)
        maf_df = builder.build()

        # convert is_cancer_gene_census to True as our test mafs aren't
        # in the census list
        udf = UserDefinedFunction(lambda x: True, BooleanType())
        new_df = maf_df.withColumn(
                'is_cancer_gene_census',
                udf(maf_df.is_cancer_gene_census))

        gene_df = new_df.select(
            *struct_select('gene.yml', ignore=['transcripts']))
        gene = gene_df.first().asDict(recursive=True)
        maf = new_df.filter(
            new_df.gene_id == gene['gene_id']).first().asDict(recursive=True)
        assert self.is_sub(gene, maf.items())
