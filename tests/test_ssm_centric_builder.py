import os
from pyspark.sql.functions import col, size
from utils import TestJsonObject

from exports.builders import SSMCentricBuilder



class TestSSMCentricBuilder(TestJsonObject):
    ''' Test intermediate result from the case centric builder '''

    @classmethod
    def setUpClass(cls):
        cls.builder = SSMCentricBuilder
        super(TestSSMCentricBuilder, cls).setUpClass()

    def test_flat(self):
        ssm = self.builder.build(self.maf_df).ssm_centric
        assert ssm.count() == 18
        assert ssm.filter(size(col('observation')) == 1).count() == 18
        assert (
            ssm.filter(size(col('consequence')) == 17)
            .filter(ssm.gene_id == 'ENSG00000029363').count()) == 1
        assert (
            ssm.filter(size(col('consequence')) == 16)
            .filter(ssm.gene_id == 'ENSG00000079841').count()) == 1

    def test_deep(self):
        ssm_dir = os.path.join(self.T.conf.output_dir, self.T.index)
        for file in os.listdir(ssm_dir):
            es_doc, true_doc = self.T.get_docs_to_compare_new(self.T.index_generator(self.sqlContext), file)
            self.validate_transcript_list(es_doc['consequence'], true_doc['consequence'])
            self.validate_case_list(es_doc['occurrence'], true_doc['occurrence'])
            self.validate_two_nested_jsons(es_doc, true_doc, ignore_list=['consequence', 'occurrence'])
