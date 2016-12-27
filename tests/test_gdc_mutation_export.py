import pytest
from elasticsearch import Elasticsearch
from utils import SparkTestCase
from config import TestConfig
from exports import GDCMutationExport


@pytest.mark.usefixtures('test_index_class')
class TestGDCMutationExport(SparkTestCase):

    def setUp(self):
        super(TestGDCMutationExport, self).setUp()
        self.exporter = GDCMutationExport(self.sc, self.sqlContext, TestConfig)
