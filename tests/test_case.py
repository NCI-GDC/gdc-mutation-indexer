from utils import SparkTestCase
import pytest

from config import TestConfig
from exports.builders import CaseBuilder

conf = TestConfig


@pytest.mark.usefixtures('test_index')
class TestCase(SparkTestCase):

    def test_case_build(self):
        builder = CaseBuilder(conf, self.sqlContext)
        df = builder.build()
        self.assertEqual(df.count(), self.es.search(conf.graph_index,
                                                    conf.graph_document,
                                                    size=0)['hits']['total'])
