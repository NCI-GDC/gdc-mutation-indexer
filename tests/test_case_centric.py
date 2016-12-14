import unittest
import pytest

from config import TestConfig

conf = TestConfig

@pytest.mark.usefixtures('test_index')
class TestCaseCentric(unittest.TestCase):

    def setUp(self):
        self.case1 = self.es.get(conf.case_centric,
                                 conf.case_centric,
                                 '1bf54408-b5cb-45dc-ad03-ef2866a0ff59')
        print self.case1

    @pytest.mark.parametrize('doc,path', [
        (self.case1, 'submitter_id'),
        (self.case1, 'case_id'),
    ])
    def test_doc_contains(self, doc, path):
        results = parse(path).find(doc)
        assert len([r.value for r in results]) == 1
