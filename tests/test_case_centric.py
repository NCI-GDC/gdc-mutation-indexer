import unittest
import pytest

from config import TestConfig

conf = TestConfig

@pytest.mark.parametrize('doc,path', [
    ('1bf54408-b5cb-45dc-ad03-ef2866a0ff59','case_id')
])
def test_doc_contains(test_index, doc, path):
    
    d = test_index.get(conf.case_centric,
                             conf.case_centric,
                             doc)
    results = parse(path).find(d)
    assert len([r.value for r in results]) == 1
