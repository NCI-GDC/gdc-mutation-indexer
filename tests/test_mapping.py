import json
import pytest
from jsonpath_rw import parse

from exports.mappings import Mapper, GeneMapper


@pytest.fixture(scope="session")
def mappings():
    gene_mapper = GeneMapper()
    return {
        'gene': gene_mapper.mapping,
    }


@pytest.mark.parametrize('doc_type,path', [
    ('gene', '_all'),
    ('gene', '_id'),
    ('gene', 'dynamic'),
    ('gene', 'properties.case'),
    ('gene', 'properties.case.properties.case_id'),
    ('gene', 'properties.case.properties.demographic'),
    ('gene', 'properties.case.properties.diagnoses'),
    ('gene', 'properties.case.properties.project'),
    ('gene', 'properties.case.properties.project.properties.project_id'),
    ('gene', 'properties.case.properties.project.properties.program'),
    ('gene', 'properties.case.properties.ssm'),
    ('gene', 'properties.case.properties.ssm.properties.consequence'),
    ('gene', 'properties.case.properties.ssm.properties.consequence.properties.transcript'),
    ('gene', 'properties.case.properties.ssm.properties.consequence.properties.transcript.properties.annotation'),
    ('gene', 'properties.case.properties.ssm.properties.observation'),
])
def test_mapping_contains(mappings, doc_type, path):
    results = parse(path).find(mappings[doc_type])
    assert len([r.value for r in results]) == 1
