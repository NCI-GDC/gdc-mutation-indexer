import json
import pytest
from jsonpath_rw import parse

from exports.mappers import (
    Mapper,
    GeneMapper,
    SSMMapper,
    SSMOccurrenceMapper,
    CaseMapper,
)


@pytest.fixture(scope="session")
def mappings():
    gene_mapper = GeneMapper()
    ssm_mapper = SSMMapper()
    ssm_occurrence_mapper = SSMOccurrenceMapper()
    case_mapper = CaseMapper()
    return {
        'gene': gene_mapper.mapping,
        'ssm': ssm_mapper.mapping,
        'ssm_occurrence': ssm_occurrence_mapper.mapping,
        'case': case_mapper.mapping
    }


@pytest.mark.parametrize('doc_type,path', [
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
    ('ssm', 'dynamic'),
    ('ssm', 'properties.consequence'),
    ('ssm', 'properties.consequence.properties.transcript'),
    ('ssm', 'properties.consequence.properties.transcript.properties.gene'),
    ('ssm', 'properties.consequence.properties.transcript.properties.annotation'),
    ('ssm', 'properties.occurrence'),
    ('ssm', 'properties.occurrence.properties.case'),
    ('ssm', 'properties.occurrence.properties.case.properties.observation'),
    ('ssm_occurrence', 'dynamic'),
    ('ssm_occurrence', 'properties.ssm'),
    ('ssm_occurrence', 'properties.ssm.properties.consequence'),
    ('ssm_occurrence', 'properties.ssm.properties.consequence.properties.transcript'),
    ('ssm_occurrence', 'properties.ssm.properties.consequence.properties.transcript.properties.gene'),
    ('ssm_occurrence', 'properties.ssm.properties.consequence.properties.transcript.properties.annotation'),
    ('ssm_occurrence', 'properties.case'),
    ('ssm_occurrence', 'properties.case.properties.observation'),
    ('case', 'dynamic'),
    ('case', 'properties.gene'),
    ('case', 'properties.gene.properties.ssm'),
    ('case', 'properties.gene.properties.ssm.properties.consequence'),
    ('case', 'properties.gene.properties.ssm.properties.observation'),
    ('case', 'properties.gene.properties.ssm.properties.consequence.properties.transcript'),
    ('case', 'properties.gene.properties.ssm.properties.consequence.properties.transcript.properties.annotation'),
])
def test_mapping_contains(mappings, doc_type, path):
    results = parse(path).find(mappings[doc_type])
    assert len([r.value for r in results]) == 1


@pytest.mark.parametrize('doc_type,path', [
    ('case', 'nested'),
])
def test_mapping_not_in(mappings, doc_type, path):
    results = parse(path).find(mappings[doc_type])
    assert len([r.value for r in results]) == 0

@pytest.mark.parametrize('doc_type,path,value', [
    ('gene', 'properties.case.type', 'nested'),
    ('gene', 'properties.case.properties.diagnoses.type', 'nested'),
    ('gene', 'properties.case.properties.ssm.type', 'nested'),
    ('gene', 'properties.case.properties.ssm.properties.consequence.type', 'nested'),
    ('gene', 'properties.case.properties.ssm.properties.observation.type', 'nested'),
    ('gene', 'properties.transcripts.type', 'nested'),
    ('ssm', 'properties.consequence.type', 'nested'),
    ('ssm', 'properties.occurrence.type', 'nested'),
    ('ssm', 'properties.occurrence.properties.case.properties.observation.type', 'nested'),
    ('ssm_occurrence', 'properties.case.properties.observation.type', 'nested'),
    ('ssm_occurrence', 'properties.ssm.properties.consequence.type', 'nested'),
    ('case', 'properties.gene.type', 'nested'),
    ('case', 'properties.gene.properties.ssm.type', 'nested'),
    ('case', 'properties.gene.properties.ssm.properties.consequence.type', 'nested'),
    ('case', 'properties.gene.properties.ssm.properties.observation.type', 'nested'),
])
def test_mapping_path_equals(mappings, doc_type, path, value):
    results = parse(path).find(mappings[doc_type])
    assert len([r.value for r in results]) == 1
    assert results[0].value == value
