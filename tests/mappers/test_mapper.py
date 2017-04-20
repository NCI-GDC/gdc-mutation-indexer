import pytest
from jsonpath_rw import parse

from exports.mappers import ModelMapper


@pytest.fixture(scope="session")
def mappings():
    case_mapper = ModelMapper('case_centric')
    gene_mapper = ModelMapper('gene_centric')
    ssm_mapper = ModelMapper('ssm_centric')
    ssm_occurrence_mapper = ModelMapper('ssm_occurrence_centric')
    return {
        'gene': gene_mapper.type_mappings['gene_centric'],
        'ssm': ssm_mapper.type_mappings['ssm_centric'],
        'ssm_occurrence': ssm_occurrence_mapper.type_mappings['ssm_occurrence_centric'],
        'case': case_mapper.type_mappings['case_centric']
    }


@pytest.fixture(scope="session")
def mappings_with_settings():
    case = ModelMapper('case_centric').create_index_settings()
    gene = ModelMapper('gene_centric').create_index_settings()
    ssm = ModelMapper('ssm_centric').create_index_settings()
    ssm_occ = ModelMapper('ssm_occurrence_centric').create_index_settings()
    return {
        'gene': gene, 'ssm': ssm, 'case': case, 'ssm_occurrence': ssm_occ,
        }


@pytest.mark.parametrize('index_name', ['case', 'gene', 'ssm', 'ssm_occurrence'])
def test_mapping_settings(mappings_with_settings, index_name):
    mappings = mappings_with_settings[index_name]
    for key in ['mappings', 'settings']:
        assert key in mappings

    for doctype in mappings['mappings']:
        for key in ['_all', '_source', 'dynamic']:
            assert key in mappings['mappings'][doctype]

    assert mappings['settings'] is not None
    assert 'analysis' in mappings['settings']


@pytest.mark.parametrize('doc_type,path', [
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
    ('ssm', 'properties.consequence'),
    ('ssm', 'properties.consequence.properties.transcript'),
    ('ssm', 'properties.consequence.properties.transcript.properties.gene'),
    ('ssm', 'properties.consequence.properties.transcript.properties.annotation'),
    ('ssm', 'properties.occurrence'),
    ('ssm', 'properties.occurrence.properties.case'),
    ('ssm', 'properties.occurrence.properties.case.properties.observation'),
    ('ssm_occurrence', 'properties.ssm'),
    ('ssm_occurrence', 'properties.ssm.properties.consequence'),
    ('ssm_occurrence', 'properties.ssm.properties.consequence.properties.transcript'),
    ('ssm_occurrence', 'properties.ssm.properties.consequence.properties.transcript.properties.gene'),
    ('ssm_occurrence', 'properties.ssm.properties.consequence.properties.transcript.properties.annotation'),
    ('ssm_occurrence', 'properties.case'),
    ('ssm_occurrence', 'properties.case.properties.observation'),
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

