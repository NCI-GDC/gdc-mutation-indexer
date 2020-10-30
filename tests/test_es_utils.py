import pytest
from normalizer.mapper import ModelMapper

from exports.es_utils import get_non_null_fields
from tests_config import TestConfig

config = TestConfig()


@pytest.fixture
def diagnoses_missing_field(source_es_client):
    graph_mapper = ModelMapper('gdc_from_graph', 'case')
    centric_mapper = ModelMapper('case_centric')

    graph_diagnoses = graph_mapper.select_mapping('diagnoses')['properties']
    centric_diagnoses = centric_mapper.select_mapping('diagnoses')['properties']

    diff = set(graph_diagnoses.keys()) - set(centric_diagnoses.keys())

    default_blacklist = {
        f.split('.')[1]
        for f in config.case_exclude_fields if f.startswith('diagnoses.')
    }

    diff = diff - default_blacklist

    target_field = None
    for field in diff:
        field_type = graph_diagnoses[field]['type']
        if field_type == 'keyword':
            target_field = field
            break

    assert target_field

    dummy_document = {
        'case_id': 'foo',
        'diagnoses': {
            target_field: 'dummy-value'
        }
    }

    index_name = config.graph_case_index
    result = source_es_client.index(index=index_name, body=dummy_document)
    source_es_client.indices.refresh(index_name)

    assert result['result'] == 'created'

    yield dummy_document

    source_es_client.delete(index=index_name, id=result['_id'])
    source_es_client.indices.refresh(index_name)


@pytest.mark.usefixtures('setup_graph_indices')
def test_missing_fields(diagnoses_missing_field):
    result = get_non_null_fields(config)

    field, _ = diagnoses_missing_field['diagnoses'].popitem()
    expected_field = 'diagnoses.{}'.format(field)

    assert result
    assert set(result) == {expected_field}
