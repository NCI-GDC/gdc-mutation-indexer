import pytest
from normalizer.mapper import ModelMapper

from exports.es_utils import get_non_null_fields
from tests_config import TestConfig

config = TestConfig()


@pytest.fixture
def diagnoses_missing_field(setup_test_index):
    es = setup_test_index
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

    result = es.index(index=config.graph_index, doc_type=config.graph_document,
                      body=dummy_document)
    es.indices.refresh(index=config.graph_index)

    assert result['created'] is True

    yield dummy_document

    es.delete(index=config.graph_index, doc_type=config.graph_document,
              id=result['_id'])
    es.indices.refresh(config.graph_index)


def test_missing_fields(diagnoses_missing_field, setup_test_index):
    result = get_non_null_fields(config)

    field, _ = diagnoses_missing_field['diagnoses'].popitem()
    expected_field = 'diagnoses.{}'.format(field)

    assert result
    assert set(result) == {expected_field}
