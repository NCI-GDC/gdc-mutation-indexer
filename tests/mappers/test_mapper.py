import pytest
import pkg_resources
import yaml
import os
from jsonpath_rw import parse

import gdcmodels
from tests_config import TestConfig
from exports.mappers import ModelsMapper

conf = TestConfig()


@pytest.fixture(scope="session")
def mappers():
    return {index_name: ModelsMapper(index_name)
            for index_name in conf.index_names}


@pytest.fixture(scope="session")
def mappings(mappers):
    return {index_name: mapper.type_mappings[index_name]
            for index_name, mapper in mappers.items()}


@pytest.fixture(scope="session")
def mappings_with_settings(mappers):
    return {index_name: mapper.create_index_settings()
            for index_name, mapper in mappers.items()}


@pytest.mark.parametrize('index_name', conf.index_names)
def test_mapping_settings(mappers, mappings_with_settings, index_name):
    mappings = mappings_with_settings[index_name]

    for key in ['mappings', 'settings']:
        assert key in mappings

    for doctype in mappings['mappings']:
        for key in ['_all', '_source', 'dynamic']:
            assert key in mappings['mappings'][doctype]

    assert mappings['settings'] is not None

    assert 'analysis' in mappings['settings']

    # Load common settings file:
    cs_file = pkg_resources.resource_string('exports',
                                            os.path.join('schemas',
                                                         'common_settings.yml'))
    common_settings = yaml.safe_load(cs_file)

    mapping_settings = gdcmodels.get_es_models()[index_name]['_settings']
    # Check that mapping_settings overwrite common_settings
    for key, value in mappings['settings'].items():
        if key in mapping_settings:
            assert value == mapping_settings[key]
        else:
            assert value == common_settings['settings'][key]


def test_mapping_structure(mappings, mappings_with_settings):

    for index, mapping in mappings.items():
        for key in ['_all', '_settings', '_source', 'dynamic', index]:
            assert key in mapping

        for key in ['_all', 'properties']:
            assert key in mapping[index]

    for index, mapping in mappings_with_settings.items():
        for key in ['mappings', 'settings']:
            assert key in mapping

        assert index in mapping['mappings']

        for key in ['_all', '_settings', '_source', 'dynamic', index]:
            assert key in mapping['mappings'][index] 

        assert 'properties' in mapping['mappings'][index][index]


def test_gdc_from_graph_mapping():
    mapper = ModelsMapper('gdc_from_graph')

    for doc_type in ['case', 'file', 'project', 'annotation']:
        assert doc_type in mapper.type_mappings['gdc_from_graph']


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
    doc_type = '{}_centric'.format(doc_type)
    results = parse(path).find(mappings[doc_type][doc_type])
    assert len([r.value for r in results]) == 1


@pytest.mark.parametrize('doc_type,path', [
    ('case', 'nested'),
])
def test_mapping_not_in(mappings, doc_type, path):
    doc_type = '{}_centric'.format(doc_type)
    results = parse(path).find(mappings[doc_type][doc_type])
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
    doc_type = '{}_centric'.format(doc_type)
    results = parse(path).find(mappings[doc_type][doc_type])
    assert len([r.value for r in results]) == 1
    assert results[0].value == value


def test_get_dict_paths():
    test_dict = {
        'a': {
              'b': 'c',
              'j': 'k'
        },
        'd': {
              'f': {'g': 'h'},
              'l': 'm',
              'n': ['o', 'p', 'q'],
        }
    }

    expected_output = ['root.a.b.c', 'root.a.j.k', 'root.d.f.g.h', 'root.d.l.m',
                       'root.d.n.o', 'root.d.n.p', 'root.d.n.q']
    paths, path = ModelsMapper.get_dict_paths(test_dict)

    assert len(paths) == len(set(paths))
    assert set(paths) == set(expected_output)


def test_get_paths():
    test_mapping = {
        'path': {
            'to': {
                'my_skip_field': {'type': 'keyword'},
                'my_exclude_field': {'type': 'keyword'},
                'my_good_field': {'type': 'keyword'},
                'my_copy_to_field': {'copy_to': {'type': 'keyword'}},
                'my_bad_field': {'type': 'keyword'},
                'field_autocomplete': {'type': 'keyword'},
            }
        },

        'skip': {
            'this': {'type': 'keyword'},
            'that': {'type': 'keyword'},
        },

        'other': {
            'exclude_this_branch': {'a': {'b': {'type': 'keyword'}},
                                    'c': {'type': 'keyword'}},
            'field': {
                'skipped': {'type': 'text'},
                'good': {'type': 'text'},
            }
        }
    }

    mapper = ModelsMapper('case_centric')
    mapper.type_mappings = {'case_centric': test_mapping}

    stop_words = ['exclude', 'skip']
    paths_to_skip = ['path.to.my_bad_field', 'skip.this']

    paths = mapper.get_paths(stop_words=stop_words, paths_to_skip=paths_to_skip)

    assert sorted(paths) == ['other.field.good', 'path.to.my_good_field']
 
