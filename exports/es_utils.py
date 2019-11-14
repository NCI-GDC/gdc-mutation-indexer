import re

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from normalizer.mapper import ModelMapper


def iterate_es_results(es_client, index_name, doc_type, query=None):
    """
    Returns iterator over elasticsearch query results
    """
    if query is None:
        query = {}

    doc_iterator = scan(es_client,
                        index=index_name,
                        doc_type=doc_type,
                        scroll='2m',
                        size=100,
                        query=query)
    return doc_iterator


def get_values_from_path(es_doc, path):
    """
    Retrieves the value(s) from the document's dot-delimited path

    NOTE: Since there could be array fields in :path, there can be multiple values
    at the :path in :es_doc
    """

    if isinstance(path, str):
        path = path.split('.')

    values = []
    for i, step in enumerate(path):
        if isinstance(es_doc, list):
            for subdoc in es_doc:
                values.extend(get_values_from_path(subdoc, path[i+1:]))
        elif isinstance(es_doc, dict):
            subdoc = es_doc[step]
            values.extend(get_values_from_path(subdoc, path[i+1:]))
        else:
            values.append(es_doc)

    return values


def get_es_doc_count(es_client, index_name, doc_type, query=None):
    if query is None:
        query = {}
    es_client.indices.refresh(index=index_name)
    return es_client.count(
        index=index_name,
        doc_type=doc_type,
        body=query
    )['count']


def get_nested_field_by_value_query(
        field, nested_path, value, not_equals=False):
    """
    Builds query for :field which is underneath nested :nested_path elasticsearch path
    and which has value == :value (value != :value if :not_equals is True)
    """
    if not_equals:
        clause = 'must_not'
    else:
        clause = 'must'

    query = {
        'query': {
            'bool': {
                'should': [{
                    'bool': {
                        clause: {
                            'nested': {
                                'path': nested_path,
                                'query': {
                                    'terms': {
                                        field: value
                                    }
                                }
                            }
                        }
                    }
                }]
            }
        }
    }

    return query


def get_non_null_fields(config, blacklist=None):
    """
    Compare the graph index and case_centric mappings to figure out the field
    differences. Given the missing fields, query graph index and check if any
    of the fields have actual values.

    :param config: MI run config
    :param blacklist: a list of fields to ignore in the differences
    :return: list of fields that have values in graph index, but missing not
        defined in case_centric mappings
    """
    if not blacklist:
        # Load default blacklist fields from the config
        blacklist = config.exclude_fields

    es_client = config.es

    # get actual graph index mappings
    gi_mappings = es_client.indices.get_mapping(config.graph_index,
                                                config.graph_document)
    # get actual graph index settings
    gi_settings = es_client.indices.get_settings(config.graph_index)
    gi_doc_mappings = gi_mappings.values()[0]['mappings']

    # Need to create the mappings in gdcmodels format
    gc_mappings = {
        config.graph_index: {
            config.graph_document: {
                '_mapping': gi_doc_mappings[config.graph_document],
            },
            '_settings': gi_settings.values()[0]['settings'],
        }
    }

    centric_mapper = ModelMapper('case_centric')
    graph_mapper = ModelMapper(config.graph_index,
                               config.graph_document)

    graph_mapper.models = gc_mappings

    # Get exclude fields, some of which can be wildcard
    concretes = []
    wildcards = []
    for f in blacklist:
        if '*' in f:
            # making it regex compatible to filter later
            wildcards.append(f.replace('*.', '*').replace('*', '.*'))
        else:
            concretes.append(f)

    graph_paths = graph_mapper.get_paths()
    centric_paths = centric_mapper.get_paths()

    # filter out explicit fields
    for c in concretes:
        graph_paths = [p for p in graph_paths if not p.startswith(c)]

    # filter out wildcard fields
    for w in wildcards:
        graph_paths = [p for p in graph_paths if not re.match(w, p)]

    missing_paths = set(graph_paths) - set(centric_paths)

    paths_with_data = []

    nested_docs_paths = {
        p.replace('root.', '').replace('properties.', '').replace('.type.nested', '')
        for p in graph_mapper.leaf_paths if p.endswith('nested')
    }

    for path in missing_paths:
        nested = None

        parts = path.split('.')
        for i in range(len(parts), 0, -1):
            sub_path = '.'.join(parts[:i])
            if sub_path in nested_docs_paths:
                nested = sub_path
                break

        if nested == path:
            # Extend config.case_exclude_fields to exclude nested types
            continue

        exists = {'exists': {'field': path}}
        if nested:
            query = {
                'nested': {
                    'path': nested,
                    'query': exists
                }
            }
        else:
            query = exists

        if get_es_doc_count(
                es_client=es_client, index_name=config.graph_index,
                doc_type=config.graph_document, query={'query': query}) > 0:
            paths_with_data.append(path)

    return paths_with_data
