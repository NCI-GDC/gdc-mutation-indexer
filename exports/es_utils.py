from elasticsearch.helpers import scan


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
