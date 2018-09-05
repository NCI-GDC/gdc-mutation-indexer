from elasticsearch.helpers import scan


def iterate_es_results(es, index_name, doc_type, query=None):
    """
    Returns iterator over elasticsearch query results
    """
    if query is None:
        query = {}

    doc_iterator = scan(es,
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
        print 'step', step
        if isinstance(es_doc, list):
            for subdoc in es_doc:
                values.extend(get_values_from_path(subdoc, path[i+1:]))
        elif isinstance(es_doc, dict):
            subdoc = es_doc[step]
            print es_doc, subdoc, step, path
            values.extend(get_values_from_path(subdoc, path[i+1:]))
        else:
            values.append(es_doc)

    return values

