def get_array_paths(doc, path=[]):
    '''
    Given a json document, return the dot-path of all array type attributes.
    This is important as array values cannot be identified from the mapping
    of an index, although they must be supplied to the elasticsearch-hadoop
    adapter when saving to elasticsearch
    '''
    if type(doc) not in [list, dict]:
        return []

    paths = []
    for k, v in doc.items():
        if type(v) is list:
            paths.append('.'.join(path+[k]))
            for elem in v:
                paths.extend(get_array_paths(elem, path + [k]))
        elif type(v) is dict:
            paths.extend(get_array_paths(v, path + [k]))
            
    return paths
