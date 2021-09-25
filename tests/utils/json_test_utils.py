import os

import tests_config

conf = tests_config.Config()

MAPPING = {
###
#  This mapping is used to specify the identifier for an nested json item in a list.
#  - 'key' of dictionary entry is the name of the list.
#  - 'name' is the name (json name) of every item. It is empty if item does not have name.
#  - 'id' is a list of fields help to identify an item in the list.
#  - For every index, you have to specify all the lists of nested json objects here.
###
        "consequence": {'name': 'transcript', 'id': ['transcript_id']},
        "occurrence": {'name': 'case', 'id': ['submitter_id']},
        "case": {'name': '', 'id': ['case_id']},
        "diagnoses": {'name': '', 'id': ['diagnosis_id']},
        "data_categories": {'name': '', 'id': ['data_category', 'file_count']},
        "observation": {'name': '', 'id': ['tumor_sample_uuid']},
        "gene": {'name': '', 'id': ['gene_id']},
        "ssm": {'name': '', 'id': ['ssm_id']},
        "transcripts": {'name': '', 'id': ['transcript_id']},
        "exons": {'name': '', 'id': ['start', 'end']},
        "domains": {'name': '', 'id': ['start', 'end']}
    }

LEVEL_SEPARATOR = "::"
KEY_VALUE_SEPARATOR = "="


class DiffObject(object):
    def __init__(self, address, diff_values, message):
        self.address = address
        self.diff_values = diff_values
        self.message = message

    def __str__(self):
        output_dict = {'address': self.address,
                       'diff': self.diff_values,
                       'message': self.message
                       }
        return str(output_dict)

    def __repr__(self):
        return self.__str__()


class DiffsReporter(object):

    def __init__(self):
        pass

    @staticmethod
    def report_diffs(diffs, filename):
        stat_info = diffs[-1]
        diffs = diffs[:-1]

        filepath = os.path.join(conf.log_dir, filename + '.log')
        try:
            os.remove(filepath)
        except:
            pass

        with open(filepath, 'w') as f:
            f.write('{}/{} <- paths/total diffs\n{}'.format(stat_info['count'],
                                                            len(diffs),
                                                            stat_info['detail']))
            for diff in diffs:
                f.write('\n-> {}\n\t{}'.format(diff.address, diff.message))

    @staticmethod
    def report_summary():
        for filename in os.listdir(conf.log_dir):
            with open(os.path.join(conf.log_dir, filename), 'r') as f:
                n_paths, n_diffs = map(int, f.readlines()[0].split()[0].split('/'))
            print '{}: wrong paths {}, n_diffs {}'.format(filename, n_paths, n_diffs)


def __build_dict_from_list_json(list_json, identity_fields, object_name=""):
    res_dict = {}
    for js in list_json:
        key = __identity_from_identity_fields(js, identity_fields, object_name)
        js_content = js
        if object_name != "":
            js_content = js[object_name]
        res_dict[key] = js_content
    return res_dict


def __identity_from_identity_fields(json, identity_fields, object_name=""):
    if object_name is not "":
        json = json[object_name]
    identity = ''
    for key in identity_fields:
        identity += str(json[key]) + '-'
    identity = identity[:-1]
    return identity


def __validate_two_flat_lists(address, f_list, other_list):
    res = []
    f_len = len(f_list)
    other_len = len(other_list)
    res.extend(assert_equal(address, f_len, other_len,
                            "first list has {0} items while second list has {1} items"
                            .format(f_len, other_len)))
    s = set(f_list)
    for item in other_list:
        res.extend(assert_in(address, item, other_list,
                             "{0} does not exists in {1}".format(item, s)))
    return res


def __validate_list_keys(address, dict_jsons, other_dict_jsons, identity_fields, object_name=None):
    res = []
    first_size = len(dict_jsons.keys())
    second_size = len(other_dict_jsons.keys())

    res.extend(assert_equal(address, first_size, second_size,
                            "First list has {0} items while second list has {1} items"
                            .format(first_size, second_size)))

    for key in dict_jsons.keys():
        new_address = address + "{}{}".format(KEY_VALUE_SEPARATOR, key)
        diff = assert_in(new_address, key, other_dict_jsons.keys(),
                         "{0} is not in {1}".format(key, other_dict_jsons.keys()))
        res.extend(diff)
    return res


def __gathering_statistic_info(diffs):
    paths_having_problem = set([])
    for diff in diffs:
        path = ""
        path_items = diff.address.split(LEVEL_SEPARATOR)
        for path_item in path_items:
            parts = path_item.split(KEY_VALUE_SEPARATOR)
            path += '{0}{1}'.format(parts[0], LEVEL_SEPARATOR)
        paths_having_problem.add(path[:-1])
    return {"count": len(paths_having_problem),
            "detail": list(paths_having_problem)}


def assert_equal(address, value, other, message):
    if value != other:
        return [DiffObject(address, [value, other], message)]
    return []


def assert_in(address, value, other, message):
    if value not in other:
        return [DiffObject(address, [value, other], message)]
    return []


def __validate_two_list_jsons(address, list_jsons, other_list_jsons,
                              identity_fields, object_name, join_only=False):
    '''
    Call this function when you want to validate two list of nested object
    :param address: json address of the parent json node of the list
    :param list_jsons: list of json objects in testing json
    :param other_list_jsons: list of json object in golden set
    :param identity_fields: list of fields help to identify every single node in list of json
    :param object_name: field to get the content of json object if there is a named json object in the list
    :param join_only: set it to True if you only want to test the cardinality (resulted by join).
            Set it to True if you want to test the full content of the json file
    :return: list of differences
    '''
    address += "{}{}".format(LEVEL_SEPARATOR, object_name)
    dict_jsons = __build_dict_from_list_json(list_jsons,
                                             identity_fields, object_name)
    other_dict_jsons = __build_dict_from_list_json(other_list_jsons,
                                                   identity_fields, object_name)

    res = __validate_list_keys(address, dict_jsons,
                               other_dict_jsons, identity_fields, object_name)

    for key in dict_jsons.keys():
        if key not in other_dict_jsons.keys():
            continue
        if join_only:
            res.extend(validate_two_nested_jsons_joining(address,
                                                         dict_jsons[key],
                                                         other_dict_jsons[key]))
        else:
            res.extend(validate_two_nested_jsons(address,
                                                 dict_jsons[key],
                                                 other_dict_jsons[key]))

    return res


def validate_two_nested_jsons_joining(address, json_obj,
                                      other_json_obj, ignored_list=None):
    '''
    Call this function when you only want to validate the correctness of joins
    :param address: json address of the parent json node of the list
    :param json_obj: json objects in testing json
    :param other_json_obj: json object in golden set
    :param ignored_list: list of fields that are ignored in the validation
    :return:
    '''
    res = []
    if ignored_list is None:
        ignored_list = []
    for field in json_obj.keys():
        new_address = address + "{}{}".format(LEVEL_SEPARATOR, field)
        if field not in other_json_obj.keys():
            continue
        elif field not in ignored_list:
            if type(json_obj[field]) is list:
                if field in MAPPING.keys():
                    mapping_field = MAPPING[field]
                    res.extend(__validate_two_list_jsons(new_address,
                                                         json_obj[field],
                                                         other_json_obj[field],
                                                         mapping_field['id'],
                                                         mapping_field['name'],
                                                         join_only=True))
    return res


def validate_two_nested_jsons(address, json_obj, other_json_obj, ignored_list=None):
    '''
    Call this function when you want to validate content of two json objects in dept
    :param address: json address of the parent json node of the list
    :param json_obj: json objects in testing json
    :param other_json_obj: json object in golden set
    :param ignored_list: list of fields that are ignored in the validation
    :return: list of differences
    '''
    res = []
    if ignored_list is None:
        ignored_list = []
    for field in json_obj.keys():
        new_address = address + "{}{}".format(LEVEL_SEPARATOR, field)
        diff = assert_in(new_address, field, other_json_obj.keys(),
                         "{0} is not in {1}".format(field, other_json_obj.keys()))
        if diff:
            res.extend(diff)
            continue
        elif field not in ignored_list:
            if type(json_obj[field]) is list:
                if field in MAPPING.keys():
                    mapping_field = MAPPING[field]
                    res.extend(__validate_two_list_jsons(new_address,
                                                         json_obj[field],
                                                         other_json_obj[field],
                                                         mapping_field['id'],
                                                         mapping_field['name'],
                                                         join_only=False))
                else:
                    res.extend(__validate_two_flat_lists(new_address,
                                                         json_obj[field],
                                                         other_json_obj[field]))
            elif type(json_obj[field]) is dict:
                res.extend(validate_two_nested_jsons(new_address,
                                                     json_obj[field],
                                                     other_json_obj[field]))
            else:
                res.extend(assert_equal(new_address,
                                        json_obj[field],
                                        other_json_obj[field],
                                        "field {0}: {1} is not equal {2}"
                                        .format(field, json_obj[field],
                                                other_json_obj[field])))
    return res
