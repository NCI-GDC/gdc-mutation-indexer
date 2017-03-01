from json_test_utils import validate_two_list_jsons, validate_two_nested_jsons, validate_two_flat_jsons


def validate_consequence_list_in_dept(address, list_transcripts, other_list_transcripts):
    return validate_two_list_jsons(address, list_transcripts, other_list_transcripts,
                                   ["transcript_id"],
                                   object_name="transcript",
                                   diff_func=validate_two_consequence)


def validate_consequence_join(address, list_transcripts, other_list_transcripts):
    return validate_two_list_jsons(address, list_transcripts, other_list_transcripts,
                                   ["transcript_id"],
                                   object_name="transcript",
                                   diff_func=validate_two_consequence,
                                   join_only=True)


def validate_two_consequence(address, json_obj, other_json_obj):
    return validate_two_nested_jsons(address, json_obj, other_json_obj)
