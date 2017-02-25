from exports.builders.utils import struct_select


def get_annotation_df(input_df, add_fields=[], drop_fields=[],
                      unique_fields=None, ignore=[]):
    return get_single_df(
        input_df, 'annotation.yml', add_fields,
        drop_fields, unique_fields, ignore)


def get_gene_df(input_df, add_fields=[], drop_fields=[],
                unique_fields=None, ignore=['transcripts']):
    return get_single_df(
        input_df, 'gene.yml', add_fields,
        drop_fields, unique_fields, ignore)


def get_ssm_df(input_df, add_fields=[], drop_fields=[],
               unique_fields=None, ignore=[]):
    return get_single_df(
        input_df, 'ssm.yml', add_fields,
        drop_fields, unique_fields, ignore)


def get_transcript_df(input_df, add_fields=[], drop_fields=[],
                      unique_fields=None, ignore=[]):
    return get_single_df(
        input_df, 'transcript.yml', add_fields,
        drop_fields, unique_fields, ignore)


def get_single_df(
        input_df, mapping, add_fields=[], drop_fields=[],
        unique_fields=None, ignore=[]):
    df = input_df.select(
        *(add_fields + struct_select(mapping, ignore=ignore)))

    df = df.drop_duplicates(unique_fields)
    return reduce(lambda cur_df, col: cur_df.drop(col), drop_fields, df)
