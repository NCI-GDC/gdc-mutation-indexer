from exports.builders.utils import struct_select


def build_ssm_subtree(maf_df, cons_df, index_name, obs_df=None):
    """
    ssm[]
       |___ consequence[]
       |             |_____...
       |___ observation[]
    """
    ssm_df = get_ssm_df(maf_df, index_name, add_fields=['gene_id', 'case_id'])

    df = ssm_df.join(cons_df, on='ssm_id', how='left')
    if obs_df:
        df = df.join(obs_df, on=['ssm_id', 'case_id'], how='left')

    return df


def build_cnv_subtree(gistic_df, cons_df, index_name, obs_df=None,
                      add_fields=['gene_id', 'case_id']):
    """
    cnv[]
       |___ consequence[]
       |            |_____ gene{}
       |
       |___ observation[]

    """
    cnv_df = get_cnv_df(gistic_df, index_name,
                        add_fields=add_fields,
                        drop_fields=['occurrence_id'])

    df = cnv_df.join(cons_df, on='cnv_id', how='left')
    if obs_df:
        df = df.join(obs_df, on=['cnv_id', 'case_id'], how='left')

    df = df.drop('occurrence_id')

    return df


def get_annotation_df(input_df, index_name, add_fields=[], drop_fields=[],
                      unique_fields=None, ignore=[]):
    return get_single_df(input_df, index_name, 'annotation', add_fields,
                         drop_fields, unique_fields, ignore)


def get_gene_df(input_df, index_name, add_fields=[], drop_fields=[],
                unique_fields=None, ignore=['transcripts']):
    return get_single_df(input_df, index_name, 'gene', add_fields,
                         drop_fields, unique_fields, ignore)


def get_ssm_df(input_df, index_name, add_fields=[], drop_fields=[],
               unique_fields=None, ignore=[]):
    return get_single_df(input_df, index_name, 'ssm',
                         add_fields,  drop_fields, unique_fields, ignore)


def get_cnv_df(input_df, index_name, add_fields=[], drop_fields=[],
               unique_fields=None, ignore=[]):
    return get_single_df(input_df, index_name, 'cnv',
                         add_fields, drop_fields, unique_fields, ignore)


def get_transcript_df(input_df, index_name, add_fields=[], drop_fields=[],
                      unique_fields=None, ignore=[]):
    return get_single_df(input_df, index_name, 'transcript', add_fields,
                         drop_fields, unique_fields, ignore)


def get_single_df(input_df, index_name, mapping_name,
                  add_fields=[], drop_fields=[], unique_fields=None, ignore=[]):
    df = input_df.select(
        *(add_fields + struct_select(index_name, mapping_name, ignore=ignore)))

    df = df.drop_duplicates(subset=unique_fields)
    return reduce(lambda cur_df, col: cur_df.drop(col), drop_fields, df)
