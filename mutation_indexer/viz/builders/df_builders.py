import logging

from mutation_indexer.driver import utils
from mutation_indexer.viz.builders import clinical_annotations

logger = logging.getLogger("df_builder")


def build_ssm_subtree(maf_df, cons_df, index_name, obs_df=None):
    """
    ssm[]
       |___ consequence[]
       |             |_____...
       |___ observation[]
    """
    ssm_df = get_ssm_df(maf_df, index_name, add_fields=["gene_id", "case_id"])

    df = ssm_df.join(cons_df, on="ssm_id", how="left")
    if obs_df:
        df = df.join(obs_df, on=["ssm_id", "case_id"], how="left")

    return df


def build_cnv_subtree(
    ascat_df, index_name, cons_df=None, obs_df=None, add_fields=["gene_id", "case_id"]
):
    """
    cnv[]
       |___ consequence[]
       |            |_____ gene{}
       |
       |___ observation[]

    """
    cnv_df = get_cnv_df(
        ascat_df, index_name, add_fields=add_fields, drop_fields=["occurrence_id"]
    )

    df = cnv_df.join(cons_df, on="cnv_id", how="left") if cons_df else cnv_df
    if obs_df:
        df = df.join(obs_df, on=["cnv_id", "case_id"], how="left")

    df = df.drop("occurrence_id")

    return df


def get_annotation_df(
    input_df, index_name, add_fields=[], drop_fields=[], unique_fields=None, ignore=[]
):
    return get_single_df(
        input_df,
        index_name,
        "annotation",
        add_fields,
        drop_fields,
        unique_fields,
        ignore,
    )


def get_gene_df(
    input_df,
    index_name,
    add_fields=[],
    drop_fields=[],
    unique_fields=None,
    ignore=["transcripts"],
):
    return get_single_df(
        input_df, index_name, "gene", add_fields, drop_fields, unique_fields, ignore
    )


def get_ssm_df(
    input_df, index_name, add_fields=[], drop_fields=[], unique_fields=None, ignore=[]
):
    clinical_anno_df = clinical_annotations.get_clinical_annotation_df(
        index_name, input_df
    )
    df = get_single_df(
        input_df, index_name, "ssm", add_fields, [], unique_fields, ignore
    )
    df = df.join(clinical_anno_df, on="ssm_id", how="left")
    return df.select([column for column in df.columns if column not in drop_fields])


def get_cnv_df(
    input_df, index_name, add_fields=[], drop_fields=[], unique_fields=None, ignore=[]
):
    return get_single_df(
        input_df, index_name, "cnv", add_fields, drop_fields, unique_fields, ignore
    )


def get_transcript_df(
    input_df, index_name, add_fields=[], drop_fields=[], unique_fields=None, ignore=[]
):
    return get_single_df(
        input_df,
        index_name,
        "transcript",
        add_fields,
        drop_fields,
        unique_fields,
        ignore,
    )


def get_single_df(
    input_df,
    index_name,
    mapping_name,
    add_fields=[],
    drop_fields=[],
    unique_fields=None,
    ignore=[],
):
    df = input_df.select(
        *(add_fields + utils.struct_select(index_name, mapping_name, ignore=ignore))
    )

    df = df.drop_duplicates(subset=unique_fields)
    return df.select([column for column in df.columns if column not in drop_fields])
