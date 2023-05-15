import itertools
import logging
from collections.abc import Container, Iterable
from typing import Optional

from pyspark import sql

from exports.builders import clinical_annotations, utils

logger = logging.getLogger("df_builder")


def build_ssm_subtree(
    maf_df: sql.DataFrame,
    cons_df: sql.DataFrame,
    index_name: str,
    obs_df: Optional[sql.DataFrame] = None,
) -> sql.DataFrame:
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
    ascat_df: sql.DataFrame,
    index_name: str,
    cons_df: Optional[sql.DataFrame] = None,
    obs_df: Optional[sql.DataFrame] = None,
    add_fields: Iterable[str] = ("gene_id", "case_id"),
) -> sql.DataFrame:
    """
    cnv[]
       |___ consequence[]
       |            |_____ gene{}
       |
       |___ observation[]

    """
    cnv_df = get_cnv_df(
        ascat_df,
        index_name,
        add_fields=add_fields,
        drop_fields=frozenset(("occurrence_id",)),
    )

    df = cnv_df.join(cons_df, on="cnv_id", how="left") if cons_df else cnv_df
    if obs_df:
        df = df.join(obs_df, on=["cnv_id", "case_id"], how="left")

    df = df.drop("occurrence_id")

    return df


def get_annotation_df(
    input_df: sql.DataFrame,
    index_name: str,
    add_fields: Iterable[str] = (),
    drop_fields: Container[str] = (),
    unique_fields: Optional[list[str]] = None,
    ignore: Container[str] = (),
) -> sql.DataFrame:
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
    input_df: sql.DataFrame,
    index_name: str,
    add_fields: Iterable[str] = (),
    drop_fields: Container[str] = (),
    unique_fields: Optional[list[str]] = None,
    ignore=frozenset(("transcripts",)),
):
    return get_single_df(
        input_df, index_name, "gene", add_fields, drop_fields, unique_fields, ignore
    )


def get_ssm_df(
    maf_df: sql.DataFrame,
    index_name: str,
    add_fields: Iterable[str] = (),
    drop_fields: Container[str] = (),
    unique_fields: Optional[list[str]] = None,
    ignore: Container[str] = (),
) -> sql.DataFrame:
    clinical_anno_df = clinical_annotations.get_clinical_annotation_df(
        index_name, maf_df
    )
    df = get_single_df(maf_df, index_name, "ssm", add_fields, (), unique_fields, ignore)
    df = df.join(clinical_anno_df, on="ssm_id", how="left")
    columns = (column for column in df.columns if column not in drop_fields)

    return df.select(*columns)


def get_cnv_df(
    ascat_df: sql.DataFrame,
    index_name: str,
    add_fields: Iterable[str] = (),
    drop_fields: Container[str] = (),
    unique_fields: Optional[list[str]] = None,
    ignore: Container[str] = (),
) -> sql.DataFrame:
    return get_single_df(
        ascat_df, index_name, "cnv", add_fields, drop_fields, unique_fields, ignore
    )


def get_transcript_df(
    input_df: sql.DataFrame,
    index_name: str,
    add_fields: Iterable[str] = (),
    drop_fields: Container[str] = (),
    unique_fields: Optional[list[str]] = None,
    ignore: Container[str] = (),
) -> sql.DataFrame:
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
    input_df: sql.DataFrame,
    index_name: str,
    mapping_name: str,
    add_fields: Iterable[str] = (),
    drop_fields: Container[str] = (),
    unique_fields: Optional[list[str]] = None,
    ignore: Container[str] = (),
) -> sql.DataFrame:
    columns = itertools.chain(
        add_fields, utils.struct_select(index_name, mapping_name, ignore=ignore)
    )
    df = input_df.select(*columns)
    df = df.drop_duplicates(subset=unique_fields)
    columns = filter(lambda c: c not in drop_fields, df.columns)

    return df.select(*columns)
