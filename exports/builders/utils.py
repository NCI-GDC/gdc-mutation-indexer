import functools
import logging
import re
import uuid
from collections.abc import Set
from typing import Any

import pkg_resources
import yaml
from normalizer import mapper
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

logger = logging.getLogger(__name__)


DEFAULT_EXCLUDE_FIELDS: dict[str, dict[str, Set[str]]] = {}


def get_default_excludes(index, mapping):
    if DEFAULT_EXCLUDE_FIELDS:
        return set(DEFAULT_EXCLUDE_FIELDS.get(mapping, {}).get(index, []))

    path = pkg_resources.resource_filename("exports", "schemas/exclude.defaults.yaml")

    with open(path) as f:
        excludes = yaml.safe_load(f)

    for k, v in excludes.items():
        DEFAULT_EXCLUDE_FIELDS[k] = v

    return set(DEFAULT_EXCLUDE_FIELDS.get(mapping, {}).get(index, []))


def ssm_label(chromosome, variant_type, start_pos, end_pos, ref_allele, tumor_allele):
    """
    Create a label (genomic change) from an ssm based on its variant type:

    :param chromosome: The chromosome where the mutation occurred
    :param variant_type: The variant type (e.g., ``SNP``, ``DNP``, ``DEL``, ``INS``...)
    :param start_pos: The starting position of the mutation
    :param end_pos: The end position of the mutation
    :param ref_allele: The reference allele
    :param tumor_allele: The tumor allele
    """
    chromosome = chromosome.replace("chr", "")

    if variant_type == "SNP":
        label = "chr{}:g.{}{}>{}".format(
            chromosome, start_pos, ref_allele, tumor_allele
        )
    elif variant_type in {"DNP", "TNP", "ONP"}:
        label = "chr{}:g.{}_{}delins{}".format(
            chromosome, start_pos, end_pos, tumor_allele
        )
    elif variant_type == "DEL":
        label = "chr{}:g.{}del{}".format(chromosome, start_pos, ref_allele)
    elif variant_type == "INS":
        label = "chr{}:g.{}_{}ins{}".format(
            chromosome, start_pos, end_pos, tumor_allele
        )
    else:
        label = chromosome

    return label


def ssm_label_col(
    chromosome, variant_type, start_pos, end_pos, ref_allele, tumor_allele
):
    return F.udf(ssm_label, types.StringType())(
        chromosome, variant_type, start_pos, end_pos, ref_allele, tumor_allele
    )


def generate_uuid5(*values: Any) -> str:
    """
    From Junjun's indexer:
    https://github.com/NCI-GDC/es-indexer/blob/d30cf9ef9a445c5bea441b9333ca4b8c2c2c33cb/es_indexer/dataframe/processor/uuid5_field.py#L6

    Let's use uuid5 hash for distributed 'unique' ID generation.
    This is sure not safe to. We should later switch to get ID
    from some kind of central ID Service. One other benefit to
    use ID service is that we can get short IDs
    This is a UDF.
    """
    # first value is entity type, the rest are fields made up
    # to a business key uniquely identifying an entity
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_DNS,
            "\t".join([v if type(v) == str else str(v) for v in values]),
        )
    )


def uuid5_col(*values):
    return F.udf(generate_uuid5, types.StringType())(*values)


def extract_impact(df, column, res_colname):
    """
    Extracts impact from fields like:
    'possibly_damaging(0.614)'

    impact = 'possibly_damaging'
    """

    return df.withColumn(res_colname, F.regexp_extract(column, "(.*)\(.*\)$", 1))


def extract_score(df, column, res_colname):
    """
    Extracts score from fields like:
    'possibly_damaging(0.614)'

    score = '0.614'
    """

    return df.withColumn(
        res_colname,
        F.regexp_extract(column, "(\w)\((\d*.?(\d?)*)\)$", 2).cast(types.DoubleType()),
    )


def extract_sift_polyphen(df):
    """
    Extracts '{polyphen|sift}_{impact|score}' from 'polyphen' and 'sift' columns
    """
    for c in ["polyphen", "sift"]:
        df = extract_impact(df, c, "{}_impact".format(c.lower()))
        df = extract_score(df, c, "{}_score".format(c.lower()))
    df = df.drop("polyphen").drop("sift")
    return df


def select_mapping(index_name, mapping_name, selector=None, exclude_fields=None):
    if exclude_fields is None:
        exclude_fields = get_default_excludes(index_name, mapping_name)

    model_mapper = mapper.ModelMapper(index_name)
    mapping = model_mapper.select_mapping(mapping_name, selector)

    mapping["properties"] = {
        k: v for k, v in mapping["properties"].items() if k not in exclude_fields
    }

    return mapping


def struct_select(index_name, mapping_name, ignore=(), selector=None):
    """
    Takes the structure from a mapping and produces arguments for a select
    to reorganize a flat dataframe of those fields into the desired structure.
    Eg:
    Given the mapping:
    ```
    properties:
      center:
        type: keyword
      input_bam_file:
        properties:
          normal_bam_uuid:
            type: keyword
    ```
    Produce the select arguments:
    `struct('center', struct('normal_bam_uuid').alias('input_bam_file'))`
    """

    def restructure(doc):
        if not isinstance(doc, dict):
            return []

        cols = []
        for k, v in doc.items():
            # Ignore OICR autocomplete features
            if (
                k == "gene_aa_change"
                or k == "copy_to"
                or "_autocomplete" in k
                or k == "clinical_annotations"
            ):
                pass

            elif "type" in v and "properties" not in v:
                name = k
                if "default" in v:
                    name = v["default"]
                cols.append(F.col(name).alias(k))
            else:
                if k not in ignore and "properties" in v:
                    cols.append(F.struct(restructure(v["properties"])).alias(k))
                elif k not in ignore:
                    cols.append(F.struct(restructure(v)).alias(k))
                else:
                    cols.append(k)
        return cols

    mapping = select_mapping(index_name, mapping_name, selector=selector)

    return restructure(mapping["properties"])


def select_nested(index_name, mapping_name, ignore=(), selector=None):
    def flatten_nested(doc):
        if not isinstance(doc, dict):
            return []

        cols = []
        for k, v in doc.items():
            if (
                k == "gene_aa_change"
                or k == "copy_to"
                or "_autocompolete" in k
                or k == "clinical_annotations"
            ):
                continue

            if k in ignore:
                continue

            if "type" in v and "properties" not in v:
                name = k
                if "default" in v:
                    name = v["default"]
                cols.append(F.col(name).alias(k))
            else:
                if "properties" in v:
                    cols.extend(flatten_nested(v["properties"]))
                else:
                    cols.extend(flatten_nested(v))
        return cols

    mapping = select_mapping(
        index_name, mapping_name, selector=selector, exclude_fields=()
    )

    return flatten_nested(mapping["properties"])


def percentile(vector, p):
    """
    Calculates the p percentile of vector
    """
    vector = sorted(vector)
    vector_len = len(vector)
    position = (vector_len - 1) * float(p) / 100
    floored_pos = int(position)
    rest = position - floored_pos
    if floored_pos >= vector_len - 1:
        return vector[vector_len - 1]

    return vector[floored_pos] + (vector[floored_pos + 1] - vector[floored_pos]) * rest


def extract_aas_position(df):
    """
    create aa_start and aa_end field based on aa_change string
    there is one problem that synonymous_variant aa_change does not contain
    aa position information
    """

    def extract(aa_change, start=True):
        match = re.findall(re.compile("(\d+)(?:\D+?)*(\d+)*(?:\D+)"), aa_change)
        if match:
            aa_start, aa_end = match[0]
            if start or not aa_end:
                return int(aa_start)

            return int(aa_end)
        return "null"

    df = df.withColumn(
        "aa_start", F.udf(extract, types.IntegerType())(F.col("aa_change"))
    )
    df = df.withColumn(
        "aa_end",
        F.udf(lambda aa_change: extract(aa_change, False), types.IntegerType())(
            F.col("aa_change")
        ),
    )

    return df


def sanitize_aa_change(df):
    """
    Removes 'p.' from aa_change
    """

    def sanitize(aa_change):
        return aa_change.strip("p.")

    df = df.withColumn(
        "aa_change", F.udf(sanitize, types.StringType())(F.col("aa_change"))
    )

    return df


def sanitize_gene_aa_change(df):
    """
    Removes nulls, empty strings and duplicates from gene_aa_change;
    Sorts gene_aa_change
    """

    def sanitize(gene_aa_change):
        gene_aa_change = [x for x in gene_aa_change if x not in [None, ""]]
        return sorted(list(set(gene_aa_change)))

    df = df.withColumn(
        "gene_aa_change",
        F.udf(sanitize, types.ArrayType(types.StringType()))(F.col("gene_aa_change")),
    )

    return df


def convert_empty_str_to_null_in_col(df, col_name):
    """
    Converts empty string to null in df.col_name
    """

    return df.withColumn(
        col_name, F.when(F.col(col_name) != "", F.col(col_name)).otherwise(None)
    )


def get_column_name(column_name, dataset_key):
    return "{}_{}".format(column_name, dataset_key)


def add_canonical_transcript_lengths(transcripts_df: sql.DataFrame) -> sql.DataFrame:
    """
    Calculates and adds canonical_transcript_length{'','_cds','_genomic'} fields to a
    dataframe containing an array of transcripts.

    Args:
        transcripts_df: a dataframe containing a column "transcripts" which is an
            array of transcript objects.

    Return:
        A dataframe with the added columns: canonical_transcript_length,
        canonical_transcript_length_cds, and canonical_transcript_length_genomic
    """
    canonical_index = F.array_position("transcripts.is_canonical", True)
    transcripts_df = transcripts_df.withColumn(
        "canonical_transcript",
        F.when(
            canonical_index > 0, F.col("transcripts")[canonical_index - 1]
        ).otherwise(None),
    )
    transcripts_df = transcripts_df.withColumn(
        "canonical_transcript_length", F.col("canonical_transcript.length")
    )
    transcripts_df = transcripts_df.withColumn(
        "canonical_transcript_length_cds", F.col("canonical_transcript.length_cds")
    )
    transcripts_df = transcripts_df.withColumn(
        "canonical_transcript_length_genomic",
        F.col("canonical_transcript.end") - F.col("canonical_transcript.start") + 1,
    )

    return transcripts_df.drop("canonical_transcript")
