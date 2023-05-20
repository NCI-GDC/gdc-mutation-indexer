import functools
import logging
import re
import uuid
from typing import Any

import more_itertools
import pkg_resources
import yaml
from normalizer import mapper
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

logger = logging.getLogger(__name__)


DEFAULT_EXCLUDE_FIELDS = {}  # type: dict[str, Any]


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


def ssm_occurrence_uuid(namespace, ssm, case):
    return str(uuid.uuid5(uuid.UUID(str(namespace)), str(ssm) + str(case)))


def ssm_occurrence_uuid_udf(namespace):
    """
    Wraps the ssm_uuid function in a spark udf and injects a given namespace
    """
    ssm_namespaced = functools.partial(ssm_occurrence_uuid, str(namespace))
    return F.udf(ssm_namespaced, types.StringType())


def extract_impact(df, column, res_colname):
    """
    Extracts impact from fields like:
    'possibly_damaging(0.614)'

    impact = 'possibly_damaging'
    """

    return df.withColumn(res_colname, F.regexp_extract(column, r"(.*)\(.*\)$", 1))


def extract_score(df, column, res_colname):
    """
    Extracts score from fields like:
    'possibly_damaging(0.614)'

    score = '0.614'
    """

    return df.withColumn(
        res_colname,
        F.regexp_extract(column, r"(\w)\((\d*.?(\d?)*)\)$", 2).cast(types.DoubleType()),
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


def extract_all_effects(val, index=0):
    """
    Extracts an element from all_effects at the given index

    Rows are delimited by ;
    Columns are delimited by , or :
    """
    delimiter = "," if "," in val else ":"
    if len(val.split(delimiter)) > index:
        return val.split(delimiter)[index]


def all_effects_udf(index):
    f = functools.partial(extract_all_effects, index=index)
    return F.udf(f, types.StringType())


def extract_rows_udf():
    vals = F.udf(lambda x: x.split(";"), types.ArrayType(types.StringType()))
    return vals


def access_json_path(json_dict, step_list):
    """
    Access json path by list of steps
    """
    stack = list(step_list)

    if not stack:
        return json_dict

    step = stack.pop(0)
    return access_json_path(json_dict[step], stack)


def map_create_column(df, map_function, target_column_name, new_column_name):
    """
    Creates new column in pyspark DataFrame by mapping :map_function to :target_column
    """
    # TODO: allow controlling the return type
    udf = F.udf(map_function, types.StringType())
    df = df.withColumn(new_column_name, udf(getattr(df, target_column_name)))
    return df


def melt_df(df, id_vars, value_vars=None, var_name="variable", value_name="value"):
    """
    Source:
    https://stackoverflow.com/questions/41670103/how-to-melt-spark-dataframe

    See also:
    http://pandas.pydata.org/pandas-docs/stable/generated/pandas.melt.html

    The opposite of pivoting a dataframe

    :param df: input pyspark.DataFrame
    :param id_vars: Column(s) to use as identifier variables
    :type id_vars: Iterable
    :param value_vars: Column(s) to unpivot. If not specified, uses all columns
    that are not set as id_vars.
    :type value_vars: Iterable
    :param var_name: Name to use for the 'variable' column. If None, default to 'variable'.
    :param value_name: Name to use for the 'value' column. If none, default to 'value'.
    :return: long version of dataframe
    """

    # We assume id_vars is a strict subset of value_vars
    if not value_vars:
        value_vars = list(set(df.columns) - set(id_vars))

    _vars_and_vals = F.array(
        *(
            F.struct(F.lit(c).alias(var_name), F.col(c).alias(value_name))
            for c in value_vars
        )
    )

    _temp = df.withColumn("_vars_and_vals", F.explode(_vars_and_vals))

    cols = id_vars + [
        F.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]
    ]

    return_df = _temp.select(*cols)

    return return_df


def remove_columns(df, *args):
    """
    Removes columns from DataFrame
    """

    for column_name in args:
        df = df.drop(column_name)

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


def standardize_schema(dataframe, index_name, mapping_name):
    """Select only columns that are in specified document mapping"""

    doc_mapping = select_mapping(index_name, mapping_name)["properties"]
    columns_to_keep = [c for c in dataframe.columns if c in doc_mapping.keys()]
    return_df = dataframe.select(*columns_to_keep)

    return return_df


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
        match = re.findall(re.compile(r"(\d+)(?:\D+?)*(\d+)*(?:\D+)"), aa_change)
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


def _get_array_size_threshold(df: sql.DataFrame, field: str, percentile: int) -> int:
    assert 0 <= percentile <= 100, "Percentile must be between 0 and 100."

    df = df.groupBy().agg(F.sort_array(F.collect_list(F.size(field))).alias("sizes"))

    if percentile == 100:
        df = df.select(F.element_at("sizes", -1).alias("threshold"))

    else:
        sizes = F.col("sizes")
        position = F.col("position")
        offset = F.col("offset")
        position_value = F.col("position_value")
        df = df.withColumn(
            "raw_position", (F.size("sizes") - 1) * float(percentile) / 100
        )
        df = df.select(
            "sizes",
            F.floor("raw_position").alias("position"),
            (F.col("raw_position") - F.floor("raw_position")).alias("offset"),
        )
        df = df.select(
            sizes[position].alias("position_value"),
            sizes[position + 1].alias("next_value"),
            offset,
        )
        df = df.select(
            F.floor(
                position_value + (F.col("next_value") - position_value) * offset
            ).alias("threshold"),
        )

    return more_itertools.one(df.toLocalIterator()).threshold


def filter_large_arrays(
    df: sql.DataFrame,
    field: str,
    size_percentile: int,
) -> sql.DataFrame:
    """
    Truncates df_to_truncate to remove rows where field > percentile_threshold
    """
    if size_percentile >= 100:
        return df

    threshold = _get_array_size_threshold(df, field, size_percentile)

    logger.info(f"Removing all arrays in {field} with length greater than {threshold}.")

    return df.where(F.size(field) <= threshold)
