import decimal
import logging
import re
import uuid
from collections.abc import Container, Set
from typing import Any, Optional

import pkg_resources
import yaml
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types
from gdcmodels import mapper, esmodels

from mutation_indexer import es_utils
from mutation_indexer.constants import build

logger = logging.getLogger(__name__)


DEFAULT_EXCLUDE_FIELDS: dict[str, dict[str, Set[str]]] = {}


def get_default_excludes(index, mapping):
    if DEFAULT_EXCLUDE_FIELDS:
        return set(DEFAULT_EXCLUDE_FIELDS.get(mapping, {}).get(index, []))

    path = pkg_resources.resource_filename(
        "mutation_indexer", "schemas/exclude.defaults.yaml"
    )

    with open(path) as f:
        excludes = yaml.safe_load(f)

    for k, v in excludes.items():
        DEFAULT_EXCLUDE_FIELDS[k] = v

    return set(DEFAULT_EXCLUDE_FIELDS.get(mapping, {}).get(index, []))


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


def select_mapping(
    index_name: str,
    mapping_name: str,
    selector: Optional[mapper.Selector] = None,
    exclude_fields: Optional[Container[str]] = None,
) -> esmodels.ESMapping:
    """
    Selects the sub-mapping from the index.

    Args:
        index_name: The name of the index containing the mapping.
        mapping_name: The name of the sub-mapping in the index.
        selector: An optional function to filter the paths found to the given mapping or
            the name of a parent property which must be found in the valid path for the
            mapping. If none, it is assumed there is only one path to the given
            mapping_name in the index mapping.
        exclude_fields: An optional set of fields within the mapping which should be
            excluded.

    Returns:
        The sub-mapping found within the given index.
    """
    if exclude_fields is None:
        exclude_fields = get_default_excludes(index_name, mapping_name)

    model_mapper = es_utils.MappingsLoader().load_mappings(
        build.IndexType[index_name.upper()]
    )
    mapping = model_mapper.select_mapping(mapping_name, selector)

    assert "properties" in mapping

    return {
        "properties": {
            k: v for k, v in mapping["properties"].items() if k not in exclude_fields
        }
    }


def struct_select(
    index_name: str,
    mapping_name: str,
    ignore: Container[str] = (),
    selector: Optional[mapper.Selector] = None,
):
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


def is_protein_coding() -> sql.Column:
    """
    Returns:
        A column which represents whether or not a gene in the gene model is a protein
        coding gene based on the biotype.
    """
    return F.col("biotype") == F.lit("protein_coding")


def is_between_chr1_and_chr22() -> sql.Column:
    """
    Returns:
        A column which represents whether or not a gene in the gene model with a
        chromosome value between char1 and char22.
    """
    return F.coalesce(F.col("chromosome").cast(types.IntegerType()), F.lit(-1)).between(
        1, 22
    )


def filter_arrays_by_relative_size(
    df: sql.DataFrame,
    array_field: str,
    size_percentile: int,
) -> sql.DataFrame:
    """
    Filters the given data frame to only include rows where the array in the given field
    has a size which is under the given percentile threshold (based on the size of all
    of the arrays in the given column).

    Args:
        df: The data frame containing the array field which will be filtered.
        array_field: The name of the column containing the array in the data frame.
        percentile: The percentile threshold which will not be exceeded in the resulting
            data frame. Should be an integer from 0 to 100.

    Returns:
        A data frame with the arrays with a size larger than the given percentile
        threshold removed. If the percentile is 100 then the data frame is returned
        without modification.
    """
    if size_percentile >= 100:
        return df

    # Standardizes the given percentile to an equivalent decimal value, e.g. 99 -> 0.99
    percentile_decimal = size_percentile / decimal.Decimal("100")
    percentile_window = sql.Window.orderBy("_size")
    df = df.select("*", F.size(array_field).alias("_size")).select(
        "*", F.percent_rank().over(percentile_window).alias("_percentile")
    )
    df = df.where(F.col("_percentile") <= F.lit(percentile_decimal))

    return df.drop("_size", "_percentile")
