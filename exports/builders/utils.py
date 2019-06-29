import re
import uuid
import logging
from functools import partial
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark.sql.functions import (
    col,
    regexp_extract,
    struct,
    rand,
    udf,
    UserDefinedFunction,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from exports.mappers.model_mapper import ModelMapper
from exports.es_utils import iterate_es_results
from exports.builders.aliquot import AliquotBuilder

from config import LOG_FORMAT

logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger("BaseBuilder")

# Name of the temporary column used by skew_join.
SKEW_COLUMN = '_skew_correction_'


def ssm_label(chromosome, variant_type, start_pos, end_pos, ref_allele,
              tumor_allele):
    """
    Create a label (genomic change) from an ssm based on its variant type:

    SNP: "{chromosome}:g.{start_position}{reference_allele}>{tumor_allele}"
    DEL: "{chromosome}:g.{start_position}del{reference_allele}"
    INS: "{chromosome}:g.{start_position}_{end_position}ins{tumor_allele}"

    :param chromosome: The chromosome where the mutation occurred
    :param variant_type: The variant, `SNP`, `DEL`, or `INS`
    :param start_pos: The starting position of the mutation
    :param end_pos: The end position of the mutation
    :param ref_allele: The reference allele
    :param tumor_allel: The tumor allele
    """
    chromosome = chromosome.replace('chr', '')

    if variant_type == 'SNP':
        label = 'chr{}:g.{}{}>{}'.format(chromosome,
                                         start_pos, ref_allele, tumor_allele)
    elif variant_type == 'DEL':
        label = 'chr{}:g.{}del{}'.format(chromosome,
                                         start_pos, ref_allele)
    elif variant_type == 'INS':
        label = 'chr{}:g.{}_{}ins{}'.format(chromosome,
                                            start_pos, end_pos, tumor_allele)
    else:
        label = chromosome

    return label


def multiply_df(df, id_column, n):
    """Multiply the row count by N, updating the IDs at the given column."""
    if n <= 1:
        return df

    # Turn each row to a list and back so we can preserve the field order in
    # the original schema. Start by figuring out which entry in the list
    # corresponds to the ID column.
    column_index = df.columns.index(id_column)

    def multiply_column(row):
        old_id = row[id_column]

        new_rows = []
        for i in xrange(n):
            # Append the counter to make the new IDs unique and predictable.
            new_row_data = list(row)
            new_row_data[column_index] = '{}-{}'.format(old_id, i)
            new_rows.append(Row(*new_row_data))

        return new_rows

    sqlContext = df.sql_ctx
    new_rdd = df.rdd.flatMap(multiply_column)
    new_df = sqlContext.createDataFrame(new_rdd, schema=df.schema)

    return new_df


def skew_join(left, right, left_id, right_id, how=None, skew_correction=16):
    """
    Join a large dataframe with a small one, compensating for ID skew.

    Scale the small dataframe (on the right) by the given correction factor,
    then use that to distribute the rows in the large dataframe (on the left)
    more evenly during the join. A larger correction factor yields more even
    partitioning at the expense of a larger temporary right-hand dataframe.

    Mitigate scenarios where, e.g., 50% of the rows in the left-hand dataframe
    share the same ID and Spark would otherwise shuffle all of those rows into
    a single partition to carry out the join.

    :param left: Dataframe for the (large) left side of the join.
    :type left: DataFrame
    :param right: Dataframe for the (small) right side of the join.
    :type right: DataFrame
    :param left_id: Name of the ID column to join in the left dataframe.
    :type left_id: str
    :param right_id: Name of the ID column to join in the right dataframe.
    :type right_id: str
    :param how: What kind of join to execute. May be inner or left.
        Defaults to inner.
    :type how: str
    :param skew_correction: Scaling factor for correcting skew.
    :type skew_correction: int
    :return: A new dataframe with the two joined together.
    """
    # Expand the right-hand dataframe with flatMap and this UDF because
    # the obvious way (crossJoin) sometimes doesn't perform well.
    def add_skew_column(row):
        return [row + (i,) for i in xrange(skew_correction)]

    salted_df = left.withColumn(
        SKEW_COLUMN,
        functions.floor(rand() * skew_correction)
    )

    sqlContext = right.sql_ctx
    scaled_rdd = right.rdd.flatMap(add_skew_column)
    scaled_schema = right.schema.add(SKEW_COLUMN, IntegerType())
    scaled_df = sqlContext.createDataFrame(scaled_rdd,
                                           schema=scaled_schema)

    joined_df = salted_df.join(
        scaled_df,
        ((salted_df[left_id] == scaled_df[right_id])
            & (salted_df[SKEW_COLUMN] == scaled_df[SKEW_COLUMN])),
        how=how
    )
    joined_df = joined_df.drop(SKEW_COLUMN)

    return joined_df


def get_case_ids_from_source_es(config, sqlContext):
    """
    Queries source es for case_ids and acls that correspond to maf aliquots.

    This function returns the acls associated with the
    case_id -> aliquot -> maf_url -> maf_filename.

    1) if any of the observations is open then case level is open;
    2) if all observations are controlled
    and populated with the same dbgap study code,
    then case level will be the same dbgap study code;
    3) if study code in 2) have different values from observation,
    then there is something wrong.
    """

    # Read unique aliquots from maf headers
    aliquot_to_url = AliquotBuilder(config, sqlContext).build()

    # Convert to dictionary
    aliquot_to_url = aliquot_to_url.select('aliquot_id', 'url').rdd.collectAsMap()
    unique_aliquots = aliquot_to_url.keys()

    query = {
        "_source": ["_id", "samples.portions.analytes.aliquots.submitter_id"],
        "query": {
            "nested": {
                "path": "samples.portions.analytes.aliquots",
                "query": {
                    "constant_score": {
                        "filter": {
                            "terms": {
                                "samples.portions.analytes.aliquots.submitter_id": list(unique_aliquots)
                            }
                        }
                    }
                }
            }
        }
    }

    results = iterate_es_results(
        config.es,
        config.graph_index,
        config.graph_document,
        query=query
    )

    cases_urls = {}
    case_ids = set()

    # walk to the aliquot
    for hit in results:
        case_id = hit["_id"]
        case_ids.add(case_id)
        samples = hit['_source']['samples']
        aliquots_to_lookup = []
        for sample in samples:
            portions = sample['portions']
            for portion in portions:
                analytes = portion['analytes']
                for analyte in analytes:
                    aliquots = analyte['aliquots']
                    for aliquot in aliquots:
                        submitter_id = aliquot['submitter_id']
                        if submitter_id in unique_aliquots:
                            aliquots_to_lookup.append(submitter_id)

        # go from aliquots to url to maf_name to acl
        aliquot_acls = []
        for aliquot in aliquots_to_lookup:
            url = aliquot_to_url[aliquot]
            filename = config.maf_url_to_file_name(url)
            acl = config.acls[filename]
            aliquot_acls.append(acl)

        # dedupe (annoying because acls are lists)
        aliquot_acls = list(set(x for l in aliquot_acls for x in l))

        assert 0 < len(aliquot_acls) <= 2, 'Invalid acls ' \
            'for case {}, aliquot(s) {}, phsids {}' \
            ''.format(case_id, aliquots_to_lookup, aliquot_acls)

        # If only one acl across aliquots, use that
        if len(aliquot_acls) == 1:
            cases_urls[case_id] = aliquot_acls
        else:
            # If we find more than one acl, we must have
            # the scenario [open, phsid000x]
            # ([phsid000x, phsid000y] means something is wrong)
            assert u'open' in aliquot_acls, 'Multiple phsids ' \
                'found for case {}, aliquots {}, phsids {}' \
                ''.format(case_id, aliquots_to_lookup, aliquot_acls)

            cases_urls[case_id] = [u'open']

    # We found a url for each case
    assert len(case_ids) == len(cases_urls)

    # There may be more than one aliquot per case
    # I.e., the following example is valid:
    #
    # case 1: aliquot x, aliquot y
    # case 2: aliquot z
    #
    # (or)
    #
    # aliquot | case
    # --------------
    #    x    | 1
    #    y    | 1
    #    z    | 2
    assert len(unique_aliquots) >= len(case_ids)

    # Create a dataframe from case_ids set
    cases_df = sqlContext.createDataFrame(
        ((x, y) for x, y in cases_urls.items()), ['case_id', 'case_acl']
    )

    return cases_df


def get_aliquots_from_headers(sqlContext, maf_urls):
    """
    Reads a set of tuples of (unique aliquots, maf headers)
    """
    unique_aliquots = set()
    aliquot_to_url = {}
    for url in maf_urls:
        if str(url).endswith('DR-10.0.somatic.maf.gz'):
            header = read_maf_header(sqlContext, url, n_lines=5).collect()
            header = map(lambda r: r.asDict().values()[0].split(), header)
            assert header[-2][0] == '#n.analyzed.samples'
            assert header[-1][0] == '#tumor.aliquots.submitter_id'
            aliquots = header[-1][1].split(',')
            n_aliquots = int(header[-2][1])

            assert len(aliquots) == n_aliquots, '{} has inconsistent aliquot data in header'.format(url)
            unique_aliquots.update(aliquots)
            for aliquot in aliquots:
                aliquot_to_url[aliquot] = url

    return unique_aliquots, aliquot_to_url


def read_maf_header(sqlContext, url, n_lines=5):
    """
    Reads only maf header
    """
    return sqlContext.read.format('com.databricks.spark.csv')\
                          .options(delimiter='\t')\
                          .load(url).limit(n_lines)


def ssm_label_col(chromosome,
                  variant_type, start_pos, end_pos, ref_allele, tumor_allele):
    return udf(ssm_label, StringType())(chromosome, variant_type, start_pos,
                                        end_pos, ref_allele, tumor_allele)


def _udf_uuid5_field(*values):
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
    return str(uuid.uuid5(uuid.NAMESPACE_DNS,
                          '\t'.join([v if type(v) == str else str(v)
                                     for v in values])))


def uuid5_col(*values):
    return udf(_udf_uuid5_field, StringType())(*values)


def ssm_occurrence_uuid(namespace, ssm, case):
    return str(uuid.uuid5(uuid.UUID(str(namespace)), str(ssm) + str(case)))


def ssm_occurrence_uuid_udf(namespace):
    """
    Wraps the ssm_uuid function in a spark udf and injects a given namespace
    """
    ssm_namespaced = partial(ssm_occurrence_uuid, str(namespace))
    return udf(ssm_namespaced, StringType())


def extract_impact(df, column, res_colname):
    """
    Extracts impact from fields like:
    'possibly_damaging(0.614)'

    impact = 'possibly_damaging'
    """

    return df.withColumn(res_colname,
                         regexp_extract(column, '(.*)\(.*\)$', 1))


def extract_score(df, column, res_colname):
    """
    Extracts score from fields like:
    'possibly_damaging(0.614)'

    score = '0.614'
    """

    return df.withColumn(res_colname,
                         regexp_extract(column, '(\w)\((\d*.?(\d?)*)\)$', 2).cast(DoubleType()))


def extract_sift_polyphen(df):
    """
    Extracts '{polyphen|sift}_{impact|score}' from 'PolyPhen' and 'SIFT' columns
    """
    for c in ['PolyPhen', 'SIFT']:
        df = extract_impact(df, c, '{}_impact'.format(c.lower()))
        df = extract_score(df, c, '{}_score'.format(c.lower()))
    df = df.drop('PolyPhen').drop('SIFT')
    return df


def extract_all_effects(val, index=0):
    """
    Extracts an element from all_effects at the given index

    Rows are delimited by ;
    Columns are delimited by , or :
    """
    delimiter = ',' if ',' in val else ':'
    if len(val.split(delimiter)) > index:
        return val.split(delimiter)[index]


def all_effects_udf(index):
    f = partial(extract_all_effects, index=index)
    return udf(f, StringType())


def extract_rows_udf():
    vals = udf(lambda x: x.split(';'), ArrayType(StringType()))
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
    udf = UserDefinedFunction(map_function, StringType())
    df = df.withColumn(new_column_name, udf(getattr(df, target_column_name)))
    return df


def melt_df(df,
            id_vars,
            value_vars=None,
            var_name='variable',
            value_name='value',
            filter_zeroes=False):
    """
    "Unpivot" a dataframe by creating a separate row for each value column.

    :param df: input pyspark.DataFrame
    :param id_vars: Column(s) to use as identifier variables
    :type id_vars: Iterable
    :param value_vars: Column(s) to unpivot. If not specified, use all columns
        that are not set as id_vars.
    :type value_vars: Iterable
    :param var_name: Name for the 'variable' column.
        If None, default to 'variable'.
    :type var_name: str
    :param value_name: Name for the 'value' column.
        If none, default to 'value'.
    :type value_name: str
    :param filter_zeroes: Whether to omit the rows for zero/falsy values.
    :type filter_zeroes: bool
    :return: long version of dataframe
    """

    # Default to unpivoting everything that isn't an ID.
    if not value_vars:
        value_vars = list(set(df.columns) - set(id_vars))

    # Preserve the schema of the original data frame as best we can.
    # Since we'll have to merge the values into a single column anyway, assume
    # we can guess the value type by looking at just one.
    schema = StructType(
        [df.schema[col] for col in id_vars] +
        [StructField(var_name, StringType()),
         StructField(value_name, df.schema[value_vars[0]].dataType)]
    )

    # Filter out the string '0' because the Gistic data has that for neutral
    # measurements rather than the numeric 0.
    def melt_row(row):
        ids = [row[id_var] for id_var in id_vars]

        new_rows = []
        for var in value_vars:
            value = row[var]
            if not filter_zeroes or (value and value != '0'):
                new_rows.append(ids + [var, value])

        return new_rows

    rdd = df.rdd.flatMap(melt_row)
    return df.sql_ctx.createDataFrame(data=rdd, schema=schema)


def remove_columns(df, *args):
    """
    Removes columns from DataFrame
    """

    for column_name in args:
        df = df.drop(column_name)

    return df


def select_mapping(index_name, mapping_name):
    mapper = ModelMapper(index_name)

    paths_map = mapper.paths_map
    exclude_map = mapper.exclude_map

    steps = paths_map[mapping_name][index_name]
    exclude_fields = exclude_map[mapping_name][index_name]

    mapping = access_json_path(dict(mapper.type_mappings), steps)

    mapping['properties'] = {k: v for k, v in mapping['properties'].items()
                             if k not in exclude_fields}

    return mapping


def standardize_schema(dataframe, index_name, mapping_name):
    """Select only columns that are in specified document mapping"""

    doc_mapping = select_mapping(index_name, mapping_name)['properties']
    columns_to_keep = [c for c in dataframe.columns
                       if c in doc_mapping.keys()]
    return_df = dataframe.select(*columns_to_keep)

    return return_df


def struct_select(index_name, mapping_name, ignore=[]):
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
        cols = []
        if type(doc) is dict:
            for k, v in doc.items():
                # Ignore OICR autocomplete features
                if (k == 'gene_aa_change'
                    or k == 'copy_to'
                    or '_autocomplete' in k):
                    pass

                elif 'type' in v and 'properties' not in v:
                    name = k
                    if 'default' in v:
                        name = v['default']
                    cols.append(col(name).alias(k))
                else:
                    if k not in ignore and 'properties' in v:
                        cols.append(struct(restructure(v['properties'])).alias(k))
                    elif k not in ignore:
                        cols.append(struct(restructure(v)).alias(k))
                    else:
                        cols.append(k)
        return cols

    mapping = select_mapping(index_name, mapping_name)

    return restructure(mapping['properties'])


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

    return (vector[floored_pos] + (vector[floored_pos + 1] -
                                   vector[floored_pos]) * rest)


def extract_aas_position(df):
    """
    create aa_start and aa_end field based on aa_change string
    there is one problem that synonymous_variant aa_change does not contain
    aa position information
    """
    def extract(aa_change, start=True):
        match = re.findall(re.compile('(\d+)(?:\D+?)*(\d+)*(?:\D+)'), aa_change)
        if match:
            aa_start, aa_end = match[0]
            if start or not aa_end:
                return int(aa_start)

            return int(aa_end)
        return 'null'

    df = df.withColumn('aa_start', udf(extract, IntegerType())(col('aa_change')))
    df = df.withColumn('aa_end', udf(lambda aa_change: extract(aa_change, False),
                                     IntegerType())(col('aa_change')))

    return df


def sanitize_aa_change(df):
    """
    Removes 'p.' from aa_change
    """
    def sanitize(aa_change):
        return aa_change.strip('p.')

    df = df.withColumn('aa_change', udf(sanitize, StringType())(col('aa_change')))

    return df


def sanitize_gene_aa_change(df):
    """
    Removes nulls, empty strings and duplicates from gene_aa_change;
    Sorts gene_aa_change
    """
    def sanitize(gene_aa_change):
        gene_aa_change = [x for x in gene_aa_change if x not in [None, '']]
        return sorted(list(set(gene_aa_change)))

    df = df.withColumn('gene_aa_change',
                       udf(sanitize, ArrayType(StringType()))(col('gene_aa_change')))

    return df


def convert_empty_str_to_null_in_col(df, col_name):
    '''
    Converts empty string to null in df.col_name
    '''

    return df.withColumn(col_name,
            when(col(col_name) != "", col(col_name))
            .otherwise(None))
