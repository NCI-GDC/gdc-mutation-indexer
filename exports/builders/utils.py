import re
import uuid
import logging
from functools import partial
from pyspark.sql.functions import (
    array,
    col,
    concat_ws,
    explode,
    lit,
    regexp_extract,
    struct,
    udf,
    UserDefinedFunction,
    when,
)
from pyspark.sql.types import StringType, ArrayType, DoubleType, IntegerType

from exports.mappers.model_mapper import ModelMapper
from exports.es_utils import iterate_es_results
from exports.builders.aliquot import AliquotBuilder

from config import LOG_FORMAT

logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger("BaseBuilder")


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

    sqlContext = df.sql_ctx
    multiplier_df = sqlContext.createDataFrame([(m,) for m in range(n)],
                                               '_temp_n: int')

    joined_df = df.crossJoin(multiplier_df)
    joined_df = joined_df.withColumn(id_column,
                                     concat_ws('-', joined_df[id_column], joined_df._temp_n))
    joined_df = joined_df.drop('_temp_n')

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
            var_name="variable",
            value_name="value"):
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

    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name))
        for c in value_vars
    ))

    _temp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
        col("_vars_and_vals")[x].alias(x)
        for x in [var_name, value_name]
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
