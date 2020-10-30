import collections
import pkg_resources
import re
import uuid
import logging
from functools import partial

import dateutil.parser
import yaml
from normalizer.mapper import ModelMapper
from pyspark.sql.functions import (
    lit, udf, struct, col, explode, array, when, regexp_extract,
    UserDefinedFunction,
)
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, StringType, StructField, StructType
)

from exports.es_utils import iterate_es_results
from exports.builders.aliquot import AliquotBuilder
from config import LOG_FORMAT

logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger("BaseBuilder")


DEFAULT_EXCLUDE_FIELDS = {}


def get_default_excludes(index, mapping):
    if DEFAULT_EXCLUDE_FIELDS:
        return set(DEFAULT_EXCLUDE_FIELDS.get(mapping, {}).get(index, []))

    path = pkg_resources.resource_filename('exports',
                                           'schemas/exclude.defaults.yaml')

    with open(path) as f:
        excludes = yaml.safe_load(f)

    for k, v in excludes.items():
        DEFAULT_EXCLUDE_FIELDS[k] = v

    return set(DEFAULT_EXCLUDE_FIELDS.get(mapping, {}).get(index, []))


def ssm_label(chromosome, variant_type, start_pos, end_pos, ref_allele,
              tumor_allele):
    """
    Create a label (genomic change) from an ssm based on its variant type:

    :param chromosome: The chromosome where the mutation occurred
    :param variant_type: The variant type (e.g., ``SNP``, ``DNP``, ``DEL``, ``INS``...)
    :param start_pos: The starting position of the mutation
    :param end_pos: The end position of the mutation
    :param ref_allele: The reference allele
    :param tumor_allele: The tumor allele
    """
    chromosome = chromosome.replace('chr', '')

    if variant_type == 'SNP':
        label = 'chr{}:g.{}{}>{}'.format(chromosome,
                                         start_pos, ref_allele, tumor_allele)
    elif variant_type in {'DNP', 'TNP', 'ONP'}:
        label = 'chr{}:g.{}_{}delins{}'.format(chromosome,
                                               start_pos, end_pos, tumor_allele)
    elif variant_type == 'DEL':
        label = 'chr{}:g.{}del{}'.format(chromosome,
                                         start_pos, ref_allele)
    elif variant_type == 'INS':
        label = 'chr{}:g.{}_{}ins{}'.format(chromosome,
                                            start_pos, end_pos, tumor_allele)
    else:
        label = chromosome

    return label


def _create_aliquot_submitter_id_query(submitter_ids, project_ids):
    """Get an ES query clause for matching cases based on aliquot submitter IDs.

    Optionally add a requirement that the cases be within certain projects.
    """
    aliquot_clause = {
        'nested': {
            'path': 'samples.portions.analytes.aliquots',
            'query': {
                'terms': {
                    'samples.portions.analytes.aliquots.submitter_id': submitter_ids
                }
            },
        }
    }

    if project_ids:
        return {
            'bool': {
                'must': [{'terms': {'project.project_id': project_ids}}, aliquot_clause]
            }
        }

    return aliquot_clause


def get_case_ids_from_source_es(config, sqlContext):
    """Query source ES for case_ids that correspond to MAF aliquots.

    TODO: Make this query ES through Spark instead...?

    Returns:
        A dataframe with a single ``case_id`` column listing the case IDs associated
        with the aliquots identified by `AliquotBuilder`.
    """

    project_filter = frozenset(config.projects) if config.projects else None

    # Read unique aliquots from maf headers
    aliquot_df = AliquotBuilder(config, sqlContext).build()

    # Figure out which aliquots are required to be in certain projects and which
    # could come from anywhere.
    floating_submitter_ids = set()
    submitter_ids_by_project = collections.defaultdict(set)
    for aliquot in aliquot_df.toLocalIterator():
        if aliquot.project_id:
            # If we were configured only to build certain projects, then there's no
            # point in tracking aliquots from other projects.
            if (not project_filter) or aliquot.project_id in project_filter:
                submitter_ids_by_project[aliquot.project_id].add(aliquot.submitter_id)
        else:
            floating_submitter_ids.add(aliquot.submitter_id)

    # Build queries for those aliquot IDs with each of the projects we split out.
    clauses = [
        _create_aliquot_submitter_id_query(list(submitter_ids), [project_id])
        for project_id, submitter_ids in submitter_ids_by_project.items()
    ]

    if floating_submitter_ids:
        floating_clause = _create_aliquot_submitter_id_query(
            list(floating_submitter_ids), config.projects
        )
        clauses.append(floating_clause)

    query = {'_source': False, 'query': {'bool': {'should': clauses}}}

    results = iterate_es_results(
        config.source_es,
        index_name=config.graph_case_index,
        doc_type=config.graph_case_doc_type,
        query=query,
    )

    cases = [{'case_id': hit['_id']} for hit in results]

    # Do a quick sanity check for the possibility of an aliquot matching multiple cases.
    # TODO Do we want to try harder? What if some cases have multiple aliquots and
    # that offsets problems with other cases?
    num_aliquots = len(floating_submitter_ids) + sum(
        len(ids) for ids in submitter_ids_by_project.values()
    )
    num_cases = len(cases)
    assert num_aliquots >= num_cases, \
        "Found {} aliquots with {} cases".format(num_aliquots, num_cases)

    # Create a dataframe with the case IDs corresponding to the identified aliquots.
    # Give an explicit schema in case we found nothing, as schema inference doesn't
    # work on empty dataframes.
    cases_df_schema = StructType([StructField('case_id', StringType())])
    cases_df = sqlContext.createDataFrame(cases, schema=cases_df_schema)

    return cases_df


def _create_gene_expression_files_query(
    workflow_types,
    acl=("open",),
    projects=None,
):
    """
    Create gene expression files query given target ``workflow_types`` and
    optional ``acl`` and ``projects``

    Args:
        workflow_types(list): list of workflow types to query
        acl(list): file acl list (defaults to open files only)
        projects(list): list of project_id's to limit the query to
    """

    data_type_clause = {"terms": {"data_type": ["Gene Expression Quantification"]}}
    acl_clause = {"terms": {"acl": acl}}
    analysis_type_clause = {"terms": {"analysis.workflow_type": workflow_types}}

    musts = [data_type_clause, acl_clause, analysis_type_clause]

    if projects:
        projects_clause = {
            "nested": {
                "path": "cases",
                "query": {"terms": {"cases.project.project_id": projects}},
            }
        }
        musts.append(projects_clause)

    return {"bool": {"must": musts}}


def _select_primary_aliquot_gene_expressions(es_hits):
    """
    Select gene expression files that correspond to primary aliquots. The details
    can be found in DEV-219.

    In short, the gene expression files should be prioritized by sample type
    the expression values were generated from (highest to lowest):
         1. "Primary Tumor"
         2. "Primary Blood Derived Cancer - Bone Marrow"
         3. "Primary Blood Derived Cancer - Peripheral Blood"
         4. "Metastatic"
         5. "Additional Metastatic"
         6. "Recurrent Tumor"
         7. "Recurrent Blood Derived Cancer - Bone Marrow"
         8. "Recurrent Blood Derived Cancer - Peripheral Blood"
         9. "Additional - New Primary"
         10. Any other `sample_type` sorted alphabetically
         11. If there are ties, then sort by `created_datetime`
         12. If there are ties, then sort by `file_id`

    Args:
        iterable(dict): an iterable object that yield file metadata in as a `dict`

    Returns:
        dict: file_id to case_metadata mapping

    """

    GeneExpressionFile = collections.namedtuple(
        "GeneExpressionFile",
        field_names=["file_id", "created_datetime", "sample_weight"],
    )

    file_map = {}
    case_files = collections.defaultdict(list)
    metadata = {}

    sample_weights = {
        "Primary Tumor": 1,
        "Primary Blood Derived Cancer - Bone Marrow": 2,
        "Primary Blood Derived Cancer - Peripheral Blood": 3,
        "Metastatic": 4,
        "Additional Metastatic": 5,
        "Recurrent Tumor": 6,
        "Recurrent Blood Derived Cancer - Bone Marrow": 7,
        "Recurrent Blood Derived Cancer - Peripheral Blood": 8,
        "Additional - New Primary": 9,
    }

    for hit in es_hits:
        source = hit["_source"]
        file_id = source["file_id"]
        created_time = dateutil.parser.parse(source["created_datetime"])

        # If no cases were returned, there's nothing to do, since we cannot map
        # files back
        if "cases" not in source:
            continue

        case = source["cases"][0]

        case_id = case["case_id"]
        metadata[file_id] = case

        # NOTE: We might encounter a use-case in the future, where we can have
        #   multiple samples associated with a single gene expression file
        sample_type = case["samples"][0]["sample_type"]

        sample_weight = sample_weights.get(sample_type, 10)

        gef = GeneExpressionFile(file_id, created_time, sample_weight)

        case_files[case_id].append(gef)

    for case_id, ge_files in case_files.items():
        ge_files = sorted(
            ge_files,
            key=lambda x: (x.sample_weight, x.created_datetime, x.file_id),
        )
        file_id = ge_files[0].file_id

        file_map[file_id] = metadata[file_id]
        file_map[file_id]["file_id"] = file_id

    return file_map


def is_main_url(metadata):
    """
    Check if given metadata corresponds to main IndexD URL:
        * type == cleversafe
        * state == validated

    Returns:
        bool: True if main URL, False otherwise
    """
    return (
        metadata.get("type") in ["cleversafe"] and
        metadata.get("state") == "validated"
    )


def get_and_format_url(doc):
    """
    Select main IndexD url if one exist and format it to something that Spark
    understands

    Args:
        doc (indexclient.client.Document): IndexD document to extract URL from

    Returns:
        str: formatted main URL
    """
    for url, meta in doc.urls_metadata.items():
        if is_main_url(meta):
            url = url.replace("s3://", "s3a://").replace("cleversafe.service.consul/", "")
            return url
    return None


def _get_main_urls(indexd_client, file_ids):
    """
    Construct a {file_id -> url} mapping, where url is a main URL for each file_id

    Args:
        indexd_client (indexclient.client.IndexClient): indexd client
        file_ids (list): a list of file_ids

    Returns:
        dict: file_id to main url mapping
    """
    urls_map = {}

    batch_n = 0
    batch_size = 1000

    batches = []
    while batch_n * batch_size < len(file_ids):
        batches.append(file_ids[batch_n*batch_size:(batch_n+1)*batch_size])
        batch_n += 1

    for batch_ids in batches:
        docs = indexd_client.bulk_request(batch_ids)

        for doc in docs:
            urls_map[doc.did] = get_and_format_url(doc)

    return urls_map


def get_gene_expression_metadata(
    config,
    source=None,
    workflow_types=None,
    gene_expression_selector=_select_primary_aliquot_gene_expressions,
):
    """
    Query ES graph file index to extract case and gene expression metadata,
    return the results in a list
    """
    query = _create_gene_expression_files_query(
        projects=config.projects,
        workflow_types=workflow_types,
    )

    if source is None:
        source = ["cases.case_id", "cases.samples.sample_type"]

    body = {
        "_source": source,
        "query": query,
    }

    results = iterate_es_results(
        config.source_es,
        index_name=config.graph_file_index,
        doc_type=config.graph_file_doc_type,
        query=body,
    )

    file_map = gene_expression_selector(results)

    urls_map = _get_main_urls(config.indexd, list(file_map))

    files = []
    for file_id, url in urls_map.items():
        if url is None:
            logger.warning("File is missing: '{}'".format(file_id))
        else:
            meta = file_map[file_id]
            meta["file_url"] = url
            files.append(meta)

    return files


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
    Extracts '{polyphen|sift}_{impact|score}' from 'polyphen' and 'sift' columns
    """
    for c in ['polyphen', 'sift']:
        df = extract_impact(df, c, '{}_impact'.format(c.lower()))
        df = extract_score(df, c, '{}_score'.format(c.lower()))
    df = df.drop('polyphen').drop('sift')
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


def select_mapping(index_name, mapping_name, selector=None,
                   exclude_fields=None):
    if exclude_fields is None:
        exclude_fields = get_default_excludes(index_name, mapping_name)

    mapper = ModelMapper(index_name)
    mapping = mapper.select_mapping(mapping_name, selector)

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
            if (k == 'gene_aa_change' or k == 'copy_to'
                    or '_autocomplete' in k
                    or k == 'clinical_annotations'):
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

    mapping = select_mapping(index_name, mapping_name, selector=selector)

    return restructure(mapping['properties'])


def select_nested(index_name, mapping_name, ignore=(), selector=None):
    def flatten_nested(doc):
        if not isinstance(doc, dict):
            return []

        cols = []
        for k, v in doc.items():
            if (k == 'gene_aa_change' or k == 'copy_to' or
                    '_autocompolete' in k or k == 'clinical_annotations'):
                continue

            if k in ignore:
                continue

            if 'type' in v and 'properties' not in v:
                name = k
                if 'default' in v:
                    name = v['default']
                cols.append(col(name).alias(k))
            else:
                if 'properties' in v:
                    cols.extend(flatten_nested(v['properties']))
                else:
                    cols.extend(flatten_nested(v))
        return cols

    mapping = select_mapping(index_name, mapping_name, selector=selector,
                             exclude_fields=())

    return flatten_nested(mapping['properties'])


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
    """
    Converts empty string to null in df.col_name
    """

    return df.withColumn(
        col_name,
        when(col(col_name) != "", col(col_name)).otherwise(None)
    )


def get_column_name(column_name, dataset_key):
    return '{}_{}'.format(column_name, dataset_key)


def transform_variant_caller(callers):
    partitioned = callers.split(';')
    sanitized = [caller.strip('*') for caller in partitioned]

    return sanitized
