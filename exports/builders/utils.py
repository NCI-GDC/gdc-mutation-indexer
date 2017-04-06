import re
import uuid
import yaml
import pkg_resources
import logging
from functools import partial
from pyspark.sql.functions import udf, struct, col, explode, array
from pyspark.sql.types import StringType, ArrayType, LongType, IntegerType

logging.basicConfig()
logger = logging.getLogger("BaseBuilder")


def ssm_label(chromosome, variant_type, start_pos, end_pos, ref_allele, tumor_allele):
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
        label = 'chr{}:g.{}_{}ins{}'.format(chromosome, start_pos,
                                            end_pos, tumor_allele)
    else:
        label = chromosome

    return label


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


def extract_transcript_id(val):
    """
    Extract the transcript ids from the all_effects column

    Rows are delimited by ;
    Columns are delimited by , or :
    """
    delimiter = ',' if ',' in val else ':'
    rows = val.split(';')
    transcript_ids = []
    for r in rows:
        if len(r.split(delimiter)) > 3:
            transcript_ids.append(r.split(delimiter)[3])
        else:
            raise Exception('Unexpected number of transcripts')
    return transcript_ids


def transcript_id_udf():
    return udf(extract_transcript_id, ArrayType(StringType()))


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


def load_mapping(path):
    resource_package = 'exports'
    resource_path = '/'.join(('mappings', path))
    return yaml.safe_load(pkg_resources.resource_string(resource_package,
                                                        resource_path))


def struct_select(path, ignore=[]):
    """
    Takes the structure from a mapping and produces arguements for a select
    to reorganize a flat dataframe of those fields into the desiced structure.
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
    mapping = load_mapping(path)

    select = ()

    def restructure(doc):
        cols = []
        if type(doc) is dict:
            for k,v in doc.items():
                if 'type' in v and 'properties' not in v:
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

    select = restructure(mapping['properties'])
    return select


def percentile(vector, p):
    """
    Calculates the p percentile of vector
    """
    sorted_vector = sorted(vector)
    vector_len = len(vector)
    position = (vector_len-1)*float(p)/100
    floored_pos = int(position)
    rest = position - floored_pos
    if floored_pos >= vector_len - 1:
        return sorted_vector[vector_len - 1]

    return sorted_vector[floored_pos] +\
           (sorted_vector[floored_pos+1] - sorted_vector[floored_pos]) * rest

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

    df = df.withColumn('aa_start', udf(extract,IntegerType())(col('aa_change')))
    df = df.withColumn('aa_end', udf(lambda aa_change: extract(aa_change, False),
        IntegerType())(col('aa_change')))

    return df
