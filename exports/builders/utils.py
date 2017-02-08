import os
import uuid
import yaml
import pkg_resources
from functools import partial

from pyspark.sql.functions import udf, struct, col
from pyspark.sql.types import StringType, ArrayType, IntegerType


def ssm_label(chromosome, variant_type, start_pos, end_pos, ref_allele, tumor_allele):
    '''
    Create a label from an ssm based on its variant type:

    SNP: "{chromosome}:g.{start_position}{reference_allele}>{tumor_allele}"
    DEL: "{chromosome}:g.{start_position}del{reference_allele}"
    INS: "{chromosome}:g.{start_position}_{end_position}ins{tumor_allele}"
    '''
    chromosome = chromosome.replace('chr', '')
    if variant_type is 'SNP':
        label = '{}:g.{}{}>{}'.format(chromosome, start_pos, ref_allele, tumor_allele)
    elif variant_type is 'DEL':
        label = '{}:g.{}del{}'.format(chromosome, start_pos, ref_allele)
    elif variant_type is 'INS':
        label = '{}:g.{}_{}ins{}'.format(chromosome, start_pos, end_pos, tumor_allele)
    else:
        label = chromosome

    return label


def ssm_uuid(namespace, chromosome, variant_type, start_pos, end_pos, ref_allele, tumor_allele):
    '''
    Creates a uuid from a mutation

    '''
    label = ssm_label(chromosome, variant_type, start_pos, end_pos, ref_allele, tumor_allele)
    return str(uuid.uuid5(uuid.UUID(str(namespace)), str(label)))


def ssm_uuid_udf(namespace):
    '''
    Wraps the ssm_uuid function in a spark udf and injects a given namespace
    '''
    ssm_namespaced = partial(ssm_uuid, str(namespace))
    return udf(ssm_namespaced, StringType())


def ssm_occurrence_uuid(namespace, ssm, case):
    return str(uuid.uuid5(uuid.UUID(str(namespace)), str(ssm) + str(case)))


def ssm_occurrence_uuid_udf(namespace):
    '''
    Wraps the ssm_uuid function in a spark udf and injects a given namespace
    '''
    ssm_namespaced = partial(ssm_occurrence_uuid, str(namespace))
    return udf(ssm_namespaced, StringType())


def flat_fields(path):
    '''
    Produces a flat list of properties from a yaml file, used to select columns
    from the maf dataframe to be restructured later.
    Eg:
    ```
    properties:
      center:
        type: keyword
      input_bam_file:
        properties:
          normal_bam_uuid:
            type: keyword
    ```
    Becomes:
    `['center', 'normal_bam_uuid']`
    '''
    resource_package = 'exports'
    resource_path = '/'.join(('mappings', path))
    mapping = yaml.safe_load(pkg_resources.resource_string(resource_package, resource_path))

    flat = set()

    def flatten(doc, name=''):
        if type(doc) is dict:
            for k, v in doc.items():
                if 'type' in doc:
                    flat.add(name)
                    return
                flatten(v, k)
    flatten(mapping)
    return list(flat)


def extract_transcript_id(val):
    '''
    Extract the transcript ids from the all_effects column

    Rows are delimited by ;
    Columns are delimited by , or :
    '''
    delimiter = ',' if ',' in val else ';'
    rows = val.split(';')
    transcript_ids = []
    for r in rows:
        if len(r.split(delimiter)) > 3:
            transcript_ids.append(r.split(delimiter)[3])
    return transcript_ids


def transcript_id_udf():
    return udf(extract_transcript_id, ArrayType(StringType()))


def struct_select(path, ignore=[]):
    '''
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
    '''
    resource_package = 'exports'
    resource_path = '/'.join(('mappings', path))
    mapping = yaml.safe_load(pkg_resources.resource_string(resource_package, resource_path))

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
