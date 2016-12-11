import uuid
from functools import partial

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def ssm_label(chromosome, variant_type, start_pos, end_pos, ref_allele, tumor_allele):
    '''
    Create a label from an ssm based on its variant type:

    SNP: "{chromosome}:g.{start_position}{reference_allele}>{tumor_allele}"
    DEL: "{chromosome}:g.{start_position}del{reference_allele}"
    INS: "{chromosome}:g.{start_position}_{end_position}ins{tumor_allele}"
    '''
    chromosome = chromosome.replace('chr','')
    if variant_type is 'SNP':
        label = '{}:g.{}{}>{}'.format(chromosome, start_pos, ref_allele, tumor_allele)
    elif variant_type is 'DEL':
        label = '{}:g.{}del{}'.format(chromosome, start_pos, ref_allele)
    elif variant_type is 'INS':
        label = '{}:g.{}_{}ins{}'.format(chromosome, start_pos, end_pos, tumor_allele)

    return label

def ssm_uuid(namespace, chromosome, variant_type, start_pos, end_pos, ref_allele, tumor_allele):
    '''
    Creates a uuid from a mutation

    '''
    label = ssm_label(chromosome, variant_type, start_pos, end_pos, ref_allele, tumor_allele)
    return str(uuid.uuid5(uuid.UUID(str(namespace)), label))

def ssm_uuid_udf(namespace):
    '''
    Wraps the ssm_uuid function in a spark udf and injects a given namespace
    '''
    ssm_namespaced = partial(ssm_uuid, namespace=str(namespace))
    return udf(ssm_namespaced, StringType())
