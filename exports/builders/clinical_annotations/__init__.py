from exports.builders.utils import select_mapping
from pyspark.sql.functions import (
    struct, col,
)

from civic import CivicBuilder
from base import ClinicalAnnotationBuilder

import logging
logger = logging.getLogger('clinical_annotation')


def get_clinical_annotation_df(index_name, input_df, drop_fields=[], unique_fields=None):
    mapping = select_mapping(index_name, 'ssm')

    """
    Takes the structure from a mapping and produces arguments for a select
    to reorganize a flat dataframe of those fields into the desired structure.
    Eg:
    Given the mapping:
    ```
    properties:
      clinical_annotations:
        properties:
          civic:
            properties:
              gene_id:
                type: keyword
              variant_id:
                type: keyword
        type: nested
    ```
    Produce the select arguments:
    
    """

    def restructure(doc, parent_name):
        cols = []
        if type(doc) is dict:
            for k, v in doc.items():
                if 'type' in v and 'properties' not in v:
                    name = '{}_{}'.format(parent_name, k)
                    if 'default' in v:
                        name = v['default']
                    cols.append(col(name).alias(k))
                else:
                    if 'properties' in v:
                        cols.append(restructure(v['properties'], k)).alias(k)
                    else:
                        cols.append(struct(restructure(v, k)).alias(k))
        return cols

    name = 'clinical_annotations'
    cols = ['ssm_id'] + [struct(restructure(mapping['properties'], '')).alias(name)]
    logger.info(cols)

    df = input_df.select(*cols)
    df = df.drop_duplicates(subset=unique_fields)
    return reduce(lambda cur_df, col: cur_df.drop(col), drop_fields, df)
