from pyspark.sql.functions import (
    struct, col,
)
from exports.builders.utils import select_mapping

import logging
logger = logging.getLogger('clinical_annotation')


def get_clinical_annotation_df(index_name, input_df, drop_fields=(), unique_fields=None):
    mapping = select_mapping(index_name, 'ssm')

    def restructure(doc, parent_name):
        """
        Takes the structure from a mapping and produces arguments for a select
        to reorganize a flat dataframe of clinical annotations into the desired structure.
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
        ```
        """

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
                        cols.append(struct(restructure(v['properties'], k)).alias(k))
                    else:
                        cols.append(struct(restructure(v, k)).alias(k))
        return cols

    name = 'clinical_annotations'
    cols = ['ssm_id'] + restructure({name: mapping['properties'].get(name)}, '')
    logger.info(cols)

    df = input_df.select(*cols)
    df = df.drop_duplicates(subset=unique_fields)
    return reduce(lambda cur_df, col: cur_df.drop(col), drop_fields, df)
