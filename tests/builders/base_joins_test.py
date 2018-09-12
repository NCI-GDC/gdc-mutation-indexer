import json
from pyspark.sql.functions import explode, lit, col


class BaseJoinsTest:

    @staticmethod
    def get_relationship_map(dataframe, parent_id_field, child_id_field):
        """
        Retrieve one-to-many relationship map for 
        :parent_field -> :child_field-s in :dataframe as a dictionary

        Returns:
            dict(): {parent_value: {child_value_1, ..., child_value_N}
        """
        relationships = {}
        for row in dataframe.toJSON().collect():
             row = json.loads(row)
             child_id = row[child_id_field]
             parent_id = row[parent_id_field]
             relationships.setdefault(parent_id, set())
             relationships[parent_id].update({child_id})

        return relationships

    @staticmethod
    def unpack_df_list(dataframe, parent_id_field, packed_field, packed_id_field):
        """
        Explodes packed into a list fields in :dataframe
        Returns flat dataframe with only :parent_id_field and :packed_id_field

        Example:
            Given dataframe of format:
                ssm{}
                  |____ ssm_id
                  |____ consequence[]
                             |___consequence_id

            unpack_df_join(dataframe, 'ssm_id', 'consequence', 'consequence_id')
            will return flat dataframe || ssm_id | consequence_id ||

        """
        unpacked = (
            dataframe.select(parent_id_field,
                             explode(packed_field).alias('exploded'))
                     .select(parent_id_field,
                             'exploded.{}'.format(packed_id_field))
        )

        return unpacked

