import json
from pyspark.sql.functions import explode, lit, col


class BaseJoinsTest:

    @staticmethod
    def get_relationship_map(dataframe, parent_id_field, child_id_field):
        """
        TODO: support arbitrary depth relationships: (parent, child, grandchild, ...)
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
    def unpack_df_list(dataframe, parent_fields, list_field, packed_fields):
        """
        Explodes packed into a list fields in :dataframe
        Returns flat dataframe with only :parent_fields and :packed_fields

        Example:
            Given dataframe of format:
                ssm{}
                  |____ ssm_id
                  |____ consequence[]
                             |___consequence_id

            unpack_df_join(dataframe, 'ssm_id', 'consequence', 'consequence_id')
            will return flat dataframe || ssm_id | consequence_id ||

        NOTE: this works with multiple :parent_fields and :packed_fields too, e.g.
            unpack_df_join(dataframe, ['ssm_id', 'foo'],
                           'consequence', ['consequence_id', 'bar'])
            will return flat dataframe || ssm_id | foo | consequence_id | bar ||

        """
        if isinstance(parent_fields, str):
            parent_fields = [parent_fields]

        if isinstance(packed_fields, str):
            packed_fields = [packed_fields]

        child_fields = ['{}.{}'.format(list_field, f) for f in packed_fields]

        all_fields = (
            [f.split('.')[-1] for f in parent_fields] +  # this allows deeper parent fields like "foo.bar"
            child_fields
        )

        unpacked = (
            dataframe.select(explode(list_field).alias(list_field),
                             *parent_fields)
                     .select(*all_fields)
        )

        return unpacked

