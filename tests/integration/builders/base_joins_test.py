import collections
import itertools
from typing import Dict, Iterable, Set, Union

from pyspark import sql
from pyspark.sql import functions as F


class BaseJoinsTest:
    @staticmethod
    def get_relationship_map(
        dataframe: sql.DataFrame, parent_id_field: str, child_id_field: str
    ) -> Dict[str, Set[str]]:
        """
        TODO: support arbitrary depth relationships:
              (parent, child, grandchild, ...)
        Retrieve one-to-many relationship map for
        :parent_field -> :child_field-s in :dataframe as a dictionary

        Returns:
            dict(): {parent_value: {child_value_1, ..., child_value_N}
        """
        relationships = collections.defaultdict(set)
        for row in dataframe.collect():
            child_id = row[child_id_field]
            parent_id = row[parent_id_field]
            relationships[parent_id].update({child_id})

        return relationships

    @staticmethod
    def unpack_df_list(
        df: sql.DataFrame,
        parent_fields: Union[str, Iterable[str]],
        list_field: str,
        packed_fields: Union[str, Iterable[str]],
    ) -> sql.DataFrame:
        """
        Explodes packed into a list fields in :dataframe
        Returns flat dataframe with only :parent_fields and :packed_fields

        Example:
            Given dataframe of format:
                ssm{}
                    |___ ssm_id
                    |___ consequence[]
                             |___consequence_id

            unpack_df_join(
                dataframe, 'ssm_id', 'consequence', 'consequence_id'
            ) will return flat dataframe || ssm_id | consequence_id ||

        NOTE: this works with multiple :parent_fields and :packed_fields too,
            e.g.:
            unpack_df_join(
                dataframe, ['ssm_id', 'foo'],
                'consequence', ['consequence_id', 'bar']
            ) will return flat dataframe
                                    || ssm_id | foo | consequence_id | bar ||

        """
        if isinstance(parent_fields, str):
            parent_fields = (parent_fields,)

        if isinstance(packed_fields, str):
            packed_fields = (packed_fields,)

        exploded_alias = list_field.split(".")[-1]
        child_fields = (f"{exploded_alias}.{f}" for f in packed_fields)
        parent_field_aliases = {f: f.split(".")[-1] for f in parent_fields}
        all_fields = itertools.chain(parent_field_aliases.values(), child_fields)

        unpacked_df = df.select(
            F.explode(list_field).alias(exploded_alias),
            *(F.col(f).alias(a) for f, a in parent_field_aliases.items()),
        ).select(*all_fields)

        return unpacked_df
