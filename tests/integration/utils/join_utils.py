import functools
import itertools
from collections.abc import Callable, Iterable, Iterator, Mapping, Set
from typing import NamedTuple, Union

import more_itertools
from pyspark import sql
from pyspark.sql import functions as F

RelationshipMapping = Mapping[str, Union[Set[str], "RelationshipMapping"]]


class Relationship(NamedTuple):
    parent_field: str
    path_to_child: str
    child_field: str


def _follow_path(path: str) -> Callable[[sql.Row], Iterable[sql.Row]]:
    if not path:
        return lambda r: (r,)

    def follow_path(row: sql.Row, path: str) -> Iterable[sql.Row]:
        head_path, next_path = more_itertools.padded(path.split("."), n=2)
        result = row[head_path]

        if not next_path:
            if isinstance(result, Iterable):
                yield from result
            else:
                yield result
        else:
            if isinstance(result, Iterable):
                yield from itertools.chain.from_iterable(
                    follow_path(r, next_path) for r in result
                )
            else:
                yield from follow_path(result, next_path)

    return functools.partial(follow_path, path=path)


def _relationship_reduce(
    relationships: Iterator[Relationship],
) -> Callable[[Iterable[Iterable[sql.Row]]], Set[str] | RelationshipMapping,]:
    relationship = more_itertools.first(relationships)

    return lambda groups: _get_relationship_map(
        relationship, relationships, itertools.chain(*groups)
    )


def _get_relationship_map(
    relationship: Relationship,
    relationships: Iterator[Relationship],
    rows: Iterable[sql.Row],
) -> RelationshipMapping:
    next_relationship = more_itertools.first(relationships, default=None)

    if next_relationship:
        relationships = more_itertools.prepend(next_relationship, relationships)

        return more_itertools.map_reduce(
            rows,
            keyfunc=lambda r: r[relationship.parent_field],
            valuefunc=_follow_path(relationship.path_to_child),
            reducefunc=_relationship_reduce(relationships),
        )

    assert (
        relationship.child_field
    ), "Child field must be defined in the terminal relationship"

    return more_itertools.map_reduce(
        rows,
        keyfunc=lambda r: r[relationship.parent_field],
        valuefunc=_follow_path(relationship.path_to_child),
        reducefunc=lambda rs: frozenset(
            r[relationship.child_field] for r in itertools.chain(*rs)
        ),
    )


def _get_relationships(path: Iterable[str]) -> Iterator[Relationship]:
    def _split_path(path: str) -> tuple[str, str]:
        split = path.split(".")

        return ".".join(split[:-1]), split[-1]

    child_field = ""

    for parent, child in more_itertools.pairwise(path):
        parent_field = child_field or parent
        path_to_child, child_field = _split_path(child)

        yield Relationship(parent_field, path_to_child, child_field)


def get_relationship_map(
    data: sql.DataFrame | Iterable[sql.Row], relationship_path: Iterable[str]
) -> RelationshipMapping:
    """
    Builds a mapping of a parent object's ID to all of its childrens IDs found in the
    rows of the given data frame.

    Args:
        data: Either a dataframe or an iterable of rows which contain the given
            relationship path.
        relationship_path: A series of string values which all represent a parent child
            relationship where the first element is the parent, second child, third
            grandchild and etc. The parent field will be the key in the resulting
            RelationshipMapping and the value will be all associated child field values,
            either as keys in their own sub-RelationshipMapping or as values in a Set.

            Note: all child fields can be a path through structures but the terminal key
                must relate to a scalar value in the data.
                e.g. `this.is.in.a.nested.object.obj_id`: this would reteive all child
                obj_ids values found nested object.

    Returns:
        A mapping in which the key is the ID of the parent object and the values is a
        set of all unique child IDs associated with the parent object.
    """
    relationships = _get_relationships(relationship_path)
    relationship = more_itertools.first(relationships)

    if isinstance(data, sql.DataFrame):
        data = data.toLocalIterator()

    return _get_relationship_map(relationship, relationships, data)


def unpack_df_list(
    dataframe: sql.DataFrame,
    parent_fields: str | Iterable[str],
    list_field: str,
    packed_fields: str | Iterable[str],
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

    # this split allows deeper parent fields like "foo.bar"
    aliases = tuple(f.split(".")[-1] for f in parent_fields)
    all_fields = itertools.chain(aliases, child_fields)

    return dataframe.select(
        F.explode(list_field).alias(exploded_alias),
        *(F.col(f).alias(a) for f, a in zip(parent_fields, aliases)),
    ).select(*all_fields)
