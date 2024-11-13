"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://gdc-ctds.atlassian.net/wiki/spaces/GDC/pages/76316689/Mutation+Indexer+Procedure#Configuration
"""

import dataclasses


@dataclasses.dataclass(frozen=True)
class SQLiteDatabase:
    @dataclasses.dataclass(frozen=True)
    class Destination:
        bucket: str
        key: str

    batch_size: int
    destination: Destination


@dataclasses.dataclass(frozen=True)
class Databases:
    gene_expression: SQLiteDatabase
