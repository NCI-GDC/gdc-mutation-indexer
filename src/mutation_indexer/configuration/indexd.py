"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""

import dataclasses

from mutation_indexer.configuration import marshmallow_extensions


@dataclasses.dataclass(frozen=True)
class IndexD:
    """
    Configuration values for interacting with indexd
    """

    host: str
    port: int
    user: str
    password: marshmallow_extensions.SecretString
