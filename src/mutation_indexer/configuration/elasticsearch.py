"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""

import dataclasses
from typing import Mapping

from mutation_indexer.configuration import marshmallow_extensions
from mutation_indexer.constants import build


@dataclasses.dataclass(frozen=True)
class Connection:
    """
    Configuration values for managing the connection to elasticsearch
    """

    nodes: str
    user: str
    password: marshmallow_extensions.SecretString
    use_ssl: bool
    verify_certs: bool


@dataclasses.dataclass(frozen=True)
class Read:
    """
    Configuration values for reading from elasticsearch
    """

    case_index: str
    file_index: str


@dataclasses.dataclass(frozen=True)
class Write:
    """
    Configuration values for writing to elasticsearch
    """

    batch_size_bytes: str
    batch_size_entries: int
    indices: Mapping[build.IndexType, str]


@dataclasses.dataclass(frozen=True)
class Elasticsearch:
    """
    Configuration values for interacting with elasticsearch
    """

    connection: Connection
    read: Read
    write: Write
