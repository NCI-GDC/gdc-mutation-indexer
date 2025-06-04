"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""

from __future__ import annotations

import dataclasses
import pathlib
from typing import Annotated, Mapping

from marshmallow import fields

from mutation_indexer.configuration import _extensions
from mutation_indexer.constants import build


@dataclasses.dataclass(frozen=True)
class Connection:
    """
    Configuration values for managing the connection to elasticsearch
    """

    nodes: str
    user: str
    password: Annotated[str, _extensions.SecretStringField]
    verify_certs: bool
    ca_certs: Annotated[
        pathlib.Path | None,
        _extensions.ResolvedPathField(
            validate=_extensions.PathValidator(
                is_optional=True, is_file=True, exists=True
            )
        ),
    ] = None


@dataclasses.dataclass(frozen=True)
class Read:
    """
    Configuration values for reading from elasticsearch
    """

    case_index: str
    file_index: str
    max_source_filter_length: int


@dataclasses.dataclass(frozen=True)
class Write:
    """
    Configuration values for writing to elasticsearch
    """

    batch_size_bytes: str
    batch_size_entries: int
    indices: Annotated[
        Mapping[build.IndexType, str],
        fields.Dict(
            keys=fields.Enum(build.IndexType), values=_extensions.FormatMapRootField
        ),
    ]


@dataclasses.dataclass(frozen=True)
class Elasticsearch:
    """
    Configuration values for interacting with elasticsearch
    """

    connection: Connection
    read: Read
    write: Write
