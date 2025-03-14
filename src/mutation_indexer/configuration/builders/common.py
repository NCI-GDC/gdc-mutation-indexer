"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""

import dataclasses
from typing import Annotated, Sequence

from mutation_indexer.configuration import marshmallow_extensions
from mutation_indexer.constants import build


@dataclasses.dataclass(frozen=True)
class Backup:
    """
    Configuration values for backing up the output of a builder.
    """

    mode: build.BackupMode
    path: Annotated[str, marshmallow_extensions.ResolvedURI]


@dataclasses.dataclass(frozen=True)
class Builder:
    """
    The core configuration values for running any builder.
    """

    is_cached: bool
    backup: Backup
    projects: Sequence[str]
    acl: Sequence[str]


@dataclasses.dataclass(frozen=True)
class ResourceBuilder(Builder):
    package: str
    resource: str
    schema: str


@dataclasses.dataclass(frozen=True)
class IndexBuilder(Builder):
    """
    The core configuration values for running the any builder which produces an index.
    """

    partition_size: int
    id_field: str


@dataclasses.dataclass(frozen=True)
class GeneModelBuilder(Builder):
    """
    Configuration values for running the gene model builder.
    """

    census_file: Annotated[str, marshmallow_extensions.ResolvedURI]
    citobands_file: Annotated[str, marshmallow_extensions.ResolvedURI]
    gene_model_file: Annotated[str, marshmallow_extensions.ResolvedURI]
