"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""

import dataclasses
from collections.abc import Iterable
from importlib import abc, resources
from typing import Annotated

from mutation_indexer import configuration
from mutation_indexer.configuration import (
    _extensions,
    aws,
    builders,
    databases,
    elasticsearch,
    indexd,
)

# these are directly imported to created a better interface when using the
# config module
from mutation_indexer.configuration.builders import GeneModelBuilder
from mutation_indexer.constants import app


@dataclasses.dataclass(frozen=True)
class BinaryBuilder(builders.Builder):
    bucket: str
    log2_uqfpkm_key: Annotated[str, _extensions.FormatMapRootField]
    uqfpkm_key: Annotated[str, _extensions.FormatMapRootField]


@dataclasses.dataclass(frozen=True)
class CaseBuilder(builders.Builder):
    @dataclasses.dataclass(frozen=True)
    class Destination:
        bucket: str
        key: Annotated[str, _extensions.FormatMapRootField]

    destination: Destination


@dataclasses.dataclass(frozen=True)
class CaseSQLBuilder(builders.Builder):
    ...


@dataclasses.dataclass(frozen=True)
class ExpressionValueBuilder(builders.Builder):
    ...


@dataclasses.dataclass(frozen=True)
class GeneSQLBuilder(builders.Builder):
    ...


@dataclasses.dataclass(frozen=True)
class IndexBuilder(builders.IndexBuilder):
    @dataclasses.dataclass(frozen=True)
    class PartitionedBackup(builders.Backup):
        partition_by: str
        path: Annotated[str, _extensions.FormatMapRootField]

    backup: PartitionedBackup


@dataclasses.dataclass(frozen=True)
class PrimaryAliquotBuilder(builders.Builder):
    ...


@dataclasses.dataclass(frozen=True)
class Builders:
    """
    Configuration values for running the exprot the gene expression indices
    """

    binary: BinaryBuilder
    case: CaseBuilder
    case_sql: CaseSQLBuilder
    expression_value: ExpressionValueBuilder
    index: IndexBuilder
    gene_model: GeneModelBuilder
    gene_sql: GeneSQLBuilder
    primary_aliquot: PrimaryAliquotBuilder


class Configuration(configuration.Configuration):
    aws: aws.AWS
    builders: Builders
    sqlite_database: databases.SQLiteDatabase
    elasticsearch: elasticsearch.Elasticsearch
    indexd: indexd.IndexD

    @classmethod
    def _default_files(cls) -> Iterable[abc.Traversable]:
        return (
            *super()._default_files(),
            resources.files(app.Driver.GENE_EXPRESSION.module) / app.CONFIGURATION_FILE,
        )
