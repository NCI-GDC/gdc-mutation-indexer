"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""

import dataclasses

from mutation_indexer.configuration.builders import common

# these are directly imported to created a better interface when using the
# gene_expression module
from mutation_indexer.configuration.builders.common import (
    Builder,
    GeneModelBuilder,
    IndexBuilder,
)


class GeneExpressionIndexBuilder(IndexBuilder):
    @dataclasses.dataclass(frozen=True)
    class PartitionedBackup(common.Backup):
        partition_by: str

    backup: PartitionedBackup


class ValueArrayBuilder(Builder):
    bucket: str
    key_pattern: str


@dataclasses.dataclass(frozen=True)
class GeneExpression:
    """
    Configuration values for running the exprot the gene expression indices
    """

    gene_model: GeneModelBuilder
    case: Builder
    expression_value: Builder
    primary_aliquot: Builder
    gene_expression: GeneExpressionIndexBuilder
