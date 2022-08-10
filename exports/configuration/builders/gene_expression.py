"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""
import dataclasses

# these are directly imported to created a better interface when using the
# gene_expression module
from exports.configuration.builders.common import (
    Builder,
    CentricBuilder,
    GeneModelBuilder,
)


@dataclasses.dataclass(frozen=True)
class GeneExpression:
    """
    Configuration values for running the exprot the gene expression indices
    """
    gene_model: GeneModelBuilder
    case: Builder
    value: Builder
    primary_aliquot: Builder
    gene_expression: CentricBuilder
