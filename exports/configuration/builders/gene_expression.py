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
    gene_model: GeneModelBuilder
    case: Builder
    value: Builder
    primary_aliquot: Builder
    gene_expression: CentricBuilder
