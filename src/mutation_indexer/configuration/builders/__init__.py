"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""
import dataclasses

from mutation_indexer.configuration.builders import gene_expression, viz


@dataclasses.dataclass(frozen=True)
class Builders:
    """
    Configuration values for running the collective builders
    """

    gene_expression: gene_expression.GeneExpression
    viz: viz.Viz
