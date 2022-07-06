import marshmallow_dataclass

from exports.configuration.builders import gene_expression, viz


@marshmallow_dataclass.dataclass(frozen=True)
class Builders:
    gene_expression: gene_expression.GeneExpression
    viz: viz.Viz
