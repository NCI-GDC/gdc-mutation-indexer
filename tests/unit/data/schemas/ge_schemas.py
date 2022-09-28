import pathlib

from tests.unit.data import schemas


class Case(schemas.Schema):
    FINAL = "final_case.json"

    @property
    def schema_dir(self) -> pathlib.Path:
        return pathlib.Path("builders", "gene_expression", "case")


class GeneExpression(schemas.Schema):
    FINAL = "final_gene_expression.json"

    @property
    def schema_dir(self) -> pathlib.Path:
        return pathlib.Path("builders", "gene_expression", "gene_expression")


class PrimaryAliquot(schemas.Schema):
    INPUT_ES_FILE = "gene_expression_input_file.json"
    FINAL = "final_gene_expression.json"

    @property
    def schema_dir(self) -> pathlib.Path:
        return pathlib.Path("builders", "primary_aliquot")


class Value(schemas.Schema):
    INPUT = "input_star_counts.json"
    FINAL = "final_value.json"

    @property
    def schema_dir(self) -> pathlib.Path:
        return pathlib.Path("builders", "gene_expression", "value")
