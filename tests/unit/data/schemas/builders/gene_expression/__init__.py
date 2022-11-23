from tests.unit.data import schemas


class Input(schemas.Schema):
    STAR_COUNTS = "value/input_star_counts.json"
    PRIMARY_ALIQUOT_FILE = "primary_aliquot/input_file.json"

    @property
    def _prefix(self) -> str:
        return "builders/gene_expression"


class Final(schemas.Schema):
    CASE = "case/final_case.json"
    EXPRESSION_VALUE = "value/final_value.json"
    GENE_EXPRESSION = "gene_expression/final_gene_expression.json"
    PRIMARY_ALIQUOT = "primary_aliquot/final.json"

    @property
    def _prefix(self) -> str:
        return "builders/gene_expression"
