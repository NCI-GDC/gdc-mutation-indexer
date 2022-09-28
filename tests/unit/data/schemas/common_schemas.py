import pathlib

from tests.unit.data import schemas


class GeneModel(schemas.Schema):
    INPUT = "raw_gene_model.json"
    FINAL = "final_gene_model.json"

    @property
    def schema_dir(self) -> pathlib.Path:
        return pathlib.Path("builders", "gene_model")
