import pathlib

from tests.unit.data import schemas


class ASCAT(schemas.Schema):
    INPUT_ES_FILE = "es_file.json"
    INPUT = "input_ascat.yaml"
    FINAL = "final_ascat.json"

    @property
    def schema_dir(self) -> pathlib.Path:
        return pathlib.Path("builders", "ascat")


class Case(schemas.Schema):
    INPUT = "input_case.yaml"
    FINAL = "final_case.yaml"

    @property
    def schema_dir(self) -> pathlib.Path:
        return pathlib.Path("builders", "case")


class Consequence(schemas.Schema):
    FINAL = "final_consequence.yaml"
    FINALWITH_AA_CHANGE = "final_consequence_with_aa_change.yaml"
    FINAL_WITH_GENES = "final_consequence_with_genes.yaml"

    @property
    def schema_dir(self) -> pathlib.Path:
        return pathlib.Path("builders", "consequence")


class MAF(schemas.Schema):
    FINAL = "final_maf.json"

    @property
    def schema_dir(self) -> pathlib.Path:
        return pathlib.Path("builders", "maf")


class MAF_METADATA(schemas.Schema):
    INPUT_ES_FILE = "es_file.yaml"
    FINAL = "final_maf_metadata.yaml"

    @property
    def schema_dir(self) -> pathlib.Path:
        return pathlib.Path("builders", "maf_metadata")


class Observation(schemas.Schema):
    FINAL_CNV = "final_cnv_observation.yaml"
    FINAL_SSM = "final_ssm_observation.json"
    FINAL_SSM_OTHER = "final_other_ssm_observation.json"

    @property
    def schema_dir(self) -> pathlib.Path:
        return pathlib.Path("builders", "observation")


class PrimaryAliquot(schemas.Schema):
    INPUT_ES_FILE = "input_file.json"
    FINAL = "final_primary_aliquot.json"

    @property
    def schema_dir(self) -> pathlib.Path:
        return pathlib.Path("builders", "primary_aliquot")
