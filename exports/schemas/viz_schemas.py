import pathlib

from exports import schemas


class MAF(schemas.Schema):
    AGGREGATED_SOMATIC_MUTATION = "aggregated_somatic_mutation.yaml"
    MASKED_SOMATIC_MUTATION = "masked_somatic_mutation.yaml"

    @property
    def schema_dir(self) -> pathlib.Path:
        return pathlib.Path("builders", "maf")
