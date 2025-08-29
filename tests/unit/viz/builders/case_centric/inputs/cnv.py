import dataclasses

import more_itertools
from pyspark import sql


@dataclasses.dataclass(frozen=True)
class VariantCalling:
    variant_caller: str | None = "ASCAT"


@dataclasses.dataclass(frozen=True)
class Observation:
    copy_number: int | None = 3
    observation_id: str | None = "obs-1"
    sample_ploidy_integer: int | None = 2
    variant_calling: VariantCalling = VariantCalling()
    variant_status: str | None = "Tumor Only"


@dataclasses.dataclass(frozen=True)
class Observations:
    case_id: str | None = "case-0"
    cnv_id: str | None = "cnv-0"
    observation: tuple[Observation, ...] = (Observation(),)
    occurrence_id: str | None = "occ-1"


def assert_observation_translated(result_gene: sql.Row, observations: Observations) -> None:
    result_cnv = more_itertools.one(result_gene.cnv)
    result_observation = more_itertools.one(result_cnv.observation)
    observation = more_itertools.one(observations.observation)

    assert result_observation.copy_number == observation.copy_number
    assert result_observation.observation_id == observation.observation_id
    assert result_observation.sample_ploidy_integer == observation.sample_ploidy_integer
    assert result_observation.variant_status == observation.variant_status
    assert (
        result_observation.variant_calling.variant_caller
        == observation.variant_calling.variant_caller
    )
