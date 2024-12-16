import dataclasses
from typing import Optional, Tuple

import more_itertools
from pyspark import sql


@dataclasses.dataclass(frozen=True)
class VariantCalling:
    variant_caller: Optional[str] = "ASCAT"


@dataclasses.dataclass(frozen=True)
class Observation:
    copy_number: Optional[int] = 3
    observation_id: Optional[str] = "obs-1"
    sample_ploidy_integer: Optional[int] = 2
    variant_calling: VariantCalling = VariantCalling()
    variant_status: Optional[str] = "Tumor Only"


@dataclasses.dataclass(frozen=True)
class Observations:
    case_id: Optional[str] = "case-0"
    cnv_id: Optional[str] = "cnv-0"
    observation: Tuple[Observation, ...] = (Observation(),)
    occurrence_id: Optional[str] = "occ-1"


def assert_observation_translated(
    result_gene: sql.Row, observations: Observations
) -> None:
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
