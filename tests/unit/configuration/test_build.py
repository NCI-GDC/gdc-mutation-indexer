from typing import Iterable

import pytest
from marshmallow import validate

from exports.configuration import build as build_config
from exports.constants import build


class TestIndexTypesValidator:
    @pytest.mark.parametrize(
        "viz_indices",
        (
            (build.IndexType.GENE_CENTRIC,),
            (
                build.IndexType.CASE_CENTRIC,
                build.IndexType.CNV_CENTRIC,
                build.IndexType.CNV_OCCURRENCE_CENTRIC,
                build.IndexType.GENE_CENTRIC,
                build.IndexType.SSM_CENTRIC,
                build.IndexType.SSM_OCCURRENCE_CENTRIC,
            ),
        ),
        ids=("partial", "all"),
    )
    def test__call__viz_indices(self, viz_indices: Iterable[build.IndexType]) -> None:
        validator = build_config.IndexTypesValidator()

        assert viz_indices == validator(viz_indices)

    def test__call__ge_indices(self) -> None:
        ge_indices = (build.IndexType.GENE_EXPRESSION,)
        validator = build_config.IndexTypesValidator()

        assert ge_indices == validator(ge_indices)

    @pytest.mark.parametrize(
        "indices",
        (
            (build.IndexType.GENE_EXPRESSION, build.IndexType.CNV_CENTRIC),
            (
                build.IndexType.GENE_EXPRESSION,
                build.IndexType.CASE_CENTRIC,
                build.IndexType.CNV_CENTRIC,
                build.IndexType.CNV_OCCURRENCE_CENTRIC,
                build.IndexType.GENE_CENTRIC,
                build.IndexType.SSM_CENTRIC,
                build.IndexType.SSM_OCCURRENCE_CENTRIC,
            ),
            (),
        ),
        ids=("partial", "all", "none"),
    )
    def test__call__both(self, indices: Iterable[build.IndexType]) -> None:
        validator = build_config.IndexTypesValidator()

        with pytest.raises(validate.ValidationError):
            validator(indices)
