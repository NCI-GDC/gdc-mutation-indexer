import uuid
from typing import Iterable

import pytest
from marshmallow import validate

from mutation_indexer.configuration import build as build_config
from mutation_indexer.constants import build


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


class TestBuild:
    def test__is_viz_build__true_if_no_gene_expression(self) -> None:
        viz_build = build_config.Build(
            study_label="",
            data_release="",
            build_version="",
            index_types=(build.IndexType.GENE_CENTRIC,),
            projects=(),
            jar_dir="",
            manifest_dir="",
            config_file="",
            build_id=uuid.uuid4(),
        )

        assert viz_build.is_viz_build()

    def test__is_viz_build__false_if_gene_expression(self) -> None:
        ge_build = build_config.Build(
            study_label="",
            data_release="",
            build_version="",
            index_types=(build.IndexType.GENE_EXPRESSION,),
            projects=(),
            jar_dir="",
            manifest_dir="",
            config_file="",
            build_id=uuid.uuid4(),
        )

        assert not ge_build.is_viz_build()
