from collections.abc import Iterable

import marshmallow
import pytest
from tests.integration.utils import test_setup

from mutation_indexer import configuration
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
        validator = configuration.build.IndexTypesValidator()

        assert viz_indices == validator(viz_indices)

    def test__call__ge_indices(self) -> None:
        ge_indices = (build.IndexType.GENE_EXPRESSION,)
        validator = configuration.build.IndexTypesValidator()

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
        validator = configuration.build.IndexTypesValidator()

        with pytest.raises(marshmallow.ValidationError):
            validator(indices)


class TestLoadConfiguration:
    @pytest.mark.parametrize("data_release", ("dr40", "dr1", ""))
    @pytest.mark.parametrize("build_version", ("v1", "v20", ""))
    @pytest.mark.parametrize(
        "backup_path",
        (
            "",
            "path",
            "{build[data_release]}/{build[build_version]}/file_{build[data_release]}.parquet",
        ),
    )
    def test_backup_path_honors_datarelease_and_buildversion(
        self, data_release: str, build_version: str, backup_path: str
    ) -> None:
        overrides = {
            "build": {"data_release": data_release, "build_version": build_version},
            "builders": {"index": {"backup": {"path": backup_path}}},
        }
        config = test_setup.load_ge_config(overrides)

        assert config.builders.index.backup.path == backup_path.format_map(
            {"build": {"build_version": build_version, "data_release": data_release}}
        )
