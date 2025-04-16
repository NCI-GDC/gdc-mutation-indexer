from collections.abc import Iterable

import marshmallow
import pytest

from mutation_indexer import configuration
from mutation_indexer.constants import build
from tests.integration.utils import test_setup


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


class TestBuild:
    def test__is_viz_build__true_if_no_gene_expression(self) -> None:
        viz_build = configuration.build.Build(
            build_version="",
            config_file="",
            data_release="",
            driver="",
            error_log="",
            index_types=(build.IndexType.GENE_CENTRIC,),
            jar_dir="",
            manifest_dir="",
            output_log="",
            spark_submit="",
            study_label="",
            pex_file="",
            projects=(),
        )

        assert viz_build.is_viz_build()

    def test__is_viz_build__false_if_gene_expression(self) -> None:
        ge_build = configuration.build.Build(
            build_version="",
            config_file="",
            data_release="",
            driver="",
            error_log="",
            index_types=(build.IndexType.GENE_EXPRESSION,),
            jar_dir="",
            manifest_dir="",
            output_log="",
            spark_submit="",
            study_label="",
            pex_file="",
            projects=(),
        )

        assert not ge_build.is_viz_build()


class TestLoadConfiguration:
    @pytest.mark.parametrize("data_release", ("dr40", "dr1", ""))
    @pytest.mark.parametrize("build_version", ("v1", "v20", ""))
    @pytest.mark.parametrize(
        "backup_path",
        (
            "",
            "path",
            "{data_release}",
            "{build_version}",
            "{data_release}/{build_version}/file.parquet",
            "/path/{data_release}/{build_version}/file_{data_release}_{build_version}.parquet",
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

        assert config.builders.index.backup.path == backup_path.format(
            data_release=data_release, build_version=build_version
        )
