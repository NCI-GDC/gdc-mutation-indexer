import unittest

from mutation_indexer.constants import build
from tests.unit import utils


class TestDataFrame(unittest.TestCase):
    @utils.parametrize(
        (build.DataFrame.ASCAT, "ascat_df"),
        (build.DataFrame.CASE, "case_df"),
        (build.DataFrame.GENE_MODEL, "gene_model_df"),
        (build.DataFrame.MAF, "maf_df"),
        (build.DataFrame.MAF_METADATA, "maf_metadata_df"),
        (build.DataFrame.PRIMARY_ALIQUOT, "primary_aliquot_df"),
    )
    def test__to_param(self, dataframe: build.DataFrame, param: str) -> None:
        assert dataframe.to_param() == param

    @utils.parametrize(
        ("ascat_df", build.DataFrame.ASCAT),
        ("case_df", build.DataFrame.CASE),
        ("gene_model_df", build.DataFrame.GENE_MODEL),
        ("maf_df", build.DataFrame.MAF),
        ("maf_metadata_df", build.DataFrame.MAF_METADATA),
        ("primary_aliquot_df", build.DataFrame.PRIMARY_ALIQUOT),
    )
    def test__from_param(self, param: str, dataframe: build.DataFrame) -> None:
        assert build.DataFrame.from_param(param) == dataframe


class TestIndexType(unittest.TestCase):
    @utils.parametrize(
        (build.IndexType.CASE_CENTRIC, "case_centric"),
        (build.IndexType.CNV_CENTRIC, "cnv_centric"),
        (build.IndexType.CNV_OCCURRENCE_CENTRIC, "cnv_occurrence_centric"),
        (build.IndexType.GENE_CENTRIC, "gene_centric"),
        (build.IndexType.SSM_CENTRIC, "ssm_centric"),
        (build.IndexType.SSM_OCCURRENCE_CENTRIC, "ssm_occurrence_centric"),
    )
    def test__get_mapping_details__viz_index(
        self, index_type: build.IndexType, expected_index
    ) -> None:
        index, doc_type = index_type.get_mappings_details()

        assert index == expected_index
        assert doc_type is None

    @utils.parametrize((build.IndexType.CASE, "case"), (build.IndexType.FILE, "file"))
    def test__get_mapping_details__graph_index(
        self, index_type: build.IndexType, expected_doc_type: str
    ) -> None:
        index, doc_type = index_type.get_mappings_details()

        assert index == "gdc_from_graph"
        assert doc_type == expected_doc_type


class TestBackupMode(unittest.TestCase):
    @utils.parametrize((build.BackupMode.WRITE,), (build.BackupMode.BOTH,))
    def test__is_write__true(self, mode: build.BackupMode) -> None:
        assert mode.is_write()

    @utils.parametrize((build.BackupMode.READ,), (build.BackupMode.NEITHER,))
    def test__is_write__false(self, mode: build.BackupMode) -> None:
        assert not mode.is_write()

    @utils.parametrize((build.BackupMode.WRITE,), (build.BackupMode.NEITHER,))
    def test__is_read__false(self, mode: build.BackupMode) -> None:
        assert not mode.is_read()

    @utils.parametrize((build.BackupMode.READ,), (build.BackupMode.BOTH,))
    def test__is_read__true(self, mode: build.BackupMode) -> None:
        assert mode.is_read()
