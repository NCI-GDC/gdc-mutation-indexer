from unittest import mock

from exports import builders, main
from exports.constants import build


def test__get_viz_builders__all_builders() -> None:
    config = mock.MagicMock()
    config.build.index_types = (
        build.IndexType.CASE_CENTRIC,
        build.IndexType.CNV_CENTRIC,
        build.IndexType.CNV_OCCURRENCE_CENTRIC,
        build.IndexType.GENE_CENTRIC,
        build.IndexType.SSM_CENTRIC,
        build.IndexType.SSM_OCCURRENCE_CENTRIC,
    )
    _builders = main.get_viz_builders(
        config, mock.MagicMock(), mock.MagicMock()
    )
    viz_builders = {b.output: b for b in _builders.builders}

    assert viz_builders.keys() == frozenset(
        (
            build.DataFrame.ASCAT,
            build.DataFrame.CASE,
            build.DataFrame.GENE_MODEL,
            build.DataFrame.MAF,
            build.DataFrame.MAF_METADATA,
            build.DataFrame.PRIMARY_ALIQUOT,
            build.DataFrame.CASE_CENTRIC,
        )
    )
    assert isinstance(viz_builders[build.DataFrame.ASCAT], builders.ASCATBuilder)
    assert isinstance(
        viz_builders[build.DataFrame.GENE_MODEL],
        builders.GeneModelBuilder,
    )
    assert isinstance(viz_builders[build.DataFrame.MAF], builders.MAFBuilder)
    assert isinstance(
        viz_builders[build.DataFrame.MAF_METADATA],
        builders.MAFMetadataBuilder,
    )
    assert isinstance(
        viz_builders[build.DataFrame.PRIMARY_ALIQUOT],
        builders.PrimaryAliquotBuilder,
    )
    assert isinstance(
        viz_builders[build.DataFrame.CASE_CENTRIC],
        builders.CaseCentricBuilder,
    )

    assert _builders.index_builders.keys() == frozenset(
        (
            build.IndexType.CNV_CENTRIC,
            build.IndexType.CNV_OCCURRENCE_CENTRIC,
            build.IndexType.GENE_CENTRIC,
            build.IndexType.SSM_CENTRIC,
            build.IndexType.SSM_OCCURRENCE_CENTRIC,
        )
    )
    assert isinstance(
        _builders.index_builders[build.IndexType.CNV_CENTRIC],
        builders.CNVCentricBuilder,
    )
    assert isinstance(
        _builders.index_builders[build.IndexType.CNV_OCCURRENCE_CENTRIC],
        builders.CNVOccurrenceCentricBuilder,
    )
    assert isinstance(
        _builders.index_builders[build.IndexType.GENE_CENTRIC],
        builders.GeneCentricBuilder,
    )
    assert isinstance(
        _builders.index_builders[build.IndexType.SSM_CENTRIC],
        builders.SSMCentricBuilder,
    )
    assert isinstance(
        _builders.index_builders[build.IndexType.SSM_OCCURRENCE_CENTRIC],
        builders.SSMOccurrenceCentricBuilder,
    )


def test__get_ge_builders__all_builders() -> None:
    ge_builders = main.get_ge_builders(
        mock.MagicMock(), mock.MagicMock(), mock.MagicMock()
    )
    _builders = {b.output: b for b in ge_builders.builders}

    assert _builders.keys() == frozenset(
        (
            build.DataFrame.CASE,
            build.DataFrame.EXPRESSION_VALUE,
            build.DataFrame.GENE_MODEL,
            build.DataFrame.PRIMARY_ALIQUOT,
        )
    )
    assert isinstance(
        _builders[build.DataFrame.CASE],
        builders.GeneExpressionCaseInputBuilder,
    )
    assert isinstance(
        _builders[build.DataFrame.EXPRESSION_VALUE],
        builders.GeneExpressionValueInputBuilder,
    )
    assert isinstance(
        _builders[build.DataFrame.GENE_MODEL],
        builders.GeneModelBuilder,
    )
    assert isinstance(
        _builders[build.DataFrame.PRIMARY_ALIQUOT],
        builders.GeneExpressionPrimaryAliquotBuilder,
    )
    assert isinstance(
        _builders[build.DataFrame.PRIMARY_ALIQUOT],
        builders.GeneExpressionPrimaryAliquotBuilder,
    )

    assert ge_builders.index_builders.keys() == frozenset(
        (build.IndexType.GENE_EXPRESSION,)
    )
    assert isinstance(
        ge_builders.index_builders[build.IndexType.GENE_EXPRESSION],
        builders.GeneExpressionBuilder,
    )
