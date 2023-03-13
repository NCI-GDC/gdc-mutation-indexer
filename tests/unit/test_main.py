from unittest import mock

from exports import builders, main
from exports.builders import civic
from exports.constants import build


def test__get_viz_builders__all_builders() -> None:
    _builders = main.get_viz_builders(
        mock.MagicMock(), mock.MagicMock(), mock.MagicMock()
    )
    index_builders = _builders.index_builders
    viz_builders = {b.output: b for b in _builders.builders}

    assert viz_builders.keys() == frozenset(
        (
            build.DataFrame.ASCAT,
            build.DataFrame.CASE,
            build.DataFrame.GENE_MODEL,
            build.DataFrame.MAF,
            build.DataFrame.MAF_METADATA,
            build.DataFrame.PRIMARY_ALIQUOT,
            build.DataFrame.CIVIC_DNA,
            build.DataFrame.CIVIC_PROT,
        )
    )
    assert isinstance(viz_builders[build.DataFrame.ASCAT], builders.ASCATBuilder)
    assert isinstance(viz_builders[build.DataFrame.CASE], builders.CaseBuilder)
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
    assert isinstance(viz_builders[build.DataFrame.CIVIC_DNA], civic.DNABuilder)
    assert isinstance(viz_builders[build.DataFrame.CIVIC_PROT], civic.PROTBuilder)

    assert index_builders.keys() == frozenset(
        (
            build.IndexType.CASE_CENTRIC,
            build.IndexType.CNV_CENTRIC,
            build.IndexType.CNV_OCCURRENCE_CENTRIC,
            build.IndexType.GENE_CENTRIC,
            build.IndexType.SSM_CENTRIC,
            build.IndexType.SSM_OCCURRENCE_CENTRIC,
        )
    )
    assert isinstance(
        index_builders[build.IndexType.CASE_CENTRIC],
        builders.CaseCentricBuilder,
    )
    assert isinstance(
        index_builders[build.IndexType.CNV_CENTRIC],
        builders.CNVCentricBuilder,
    )
    assert isinstance(
        index_builders[build.IndexType.CNV_OCCURRENCE_CENTRIC],
        builders.CNVOccurrenceCentricBuilder,
    )
    assert isinstance(
        index_builders[build.IndexType.GENE_CENTRIC],
        builders.GeneCentricBuilder,
    )
    assert isinstance(
        index_builders[build.IndexType.SSM_CENTRIC],
        builders.SSMCentricBuilder,
    )
    assert isinstance(
        index_builders[build.IndexType.SSM_OCCURRENCE_CENTRIC],
        builders.SSMOccurrenceCentricBuilder,
    )


def test__get_ge_builders__all_builders() -> None:
    _builders = main.get_ge_builders(
        mock.MagicMock(), mock.MagicMock(), mock.MagicMock()
    )
    ge_builders = {b.output: b for b in _builders.builders}
    index_builders = _builders.index_builders

    assert ge_builders.keys() == frozenset(
        (
            build.DataFrame.CASE,
            build.DataFrame.EXPRESSION_VALUE,
            build.DataFrame.GENE_MODEL,
            build.DataFrame.PRIMARY_ALIQUOT,
        )
    )
    assert isinstance(
        ge_builders[build.DataFrame.CASE],
        builders.GeneExpressionCaseInputBuilder,
    )
    assert isinstance(
        ge_builders[build.DataFrame.EXPRESSION_VALUE],
        builders.GeneExpressionValueInputBuilder,
    )
    assert isinstance(
        ge_builders[build.DataFrame.GENE_MODEL],
        builders.GeneModelBuilder,
    )
    assert isinstance(
        ge_builders[build.DataFrame.PRIMARY_ALIQUOT],
        builders.GeneExpressionPrimaryAliquotBuilder,
    )
    assert isinstance(
        ge_builders[build.DataFrame.PRIMARY_ALIQUOT],
        builders.GeneExpressionPrimaryAliquotBuilder,
    )

    assert index_builders.keys() == frozenset((build.IndexType.GENE_EXPRESSION,))
    assert isinstance(
        index_builders[build.IndexType.GENE_EXPRESSION],
        builders.GeneExpressionBuilder,
    )
