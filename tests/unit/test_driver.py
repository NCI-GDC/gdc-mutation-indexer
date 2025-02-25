from unittest import mock

from mutation_indexer import builders, driver
from mutation_indexer.builders import civic
from mutation_indexer.constants import build


def test__get_viz_builders__all_builders() -> None:
    viz_builders = driver.get_viz_builders(
        mock.MagicMock(), mock.MagicMock(), mock.MagicMock()
    )
    generic_builders = {b.output: b for b in viz_builders.builders}

    assert generic_builders.keys() == frozenset(
        (
            build.DataFrame.ASCAT,
            build.DataFrame.ASCAT_METADATA,
            build.DataFrame.CASE,
            build.DataFrame.CIVIC_DNA,
            build.DataFrame.CIVIC_PROTEIN,
            build.DataFrame.GENE_MODEL,
            build.DataFrame.MAF,
            build.DataFrame.MAF_METADATA,
            build.DataFrame.PRIMARY_ALIQUOT,
            build.DataFrame.SEGMENT_CNV_METADATA,
            build.DataFrame.SEGMENT_CNV,
        )
    )
    assert isinstance(generic_builders[build.DataFrame.ASCAT], builders.ASCATBuilder)
    assert isinstance(
        generic_builders[build.DataFrame.ASCAT_METADATA], builders.ASCATMetadataBuilder
    )
    assert isinstance(generic_builders[build.DataFrame.CASE], builders.CaseBuilder)
    assert isinstance(generic_builders[build.DataFrame.CIVIC_DNA], civic.DNABuilder)
    assert isinstance(
        generic_builders[build.DataFrame.CIVIC_PROTEIN], civic.ProteinBuilder
    )
    assert isinstance(
        generic_builders[build.DataFrame.GENE_MODEL], builders.GeneModelBuilder
    )
    assert isinstance(generic_builders[build.DataFrame.MAF], builders.MAFBuilder)
    assert isinstance(
        generic_builders[build.DataFrame.MAF_METADATA], builders.MAFMetadataBuilder
    )
    assert isinstance(
        generic_builders[build.DataFrame.PRIMARY_ALIQUOT],
        builders.PrimaryAliquotBuilder,
    )

    assert viz_builders.index_builders.keys() == frozenset(
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
        viz_builders.index_builders[build.IndexType.CASE_CENTRIC],
        builders.CaseCentricBuilder,
    )
    assert isinstance(
        viz_builders.index_builders[build.IndexType.CNV_CENTRIC],
        builders.CNVCentricBuilder,
    )
    assert isinstance(
        viz_builders.index_builders[build.IndexType.CNV_OCCURRENCE_CENTRIC],
        builders.CNVOccurrenceCentricBuilder,
    )
    assert isinstance(
        viz_builders.index_builders[build.IndexType.GENE_CENTRIC],
        builders.GeneCentricBuilder,
    )
    assert isinstance(
        viz_builders.index_builders[build.IndexType.SSM_CENTRIC],
        builders.SSMCentricBuilder,
    )
    assert isinstance(
        viz_builders.index_builders[build.IndexType.SSM_OCCURRENCE_CENTRIC],
        builders.SSMOccurrenceCentricBuilder,
    )


def test__get_ge_builders__all_builders() -> None:
    config = mock.MagicMock()
    config.build.index_types = (build.IndexType.GENE_EXPRESSION,)
    ge_builders = driver.get_ge_builders(config, mock.MagicMock(), mock.MagicMock())
    generic_builders = {b.output: b for b in ge_builders.builders}

    assert generic_builders.keys() == frozenset(
        (
            build.DataFrame.GENE_MODEL,
            build.DataFrame.PRIMARY_ALIQUOT,
            build.DataFrame.EXPRESSION_VALUE,
            build.DataFrame.GENE_EXPRESSION,
        )
    )
    assert isinstance(
        generic_builders[build.DataFrame.GENE_MODEL], builders.GeneModelBuilder
    )
    assert isinstance(
        generic_builders[build.DataFrame.PRIMARY_ALIQUOT],
        builders.GeneExpressionPrimaryAliquotBuilder,
    )
    assert isinstance(
        generic_builders[build.DataFrame.PRIMARY_ALIQUOT],
        builders.GeneExpressionPrimaryAliquotBuilder,
    )
    assert isinstance(
        generic_builders[build.DataFrame.EXPRESSION_VALUE],
        builders.ExpressionValueBuilder,
    )
    assert isinstance(
        generic_builders[build.DataFrame.GENE_EXPRESSION],
        builders.GeneExpressionIndexBuilder,
    )


def test__get_ge_builders__excludes() -> None:
    config = mock.MagicMock()
    config.build.index_types = ()
    ge_builders = driver.get_ge_builders(config, mock.MagicMock(), mock.MagicMock())
    generic_builders = frozenset(ge_builders.builders)

    assert build.DataFrame.GENE_EXPRESSION not in generic_builders
