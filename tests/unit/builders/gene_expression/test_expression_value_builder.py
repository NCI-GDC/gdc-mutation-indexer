import dataclasses
from typing import Dict, Tuple
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from exports import builders
from exports.configuration.builders import gene_expression
from exports.constants import build
from tests.unit.data import schemas
from tests.unit.data.models import gene_expression as models


@dataclasses.dataclass(frozen=True)
class STARCountsData:
    did: str = "file-0"
    gene_id: str = "ESF4003032"
    gene_name: str = ""
    gene_type: str = "protein_coding"
    unstranded: int = 2
    stranded_first: int = 123
    stranded_second: int = 393
    tpm_unstranded: float = 22.10
    fpkm_unstranded: float = 33902.3
    fpkm_uq_unstranded: float = 22901.8


@pytest.fixture(scope="class")
def star_counts_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.Value.STAR_COUNTS.load()


@pytest.fixture(scope="class")
def primary_aliquot_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.PrimaryAliquot.FINAL.load()


@pytest.fixture(scope="class")
def gene_model_schema() -> types.StructType:
    return schemas.Builders.GeneModel.FINAL.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.Value.FINAL.load()


class TestGeneExpressionValueInputBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        spark_session: sql.SparkSession,
        star_counts_schema: types.StructType,
        primary_aliquot_schema: types.StructType,
        gene_model_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.star_counts_schema = star_counts_schema
        self.primary_aliquot_schema = primary_aliquot_schema
        self.gene_model_schema = gene_model_schema
        self.final_schema = final_schema

    def arrange_indexd_dataframe_util(
        self, star_counts: Tuple[STARCountsData, ...] = (STARCountsData(),)
    ) -> mock.MagicMock:
        star_counts_df = self.spark_session.createDataFrame(
            star_counts,  # type: ignore
            schema=self.star_counts_schema,
        )
        dataframe_util = mock.MagicMock()

        dataframe_util.get_dataframe.return_value = star_counts_df

        return dataframe_util

    def arrange_config(self) -> gene_expression.Builder:
        return mock.MagicMock(
            spec=gene_expression.Builder,
            is_cached=False,
            backup=mock.MagicMock(mode=build.BackupMode.NEITHER, path=""),
        )

    def arrange_inputs(
        self,
        gene_model: Tuple[models.GeneModel, ...] = (
            models.GeneModel(_gene_id="ESF4003032"),
        ),
        primary_aliquots: Tuple[models.PrimaryAliquot, ...] = (
            models.PrimaryAliquot(),
        ),
    ) -> Dict[str, sql.DataFrame]:
        gene_model_df = self.spark_session.createDataFrame(
            gene_model,  # type: ignore
            schema=self.gene_model_schema,
        )
        primary_aliquot_df = self.spark_session.createDataFrame(
            primary_aliquots,  # type: ignore
            schema=self.primary_aliquot_schema,
        )

        return {
            "gene_model_df": gene_model_df,
            "primary_aliquot_df": primary_aliquot_df,
        }

    def test__build__single_row(self) -> None:
        config = self.arrange_config()
        spark_session = mock.MagicMock()
        dataframe_util = self.arrange_indexd_dataframe_util()
        inputs = self.arrange_inputs()
        builder = builders.GeneExpressionValueInputBuilder(
            config, spark_session, dataframe_util
        )

        result_df = builder.build(**inputs)

        assert result_df.count() == 1
        assert result_df.schema == self.final_schema

    def test__build__data_transformed(self) -> None:
        star_count_data = STARCountsData()
        gene_model = models.GeneModel(_gene_id="ESF4003032")

        config = self.arrange_config()
        spark_session = mock.MagicMock()
        dataframe_util = self.arrange_indexd_dataframe_util((star_count_data,))
        inputs = self.arrange_inputs(gene_model=(gene_model,))
        builder = builders.GeneExpressionValueInputBuilder(
            config, spark_session, dataframe_util
        )

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.file_id == star_count_data.did
        assert len(result_row.genes) == 1

        for gene in result_row.genes:
            assert gene.gene_id == gene_model._gene_id
            assert gene.expression_value == star_count_data.fpkm_uq_unstranded
            assert gene.symbol == gene_model.symbol

    def test__build__gene_id_stripped(self) -> None:
        star_counts = (STARCountsData(gene_id="ESF4003032.4"),)

        config = self.arrange_config()
        spark_session = mock.MagicMock()
        dataframe_util = self.arrange_indexd_dataframe_util(star_counts)
        inputs = self.arrange_inputs()
        builder = builders.GeneExpressionValueInputBuilder(
            config, spark_session, dataframe_util
        )

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())
        result_gene = more_itertools.one(result_row.genes)

        assert result_gene.gene_id == "ESF4003032"

    @pytest.mark.parametrize(
        ("biotype", "gene_type"),
        (
            ("protein_coding", "transcribed_unitary_pseudogene"),
            ("transcribed_unitary_pseudogene", "protein_coding"),
        ),
        ids=("bad_star_count_data", "bad_gene_model_data"),
    )
    def test__build__filter_non_protein_coding(
        self, biotype: str, gene_type: str
    ) -> None:
        star_counts = (STARCountsData(gene_type=gene_type),)
        gene_model = (models.GeneModel(_gene_id="ESF4003032", biotype=biotype),)

        config = self.arrange_config()
        spark_session = mock.MagicMock()
        dataframe_util = self.arrange_indexd_dataframe_util(star_counts)
        inputs = self.arrange_inputs(gene_model=gene_model)
        builder = builders.GeneExpressionValueInputBuilder(
            config, spark_session, dataframe_util
        )

        result_df = builder.build(**inputs)

        assert result_df.count() == 0

    def test__build__genes_aggregated_by_file_id(self) -> None:
        star_counts = (
            STARCountsData(gene_id="gene-0"),
            STARCountsData(gene_id="gene-1"),
        )
        gene_model = (
            models.GeneModel(_gene_id="gene-0"),
            models.GeneModel(_gene_id="gene-1"),
        )

        config = self.arrange_config()
        spark_session = mock.MagicMock()
        dataframe_util = self.arrange_indexd_dataframe_util(star_counts)
        inputs = self.arrange_inputs(gene_model=gene_model)
        builder = builders.GeneExpressionValueInputBuilder(
            config, spark_session, dataframe_util
        )

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.file_id == "file-0"
        assert frozenset(gene.gene_id for gene in result_row.genes) == frozenset(
            ("gene-0", "gene-1")
        )
