import dataclasses
from os import path
from typing import Dict, Tuple
from unittest import mock
import more_itertools

import pytest
from pyspark import sql
from pyspark.sql import types

from exports import builders
from exports.builders import gene_model
from exports.configuration.builders import common, gene_expression
from exports.constants import build
from tests.unit import utils


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


@dataclasses.dataclass(frozen=True)
class PrimaryAliquot:
    file_id: str = "file-0"


@dataclasses.dataclass(frozen=True)
class GeneModel:
    _gene_id: str = "ESF4003032"
    biotype: str = "protein_coding"
    symbol: str = "HDse"


@pytest.fixture(scope="class")
def schema_dir(data_dir: str) -> str:
    return path.join(data_dir, "schemas", "builders", "gene_expression", "value")


@pytest.fixture(scope="class")
def star_counts_schema(schema_dir: str) -> types.StructType:
    return utils.load_schema(schema_dir, "input_star_counts.json")


@pytest.fixture(scope="class")
def final_schema(schema_dir: str) -> types.StructType:
    return utils.load_schema(schema_dir, "final_value.json")


class TestGeneExpressionValueInputBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        spark_session: sql.SparkSession,
        star_counts_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.star_counts_schema = star_counts_schema
        self.final_schema = final_schema

    def arrange_indexd_dataframe_util(
        self, star_counts: Tuple[STARCountsData, ...] = (STARCountsData(),)
    ) -> mock.MagicMock:
        star_counts_df = self.spark_session.createDataFrame(
            star_counts, schema=self.star_counts_schema
        )
        dataframe_util = mock.MagicMock()

        dataframe_util.get_dataframe.return_value = star_counts_df

        return dataframe_util

    def arrange_inputs(
        self,
        gene_model: Tuple[GeneModel, ...] = (GeneModel(),),
        primary_aliquots: Tuple[PrimaryAliquot, ...] = (PrimaryAliquot(),),
    ) -> Dict[str, sql.DataFrame]:
        gene_model_df = self.spark_session.createDataFrame(
            gene_model, schema="_gene_id: string, biotype: string, symbol: string"
        )
        primary_aliquot_df = self.spark_session.createDataFrame(
            primary_aliquots, schema="file_id: string"
        )

        return {
            "gene_model_df": gene_model_df,
            "gene_expression_primary_aliquot_df": primary_aliquot_df,
        }

    def arrange_config(self) -> gene_expression.Builder:
        return gene_expression.Builder(
            is_cached=True,
            backup=common.Backup(mode=build.BackupMode.NEITHER, path=""),
            projects=(),
        )

    def test__build_from_scratch__single_row(self) -> None:
        config = self.arrange_config()
        sql_context = mock.MagicMock()
        dataframe_util = self.arrange_indexd_dataframe_util()
        inputs = self.arrange_inputs()
        builder = builders.GeneExpressionValueInputBuilder(
            config, sql_context, dataframe_util
        )

        result_df = builder.build_from_scratch(**inputs)

        assert result_df.count() == 1
        assert result_df.schema == self.final_schema

    def test__build_from_scratch__data_transformed(self) -> None:
        star_count_data = STARCountsData()
        gene_model = GeneModel()

        config = self.arrange_config()
        sql_context = mock.MagicMock()
        dataframe_util = self.arrange_indexd_dataframe_util((star_count_data,))
        inputs = self.arrange_inputs(gene_model=(gene_model,))
        builder = builders.GeneExpressionValueInputBuilder(
            config, sql_context, dataframe_util
        )

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.file_id == star_count_data.did
        assert len(result_row.genes) == 1

        for gene in result_row.genes:
            assert gene.gene_id == gene_model._gene_id
            assert gene.expression_value == star_count_data.fpkm_uq_unstranded
            assert gene.symbol == gene_model.symbol

    def test__build_from_scratch__gene_id_stripped(self) -> None:
        star_counts = (STARCountsData(gene_id="ESF4003032.4"),)

        config = self.arrange_config()
        sql_context = mock.MagicMock()
        dataframe_util = self.arrange_indexd_dataframe_util(star_counts)
        inputs = self.arrange_inputs()
        builder = builders.GeneExpressionValueInputBuilder(
            config, sql_context, dataframe_util
        )

        result_df = builder.build_from_scratch(**inputs)
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
    def test__build_from_scratch__filter_non_protein_coding(
        self, biotype: str, gene_type: str
    ) -> None:
        star_counts = (STARCountsData(gene_type=gene_type),)
        gene_model = (GeneModel(biotype=biotype),)

        config = self.arrange_config()
        sql_context = mock.MagicMock()
        dataframe_util = self.arrange_indexd_dataframe_util(star_counts)
        inputs = self.arrange_inputs(gene_model=gene_model)
        builder = builders.GeneExpressionValueInputBuilder(
            config, sql_context, dataframe_util
        )

        result_df = builder.build_from_scratch(**inputs)

        assert result_df.count() == 0

    def test__build_from_scratch__genes_aggregated_by_file_id(self) -> None:
        star_counts = (
            STARCountsData(gene_id="gene-0"),
            STARCountsData(gene_id="gene-1"),
        )
        gene_model = (GeneModel("gene-0"), GeneModel("gene-1"))

        config = self.arrange_config()
        sql_context = mock.MagicMock()
        dataframe_util = self.arrange_indexd_dataframe_util(star_counts)
        inputs = self.arrange_inputs(gene_model=gene_model)
        builder = builders.GeneExpressionValueInputBuilder(
            config, sql_context, dataframe_util
        )

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.file_id == "file-0"
        assert frozenset(gene.gene_id for gene in result_row.genes) == frozenset(
            ("gene-0", "gene-1")
        )
