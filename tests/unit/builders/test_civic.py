import dataclasses
from typing import Tuple
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from exports.builders import civic
from exports.configuration.builders import viz
from exports.constants import build
from tests.unit import utils
from tests.unit.data import schemas


@dataclasses.dataclass(frozen=True)
class CivicDNA:
    civic_var_id: str = "1"
    civic_gene_id: str = "3"
    source: str = "gDNA"
    chromosome: str = "chr1"
    start_position: str = "33772590"
    reference_allele: str = "C"
    alternative_allele: str = "A"


@pytest.fixture(scope="class")
def final_dna_schema() -> types.StructType:
    return schemas.load_schema("builders/civic/final_civic_dna.yaml")


class TestCivicDNABuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        spark_session: sql.SparkSession,
        final_dna_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.final_schema = final_dna_schema

    def arrange_config(self) -> viz.CivicDNABuilder:
        config = mock.MagicMock()
        config.dna_file = ""
        config.backup.mode = build.BackupMode.NEITHER

        return config

    def arrange_spark_session(
        self, data: Tuple[CivicDNA, ...] = (CivicDNA(),)
    ) -> sql.SparkSession:
        session = mock.MagicMock(spec=sql.SparkSession)
        session.read.csv.return_value = self.spark_session.createDataFrame(
            utils.to_rows(data)
        )

        return session

    def test__build__data_transformed(self) -> None:
        data = CivicDNA()
        config = self.arrange_config()
        spark_session = self.arrange_spark_session((data,))
        builder = civic.CivicDNABuilder(config, spark_session)

        result_df = builder.build()
        result_row = more_itertools.one(result_df.collect())

        assert result_df.schema == self.final_schema
        assert result_row.civic_variant_id == data.civic_var_id
        assert result_row.civic_gene_id == data.civic_gene_id
        assert result_row.chromosome == data.chromosome
        assert result_row.reference_allele == data.reference_allele
        assert result_row.tumor_allele == data.alternative_allele


@dataclasses.dataclass(frozen=True)
class CivicProt:
    civic_var_id: str = "3"
    civic_gene_id: str = "1"
    hugo_symbol: str = "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
    gene: str = "ENSG00000181163.12"
    hgvsp: str = "p.A569S"
    source: str = "Protein"


@pytest.fixture(scope="class")
def final_prot_schema() -> types.StructType:
    return schemas.load_schema("builders/civic/final_civic_prot.yaml")


class TestCivicProtBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        spark_session: sql.SparkSession,
        final_prot_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.final_schema = final_prot_schema

    def arrange_config(self) -> viz.CivicProtBuilder:
        config = mock.MagicMock()
        config.prot_file = ""
        config.backup.mode = build.BackupMode.NEITHER

        return config

    def arrange_spark_session(
        self, data: Tuple[CivicProt, ...] = (CivicProt(),)
    ) -> sql.SparkSession:
        session = mock.MagicMock(spec=sql.SparkSession)
        session.read.csv.return_value = self.spark_session.createDataFrame(
            utils.to_rows(data)
        )

        return session

    def test__build__data_transformed(self) -> None:
        data = CivicProt()
        config = self.arrange_config()
        spark_session = self.arrange_spark_session((data,))
        builder = civic.CivicProtBuilder(config, spark_session)

        result_df = builder.build()
        result_row = more_itertools.one(result_df.collect())

        assert result_df.schema == self.final_schema
        assert result_row.civic_variant_id == data.civic_var_id
        assert result_row.civic_gene_id == data.civic_gene_id
        assert result_row.name == data.hugo_symbol
        assert result_row.hgvsp_short == data.hgvsp
