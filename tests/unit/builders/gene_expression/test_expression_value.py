import math
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from mutation_indexer import indexd_utils
from mutation_indexer.builders.gene_expression import expression_value
from mutation_indexer.constants import build
from mutation_indexer.gene_expression import configuration
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import gene_expression as models


@pytest.fixture(scope="class")
def gene_model_schema() -> types.StructType:
    return schemas.Builders.GeneModel.FINAL.load()


@pytest.fixture(scope="class")
def primary_aliquot_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.PrimaryAliquot.FINAL.load()


@pytest.fixture(scope="class")
def star_counts_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.ExpressionValue.STAR_COUNTS.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.GeneExpression.Builders.ExpressionValue.FINAL.load()


class TestExpressionValueBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        create_dataframe: utils.CreateDataFrame,
        gene_model_schema: types.StructType,
        primary_aliquot_schema: types.StructType,
        star_counts_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self.create_dataframe = create_dataframe
        self.gene_model_schema = gene_model_schema
        self.primary_aliquot_schema = primary_aliquot_schema
        self.star_counts_schema = star_counts_schema
        self.final_schema = final_schema

    def _arrange_inputs(
        self,
        gene_models: tuple[models.GeneModel, ...] = (models.GeneModel(),),
        primary_aliquots: tuple[models.PrimaryAliquot, ...] = (models.PrimaryAliquot(),),
    ) -> dict[str, sql.DataFrame]:
        gene_model_df = self.create_dataframe(gene_models, self.gene_model_schema)
        primary_aliquot_df = self.create_dataframe(
            primary_aliquots, self.primary_aliquot_schema
        )

        return dict(gene_model_df=gene_model_df, primary_aliquot_df=primary_aliquot_df)

    def _arrange_doc_dataframe_util(
        self,
        star_counts: tuple[models.STARCounts, ...] = (models.STARCounts(),),
    ) -> indexd_utils.DataFrameUtil:
        star_counts_df = self.create_dataframe(star_counts, self.star_counts_schema)
        dataframe_util = mock.MagicMock(spec=indexd_utils.DataFrameUtil)
        dataframe_util.get_dataframe.return_value = star_counts_df

        return dataframe_util

    def _arrange_config(self) -> configuration.ExpressionValueBuilder:
        config = mock.MagicMock(
            spec=configuration.ExpressionValueBuilder,
            backup=mock.MagicMock(mode=build.BackupMode.NEITHER, path=""),
            is_cached=False,
            projects=(),
        )

        return config

    def test__build__single_row(self) -> None:
        config = self._arrange_config()
        spark_session = mock.MagicMock()
        doc_dataframe_util = self._arrange_doc_dataframe_util()
        inputs = self._arrange_inputs()
        builder = expression_value.ExpressionValueBuilder(
            config, spark_session, doc_dataframe_util
        )

        result_df = builder.build(**inputs)

        assert result_df.count() == 1
        assert result_df.schema == self.final_schema

    def test__build__data_translated(self) -> None:
        config = self._arrange_config()
        spark_session = mock.MagicMock()
        star_count = models.STARCounts()
        doc_dataframe_util = self._arrange_doc_dataframe_util((star_count,))
        gene_model = models.GeneModel()
        primary_aliquot = models.PrimaryAliquot()
        inputs = self._arrange_inputs((gene_model,), (primary_aliquot,))
        builder = expression_value.ExpressionValueBuilder(
            config, spark_session, doc_dataframe_util
        )

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.case_id == primary_aliquot.case_id
        assert result_row.gene_id == star_count.gene_id == gene_model._gene_id
        assert result_row.submitter_id == primary_aliquot.submitter_id
        assert result_row.symbol == star_count.gene_name
        utils.assert_float_equal(result_row.uqfpkm, star_count.fpkm_uq_unstranded)

    @pytest.mark.parametrize("chromosome", ("0", "23", "Y"))
    def test__build__exclude_non_chr1_to_22(self, chromosome: str) -> None:
        config = self._arrange_config()
        spark_session = mock.MagicMock()
        doc_dataframe_util = self._arrange_doc_dataframe_util()
        gene_model = models.GeneModel(chromosome=chromosome)
        inputs = self._arrange_inputs((gene_model,))
        builder = expression_value.ExpressionValueBuilder(
            config, spark_session, doc_dataframe_util
        )

        result_df = builder.build(**inputs)

        assert result_df.count() == 0

    def test__build__exclude_non_protein_coding(self) -> None:
        config = self._arrange_config()
        spark_session = mock.MagicMock()
        doc_dataframe_util = self._arrange_doc_dataframe_util()
        gene_model = models.GeneModel(biotype="other")
        inputs = self._arrange_inputs((gene_model,))
        builder = expression_value.ExpressionValueBuilder(
            config, spark_session, doc_dataframe_util
        )

        result_df = builder.build(**inputs)

        assert result_df.count() == 0

    def test__build__gene_epression_id_valid_uuid5(self) -> None:
        config = self._arrange_config()
        spark_session = mock.MagicMock()
        doc_dataframe_util = self._arrange_doc_dataframe_util()
        gene_model = models.GeneModel()
        primary_aliquot = models.PrimaryAliquot()
        inputs = self._arrange_inputs((gene_model,), (primary_aliquot,))
        builder = expression_value.ExpressionValueBuilder(
            config, spark_session, doc_dataframe_util
        )

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.gene_expression_id == utils.generate_uuid5(
            primary_aliquot.case_id, gene_model._gene_id
        )

    def test__build__log2_uqfpkm_generated(self) -> None:
        config = self._arrange_config()
        spark_session = mock.MagicMock()
        star_count = models.STARCounts()
        doc_dataframe_util = self._arrange_doc_dataframe_util((star_count,))
        inputs = self._arrange_inputs()
        builder = expression_value.ExpressionValueBuilder(
            config, spark_session, doc_dataframe_util
        )

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        utils.assert_float_equal(
            result_row.log2_uqfpkm, math.log2(star_count.fpkm_uq_unstranded + 1)
        )
