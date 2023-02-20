import dataclasses
from typing import Set, Tuple
from unittest import mock

import more_itertools
import pyspark
import pytest
from pyspark import sql
from pyspark.sql import types
from typing_extensions import TypedDict

import config
from exports import builders, es_utils
from tests.unit.builders.case_centric.inputs import ascat, case, cnv, maf, sample, ssm
from tests.unit.data import schemas


@dataclasses.dataclass(frozen=True)
class PrimaryAliquot:
    aliquot_id: str = "aliquot-0"
    case_id: str = "case-0"
    entity: str = "case"
    entity_id: str = "case-0"
    experimental_strategy: str = "WXS"
    file_id: str = "file-0"


class Inputs(TypedDict):
    maf_metadata_df: sql.DataFrame
    maf_df: sql.DataFrame
    ascat_df: sql.DataFrame
    primary_aliquot_df: sql.DataFrame


@pytest.fixture(scope="class")
def maf_metadata_schema() -> types.StructType:
    return schemas.Viz.Builders.MAFMetadata.FINAL.load()


@pytest.fixture(scope="class")
def maf_schema() -> types.StructType:
    return schemas.Viz.Builders.MAF.FINAL.load()


@pytest.fixture(scope="class")
def ascat_schema() -> types.StructType:
    return schemas.Viz.Builders.ASCAT.FINAL.load()


@pytest.fixture(scope="class")
def primary_aliquot_schema() -> types.StructType:
    return schemas.Viz.Builders.PrimaryAliquot.FINAL.load()


@pytest.fixture(scope="class")
def case_schema() -> types.StructType:
    return schemas.Viz.Builders.CaseCentric.CASE.load()


@pytest.fixture(scope="class")
def ssm_observation_schema() -> types.StructType:
    return schemas.Viz.Builders.Observation.Other.FINAL.load()


@pytest.fixture(scope="class")
def cnv_observation_schema() -> types.StructType:
    return schemas.Viz.Builders.Observation.CNV.FINAL.load()


@pytest.fixture(scope="class")
def ssm_consequence_schema() -> types.StructType:
    return schemas.Viz.Builders.Consequence.FINAL.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.Viz.Builders.CaseCentric.FINAL.load()


class TestCaseCentricBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        spark_session: sql.SparkSession,
        maf_metadata_schema: types.StructType,
        maf_schema: types.StructType,
        ascat_schema: types.StructType,
        primary_aliquot_schema: types.StructType,
        case_schema: types.StructType,
        ssm_observation_schema: types.StructType,
        cnv_observation_schema: types.StructType,
        ssm_consequence_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.maf_metadata_schema = maf_metadata_schema
        self.maf_schema = maf_schema
        self.ascat_schema = ascat_schema
        self.primary_aliquot_schema = primary_aliquot_schema
        self.case_schema = case_schema
        self.ssm_observation_schema = ssm_observation_schema
        self.cnv_observation_schema = cnv_observation_schema
        self.ssm_consequence_schema = ssm_consequence_schema
        self.final_schema = final_schema

    def arrange_config(self) -> config.BaseConfig:
        return mock.MagicMock(
            spec=config.BaseConfig,
            debug=False,
            projects=(),
            output_raw="neither",
            df_repartition=1,
            percentile_threshold={"genes_per_case": 100},
        )

    def arrange_sql_context(self) -> sql.SQLContext:
        sql_context = mock.MagicMock(spec=sql.SQLContext)

        return sql_context

    def arrange_dataframe_util(
        self, cases: Tuple[case.Case, ...] = (case.Case(),)
    ) -> es_utils.DataFrameUtil:
        case_df = self.spark_session.createDataFrame(cases, schema=self.case_schema)
        dataframe_util = mock.MagicMock(spec=es_utils.DataFrameUtil)
        dataframe_util.get_dataframe.return_value = case_df

        return dataframe_util

    def arrange_rdd_util(
        self, cases: Tuple[sample.Hit, ...] = (sample.Hit(),)
    ) -> es_utils.RDDUtil:
        context: pyspark.SparkContext = self.spark_session.sparkContext
        rdd = context.parallelize(
            map(
                lambda c: (c._id, dataclasses.asdict(c._source)),
                cases,
            )
        )
        rdd_util = mock.MagicMock(spec=es_utils.RDDUtil)
        rdd_util.get_rdd.return_value = rdd

        return rdd_util

    def arrange_field_selector(self) -> es_utils.CaseFieldSelector:
        return mock.MagicMock(
            spec=es_utils.CaseFieldSelector, select_for=mock.MagicMock(return_value=())
        )

    def arrange_observation_builder(
        self,
        ssm_observations: Tuple[ssm.Observations, ...] = (ssm.Observations(),),
        cnv_observations: Tuple[cnv.Observations, ...] = (cnv.Observations(),),
    ) -> builders.ObservationBuilder:
        ssm_observation_df = self.spark_session.createDataFrame(
            ssm_observations, self.ssm_observation_schema
        )
        build_for_ssm = mock.MagicMock(return_value=ssm_observation_df)
        cnv_observation_df = self.spark_session.createDataFrame(
            cnv_observations, self.cnv_observation_schema
        )
        build_for_cnv = mock.MagicMock(return_value=cnv_observation_df)

        return mock.MagicMock(
            spec=builders.ObservationBuilder,
            build_for_ssm=build_for_ssm,
            build_for_cnv=build_for_cnv,
        )

    def arrange_consequence_builder(
        self, consequences: Tuple[ssm.Consequences, ...] = (ssm.Consequences(),)
    ) -> builders.ConsequenceBuilder:
        consequence_df = self.spark_session.createDataFrame(
            consequences, schema=self.ssm_consequence_schema
        )
        build_for_ssm = mock.MagicMock(return_value=consequence_df)

        return mock.MagicMock(
            spec=builders.ConsequenceBuilder, build_for_ssm=build_for_ssm
        )

    def arrange_inputs(
        self,
        maf_metadata: Tuple[maf.Metadata, ...] = (maf.Metadata(),),
        mafs: Tuple[maf.MAF, ...] = (maf.MAF(),),
        ascats: Tuple[ascat.ASCAT, ...] = (ascat.ASCAT(),),
        primary_aliquots: Tuple[PrimaryAliquot, ...] = (PrimaryAliquot(),),
    ) -> Inputs:
        maf_metadata_df = self.spark_session.createDataFrame(
            maf_metadata, schema=self.maf_metadata_schema
        )
        maf_df = self.spark_session.createDataFrame(mafs, schema=self.maf_schema)
        ascat_df = self.spark_session.createDataFrame(ascats, schema=self.ascat_schema)
        primary_aliquot_df = self.spark_session.createDataFrame(
            primary_aliquots, schema=self.primary_aliquot_schema
        )

        return Inputs(
            maf_metadata_df=maf_metadata_df,
            maf_df=maf_df,
            ascat_df=ascat_df,
            primary_aliquot_df=primary_aliquot_df,
        )

    def test__build__single_row(self) -> None:
        config = self.arrange_config()
        sql_context = self.arrange_sql_context()
        dataframe_util = self.arrange_dataframe_util()
        rdd_util = self.arrange_rdd_util()
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder()
        inputs = self.arrange_inputs()
        builder = builders.CaseCentricBuilder(
            config,
            sql_context,
            dataframe_util,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        builder.build(**inputs)

        assert hasattr(builder, "case_centric") and isinstance(
            builder.case_centric, sql.DataFrame
        )
        assert builder.case_centric.count() == 1
        assert builder.case_centric.schema == self.final_schema

    def test__build__data_translated(self) -> None:
        es_case = case.Case()
        es_hit = sample.Hit()
        raw_maf = maf.MAF(gene_id="MAFGENE")
        raw_ascat = ascat.ASCAT(gene_id="ASCATGENE")
        ssm_consequence = ssm.Consequences()
        ssm_observation = ssm.Observations()
        cnv_observation = cnv.Observations()

        config = self.arrange_config()
        sql_context = self.arrange_sql_context()
        dataframe_util = self.arrange_dataframe_util((es_case,))
        rdd_util = self.arrange_rdd_util((es_hit,))
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder((ssm_consequence,))
        observation_builder = self.arrange_observation_builder(
            (ssm_observation,), (cnv_observation,)
        )
        inputs = self.arrange_inputs(mafs=(raw_maf,), ascats=(raw_ascat,))
        builder = builders.CaseCentricBuilder(
            config,
            sql_context,
            dataframe_util,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        builder.build(**inputs)

        result_case = more_itertools.one(builder.case_centric.collect())
        maf_gene = more_itertools.one(
            g for g in result_case.gene if g.gene_id == "MAFGENE"
        )
        ascat_gene = more_itertools.one(
            g for g in result_case.gene if g.gene_id == "ASCATGENE"
        )

        case.assert_case_translated(result_case, es_case)
        sample.assert_hit_translated(result_case, es_hit)
        maf.assert_maf_translated(maf_gene, raw_maf)
        ssm.assert_consequences_translated(maf_gene, ssm_consequence)
        ssm.assert_observation_transated(maf_gene, ssm_observation)
        ascat.assert_ascat_transated(ascat_gene, raw_ascat)
        cnv.assert_observation_transated(ascat_gene, cnv_observation)

    @pytest.mark.parametrize(
        ("maf_case_id", "cnv_case_id", "expected_available_variations"),
        (
            ("case-1", "case-1", frozenset({})),
            ("case-0", "case-4", frozenset({"ssm"})),
            ("case-6", "case-0", frozenset({"cnv"})),
            ("case-0", "case-0", frozenset({"ssm", "cnv"})),
        ),
        ids=("no_data", "ssm_only", "cnv_only", "both"),
    )
    def test__build__available_variation_data(
        self,
        maf_case_id: str,
        cnv_case_id: str,
        expected_available_variations: Set[str],
    ) -> None:
        maf_metadata = maf.Metadata(case_id=maf_case_id)
        raw_ascat = ascat.ASCAT(case_id=cnv_case_id)

        config = self.arrange_config()
        sql_context = self.arrange_sql_context()
        dataframe_util = self.arrange_dataframe_util()
        rdd_util = self.arrange_rdd_util()
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder()
        inputs = self.arrange_inputs(maf_metadata=(maf_metadata,), ascats=(raw_ascat,))
        builder = builders.CaseCentricBuilder(
            config,
            sql_context,
            dataframe_util,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        builder.build(**inputs)

        result_case = more_itertools.one(builder.case_centric.collect())
        result_available_variations = frozenset(result_case.available_variation_data)

        assert result_available_variations == expected_available_variations

    @pytest.mark.parametrize(
        ("gene_id", "biotype", "symbol", "is_cancer_gene_census", "expected_count"),
        (
            ("gene-1", "b-0", "sym-0", "true", 2),
            ("gene-0", "b-1", "sym-0", "true", 2),
            ("gene-0", "b-0", "sym-1", "true", 2),
            ("gene-0", "b-0", "sym-0", "false", 2),
            ("gene-0", "b-0", "sym-0", "true", 1),
        ),
        ids=(
            "distinct_id",
            "distinct_biotype",
            "distinct_symbol",
            "differing_cancer_gene_census",
            "same_gene",
        ),
    )
    def test__build__distinct_genes(
        self,
        gene_id: str,
        biotype: str,
        symbol: str,
        is_cancer_gene_census: str,
        expected_count: int,
    ) -> None:
        raw_maf = maf.MAF(
            gene_id=gene_id,
            biotype=biotype,
            symbol=symbol,
            is_cancer_gene_census=is_cancer_gene_census,
        )
        raw_ascat = ascat.ASCAT(
            gene_id="gene-0",
            biotype="b-0",
            symbol="sym-0",
            is_cancer_gene_census="true",
        )

        config = self.arrange_config()
        sql_context = self.arrange_sql_context()
        dataframe_util = self.arrange_dataframe_util()
        rdd_util = self.arrange_rdd_util()
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder()
        inputs = self.arrange_inputs(mafs=(raw_maf,), ascats=(raw_ascat,))
        builder = builders.CaseCentricBuilder(
            config,
            sql_context,
            dataframe_util,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        builder.build(**inputs)

        result_case = more_itertools.one(builder.case_centric.collect())

        assert len(result_case.gene) == expected_count

    def test__build__gene_joins_neither_ssm_nor_cnv(self) -> None:
        raw_maf = maf.MAF(case_id="case-1")
        raw_ascat = ascat.ASCAT(case_id="case-1")

        config = self.arrange_config()
        sql_context = self.arrange_sql_context()
        dataframe_util = self.arrange_dataframe_util()
        rdd_util = self.arrange_rdd_util()
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder()
        inputs = self.arrange_inputs(mafs=(raw_maf,), ascats=(raw_ascat,))
        builder = builders.CaseCentricBuilder(
            config,
            sql_context,
            dataframe_util,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        builder.build(**inputs)

        result_case = more_itertools.one(builder.case_centric.collect())

        assert result_case.gene is None

    def test__build__gene_joins_ssm_only(self) -> None:
        raw_maf = maf.MAF(case_id="case-0")
        ssm_observation = ssm.Observations(case_id="case-0")

        config = self.arrange_config()
        sql_context = self.arrange_sql_context()
        dataframe_util = self.arrange_dataframe_util()
        rdd_util = self.arrange_rdd_util()
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder(
            ssm_observations=(ssm_observation,)
        )
        inputs = self.arrange_inputs(mafs=(raw_maf,), ascats=())
        builder = builders.CaseCentricBuilder(
            config,
            sql_context,
            dataframe_util,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        builder.build(**inputs)

        result_case = more_itertools.one(builder.case_centric.collect())

        assert len(result_case.gene) == 1
        assert len(result_case.gene[0].ssm) == 1
        assert result_case.gene[0].cnv is None

    def test__build__gene_joins_cnv_only(self) -> None:
        raw_ascat = ascat.ASCAT(case_id="case-0")
        cnv_observation = cnv.Observations(case_id="case-0")

        config = self.arrange_config()
        sql_context = self.arrange_sql_context()
        dataframe_util = self.arrange_dataframe_util()
        rdd_util = self.arrange_rdd_util()
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder(
            cnv_observations=(cnv_observation,)
        )
        inputs = self.arrange_inputs(mafs=(), ascats=(raw_ascat,))
        builder = builders.CaseCentricBuilder(
            config,
            sql_context,
            dataframe_util,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        builder.build(**inputs)

        result_case = more_itertools.one(builder.case_centric.collect())

        assert len(result_case.gene) == 1
        assert result_case.gene[0].ssm is None
        assert len(result_case.gene[0].cnv) == 1

    def test__build__gene_joins_both_ssm_and_cnv(self) -> None:
        raw_maf = maf.MAF(
            gene_id="g-0",
            biotype="b-0",
            symbol="s-0",
            is_cancer_gene_census="true",
            case_id="case-0",
        )
        ssm_observation = ssm.Observations(case_id="case-0")
        raw_ascat = ascat.ASCAT(
            gene_id="g-0",
            biotype="b-0",
            symbol="s-0",
            is_cancer_gene_census="true",
            case_id="case-0",
        )
        cnv_observation = cnv.Observations(case_id="case-0")

        config = self.arrange_config()
        sql_context = self.arrange_sql_context()
        dataframe_util = self.arrange_dataframe_util()
        rdd_util = self.arrange_rdd_util()
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder(
            ssm_observations=(ssm_observation,), cnv_observations=(cnv_observation,)
        )
        inputs = self.arrange_inputs(mafs=(raw_maf,), ascats=(raw_ascat,))
        builder = builders.CaseCentricBuilder(
            config,
            sql_context,
            dataframe_util,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        builder.build(**inputs)

        result_case = more_itertools.one(builder.case_centric.collect())

        assert len(result_case.gene) == 1
        assert len(result_case.gene[0].ssm) == 1
        assert len(result_case.gene[0].cnv) == 1

    def test__build__ssm_group_by_gene_and_case_id(self) -> None:
        raw_maf0 = maf.MAF(gene_id="g-0", case_id="case-0", ssm_id="ssm-0")
        raw_maf1 = maf.MAF(gene_id="g-0", case_id="case-0", ssm_id="ssm-1")
        ssm_observation0 = ssm.Observations(case_id="case-0", ssm_id="ssm-0")
        ssm_observation1 = ssm.Observations(case_id="case-0", ssm_id="ssm-1")

        config = self.arrange_config()
        sql_context = self.arrange_sql_context()
        dataframe_util = self.arrange_dataframe_util()
        rdd_util = self.arrange_rdd_util()
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder(
            ssm_observations=(ssm_observation0, ssm_observation1)
        )
        inputs = self.arrange_inputs(mafs=(raw_maf0, raw_maf1), ascats=())
        builder = builders.CaseCentricBuilder(
            config,
            sql_context,
            dataframe_util,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        builder.build(**inputs)

        result_case = more_itertools.one(builder.case_centric.collect())

        assert len(result_case.gene) == 1
        assert len(result_case.gene[0].ssm) == 2

    def test__build__cnv_group_by_gene_and_case_id(self) -> None:
        raw_ascat0 = ascat.ASCAT(cnv_id="cnv-0")
        raw_ascat1 = ascat.ASCAT(cnv_id="cnv-1")
        cnv_observation0 = cnv.Observations(cnv_id="cnv-0")
        cnv_observation1 = cnv.Observations(cnv_id="cnv-1")

        config = self.arrange_config()
        sql_context = self.arrange_sql_context()
        dataframe_util = self.arrange_dataframe_util()
        rdd_util = self.arrange_rdd_util()
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder(
            cnv_observations=(cnv_observation0, cnv_observation1)
        )
        inputs = self.arrange_inputs(mafs=(), ascats=(raw_ascat0, raw_ascat1))
        builder = builders.CaseCentricBuilder(
            config,
            sql_context,
            dataframe_util,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        builder.build(**inputs)

        result_case = more_itertools.one(builder.case_centric.collect())

        assert len(result_case.gene) == 1
        assert len(result_case.gene[0].cnv) == 2
