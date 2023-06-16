import dataclasses
from collections.abc import Iterable, Set
from unittest import mock

import more_itertools
import pyspark
import pytest
from pyspark import sql
from pyspark.sql import types
from typing_extensions import TypedDict

import config
from exports import builders, es_utils
from tests.unit import utils
from tests.unit.builders.case_centric.inputs import case, cnv, sample, ssm
from tests.unit.data import schemas
from tests.unit.data.models import viz as models


class Inputs(TypedDict):
    maf_metadata_df: sql.DataFrame
    maf_df: sql.DataFrame
    ascat_df: sql.DataFrame
    primary_aliquot_df: sql.DataFrame


def assert_ascat_translated(result_gene: sql.Row, ascat: models.ASCAT) -> None:
    result_cnv = more_itertools.one(result_gene.cnv)

    assert result_gene.gene_id == ascat.gene_id
    assert result_gene.biotype == ascat.biotype
    assert result_gene.symbol == ascat.symbol
    assert result_gene.is_cancer_gene_census == ascat.is_cancer_gene_census
    assert result_cnv.cnv_id == ascat.cnv_id
    assert result_cnv.chromosome == ascat.chromosome
    assert result_cnv.cnv_change == ascat.cnv_change
    assert result_cnv.end_position == ascat.end_position
    assert result_cnv.gene_level_cn == ascat.gene_level_cn
    assert result_cnv.ncbi_build == ascat.ncbi_build
    assert result_cnv.start_position == ascat.start_position


def assert_maf_translated(result_gene: sql.Row, maf: models.MAF) -> None:
    result_ssm = more_itertools.one(result_gene.ssm)
    result_civic = result_ssm.clinical_annotations.civic

    assert result_gene.gene_id == maf.gene_id
    assert result_gene.biotype == maf.biotype
    assert result_gene.symbol == maf.symbol
    assert result_gene.is_cancer_gene_census == maf.is_cancer_gene_census
    assert result_ssm.chromosome == maf.chromosome
    assert tuple(result_ssm.cosmic_id) == maf.cosmic_id
    assert result_ssm.end_position == maf.end_position
    assert result_ssm.genomic_dna_change == maf.genomic_dna_change
    assert result_ssm.mutation_subtype == maf.mutation_subtype
    assert result_ssm.mutation_type == maf.mutation_type
    assert result_ssm.ncbi_build == maf.ncbi_build
    assert result_ssm.reference_allele == maf.reference_allele
    assert result_ssm.start_position == maf.start_position
    assert result_ssm.tumor_allele == maf.tumor_allele
    assert result_civic.gene_id == maf.civic_gene_id
    assert result_civic.variant_id == maf.civic_variant_id


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
    return schemas.Viz.Builders.Consequence.SSM.WithoutGene.FINAL.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.Viz.Builders.CaseCentric.FINAL.load()


class TestCaseCentricBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        spark_session: sql.SparkSession,
        create_dataframe: utils.CreateDataFrame,
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
        self.create_dataframe = create_dataframe
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
        self, cases: Iterable[case.Case] = (case.Case(),)
    ) -> es_utils.DataFrameUtil:
        case_df = self.create_dataframe(cases, self.case_schema)
        dataframe_util = mock.MagicMock(spec=es_utils.DataFrameUtil)
        dataframe_util.read.return_value = case_df

        return dataframe_util

    def arrange_rdd_util(
        self, cases: Iterable[sample.Hit] = (sample.Hit(),)
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
        ssm_observations: Iterable[ssm.Observations] = (ssm.Observations(),),
        cnv_observations: Iterable[cnv.Observations] = (cnv.Observations(),),
    ) -> builders.ObservationBuilder:
        ssm_observation_df = self.create_dataframe(
            ssm_observations, self.ssm_observation_schema
        )
        build_for_ssm = mock.MagicMock(return_value=ssm_observation_df)
        cnv_observation_df = self.create_dataframe(
            cnv_observations, self.cnv_observation_schema
        )
        build_for_cnv = mock.MagicMock(return_value=cnv_observation_df)

        return mock.MagicMock(
            spec=builders.ObservationBuilder,
            build_for_ssm=build_for_ssm,
            build_for_cnv=build_for_cnv,
        )

    def arrange_consequence_builder(
        self, consequences: Iterable[ssm.Consequences] = (ssm.Consequences(),)
    ) -> builders.ConsequenceBuilder:
        consequence_df = self.create_dataframe(
            consequences, self.ssm_consequence_schema
        )
        build_for_ssm = mock.MagicMock(return_value=consequence_df)

        return mock.MagicMock(
            spec=builders.ConsequenceBuilder, build_for_ssm=build_for_ssm
        )

    def arrange_inputs(
        self,
        maf_metadata: Iterable[models.MAFMetadata] = (models.MAFMetadata(),),
        mafs: Iterable[models.MAF] = (models.MAF(),),
        ascats: Iterable[models.ASCAT] = (models.ASCAT(),),
        primary_aliquots: Iterable[models.PrimaryAliquot] = (models.PrimaryAliquot(),),
    ) -> Inputs:
        maf_metadata_df = self.create_dataframe(maf_metadata, self.maf_metadata_schema)
        maf_df = self.create_dataframe(mafs, self.maf_schema)
        ascat_df = self.create_dataframe(ascats, self.ascat_schema)
        primary_aliquot_df = self.create_dataframe(
            primary_aliquots, self.primary_aliquot_schema
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
        es_sample = more_itertools.one(es_hit._source and es_hit._source.samples or ())
        raw_maf = models.MAF(gene_id="MAFGENE")
        raw_ascat = models.ASCAT(gene_id="ASCATGENE")
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

        es_case.assert_equals(result_case)
        es_sample.assert_equals(more_itertools.one(result_case.samples))
        assert_maf_translated(maf_gene, raw_maf)
        ssm.assert_consequences_translated(maf_gene, ssm_consequence)
        ssm.assert_observation_translated(maf_gene, ssm_observation)
        assert_ascat_translated(ascat_gene, raw_ascat)
        cnv.assert_observation_translated(ascat_gene, cnv_observation)

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
        maf_metadata = models.MAFMetadata(case_id=maf_case_id)
        raw_ascat = models.ASCAT(case_id=cnv_case_id)

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
        raw_maf = models.MAF(
            gene_id=gene_id,
            biotype=biotype,
            symbol=symbol,
            is_cancer_gene_census=is_cancer_gene_census,
        )
        raw_ascat = models.ASCAT(
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
        raw_maf = models.MAF(case_id="case-1")
        raw_ascat = models.ASCAT(case_id="case-1")

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
        raw_maf = models.MAF(case_id="case-0")
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
        raw_ascat = models.ASCAT(case_id="case-0")
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
        raw_maf = models.MAF(
            gene_id="g-0",
            biotype="b-0",
            symbol="s-0",
            is_cancer_gene_census="true",
            case_id="case-0",
        )
        ssm_observation = ssm.Observations(case_id="case-0")
        raw_ascat = models.ASCAT(
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
        raw_maf0 = models.MAF(gene_id="g-0", case_id="case-0", ssm_id="ssm-0")
        raw_maf1 = models.MAF(gene_id="g-0", case_id="case-0", ssm_id="ssm-1")
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
        raw_ascat0 = models.ASCAT(cnv_id="cnv-0")
        raw_ascat1 = models.ASCAT(cnv_id="cnv-1")
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
