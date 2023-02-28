import dataclasses
from typing import Any, Callable, Iterable, Set, Tuple, TypedDict
from unittest import mock

import more_itertools
import pyspark
import pytest
from pyspark import sql
from pyspark.sql import types

from exports import builders, es_utils
from exports.configuration.builders import viz
from exports.constants import build
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
        create_dataframe: Callable[[Iterable[Any], types.StructType], sql.DataFrame],
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

    def arrange_config(self) -> viz.CaseCentricBuilder:
        return mock.MagicMock(
            partition_size=1,
            id_field="case_id",
            genes_threshold=100,
            include_as_arrays=(),
            is_cashed=False,
            backup=mock.MagicMock(mode=build.BackupMode.NEITHER, path=""),
            projects=(),
        )

    def arrange_spark_session(self) -> sql.SparkSession:
        spark_session = mock.MagicMock(spec=sql.SparkSession)

        return spark_session

    def arrange_mappings_loader(self) -> es_utils.MappingsLoader:
        loader = mock.MagicMock(spec=es_utils.MappingsLoader)
        loader.load_mappings.return_value = {}

        return loader

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
        self, consequences: Tuple[ssm.Consequences, ...] = (ssm.Consequences(),)
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
        maf_metadata: Iterable[maf.Metadata] = (maf.Metadata(),),
        mafs: Iterable[maf.MAF] = (maf.MAF(),),
        ascats: Iterable[ascat.ASCAT] = (ascat.ASCAT(),),
        primary_aliquots: Iterable[PrimaryAliquot] = (PrimaryAliquot(),),
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
        spark_session = self.arrange_spark_session()
        dataframe_util = self.arrange_dataframe_util()
        rdd_util = self.arrange_rdd_util()
        mappings_loader = self.arrange_mappings_loader()
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder()
        inputs = self.arrange_inputs()
        builder = builders.CaseCentricBuilder(
            config,
            spark_session,
            dataframe_util,
            mappings_loader,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        result_df = result_df = builder.build(**inputs)

        assert result_df.count() == 1
        assert result_df.schema == self.final_schema

    def test__build__data_translated(self) -> None:
        es_case = case.Case()
        es_hit = sample.Hit()
        raw_maf = maf.MAF(gene_id="MAFGENE")
        raw_ascat = ascat.ASCAT(gene_id="ASCATGENE")
        ssm_consequence = ssm.Consequences()
        ssm_observation = ssm.Observations()
        cnv_observation = cnv.Observations()

        config = self.arrange_config()
        spark_session = self.arrange_spark_session()
        dataframe_util = self.arrange_dataframe_util((es_case,))
        mappings_loader = self.arrange_mappings_loader()
        rdd_util = self.arrange_rdd_util((es_hit,))
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder((ssm_consequence,))
        observation_builder = self.arrange_observation_builder(
            (ssm_observation,), (cnv_observation,)
        )
        inputs = self.arrange_inputs(mafs=(raw_maf,), ascats=(raw_ascat,))
        builder = builders.CaseCentricBuilder(
            config,
            spark_session,
            dataframe_util,
            mappings_loader,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        result_df = builder.build(**inputs)

        result_case = more_itertools.one(result_df.collect())
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
        spark_session = self.arrange_spark_session()
        dataframe_util = self.arrange_dataframe_util()
        mappings_loader = self.arrange_mappings_loader()
        rdd_util = self.arrange_rdd_util()
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder()
        inputs = self.arrange_inputs(maf_metadata=(maf_metadata,), ascats=(raw_ascat,))
        builder = builders.CaseCentricBuilder(
            config,
            spark_session,
            dataframe_util,
            mappings_loader,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        result_df = builder.build(**inputs)

        result_case = more_itertools.one(result_df.collect())
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
        spark_session = self.arrange_spark_session()
        dataframe_util = self.arrange_dataframe_util()
        mappings_loader = self.arrange_mappings_loader()
        rdd_util = self.arrange_rdd_util()
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder()
        inputs = self.arrange_inputs(mafs=(raw_maf,), ascats=(raw_ascat,))
        builder = builders.CaseCentricBuilder(
            config,
            spark_session,
            dataframe_util,
            mappings_loader,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        result_df = builder.build(**inputs)

        result_case = more_itertools.one(result_df.collect())

        assert len(result_case.gene) == expected_count

    def test__build__gene_joins_neither_ssm_nor_cnv(self) -> None:
        raw_maf = maf.MAF(case_id="case-1")
        raw_ascat = ascat.ASCAT(case_id="case-1")

        config = self.arrange_config()
        spark_session = self.arrange_spark_session()
        dataframe_util = self.arrange_dataframe_util()
        mappings_loader = self.arrange_mappings_loader()
        rdd_util = self.arrange_rdd_util()
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder()
        inputs = self.arrange_inputs(mafs=(raw_maf,), ascats=(raw_ascat,))
        builder = builders.CaseCentricBuilder(
            config,
            spark_session,
            dataframe_util,
            mappings_loader,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        result_df = builder.build(**inputs)

        result_case = more_itertools.one(result_df.collect())

        assert result_case.gene is None

    def test__build__gene_joins_ssm_only(self) -> None:
        raw_maf = maf.MAF(case_id="case-0")
        ssm_observation = ssm.Observations(case_id="case-0")

        config = self.arrange_config()
        spark_session = self.arrange_spark_session()
        dataframe_util = self.arrange_dataframe_util()
        mappings_loader = self.arrange_mappings_loader()
        rdd_util = self.arrange_rdd_util()
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder(
            ssm_observations=(ssm_observation,)
        )
        inputs = self.arrange_inputs(mafs=(raw_maf,), ascats=())
        builder = builders.CaseCentricBuilder(
            config,
            spark_session,
            dataframe_util,
            mappings_loader,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        result_df = builder.build(**inputs)

        result_case = more_itertools.one(result_df.collect())

        assert len(result_case.gene) == 1
        assert len(result_case.gene[0].ssm) == 1
        assert result_case.gene[0].cnv is None

    def test__build__gene_joins_cnv_only(self) -> None:
        raw_ascat = ascat.ASCAT(case_id="case-0")
        cnv_observation = cnv.Observations(case_id="case-0")

        config = self.arrange_config()
        spark_session = self.arrange_spark_session()
        dataframe_util = self.arrange_dataframe_util()
        mappings_loader = self.arrange_mappings_loader()
        rdd_util = self.arrange_rdd_util()
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder(
            cnv_observations=(cnv_observation,)
        )
        inputs = self.arrange_inputs(mafs=(), ascats=(raw_ascat,))
        builder = builders.CaseCentricBuilder(
            config,
            spark_session,
            dataframe_util,
            mappings_loader,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        result_df = builder.build(**inputs)

        result_case = more_itertools.one(result_df.collect())

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
        spark_session = self.arrange_spark_session()
        dataframe_util = self.arrange_dataframe_util()
        mappings_loader = self.arrange_mappings_loader()
        rdd_util = self.arrange_rdd_util()
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder(
            ssm_observations=(ssm_observation,), cnv_observations=(cnv_observation,)
        )
        inputs = self.arrange_inputs(mafs=(raw_maf,), ascats=(raw_ascat,))
        builder = builders.CaseCentricBuilder(
            config,
            spark_session,
            dataframe_util,
            mappings_loader,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        result_df = builder.build(**inputs)

        result_case = more_itertools.one(result_df.collect())

        assert len(result_case.gene) == 1
        assert len(result_case.gene[0].ssm) == 1
        assert len(result_case.gene[0].cnv) == 1

    def test__build__ssm_group_by_gene_and_case_id(self) -> None:
        raw_maf0 = maf.MAF(gene_id="g-0", case_id="case-0", ssm_id="ssm-0")
        raw_maf1 = maf.MAF(gene_id="g-0", case_id="case-0", ssm_id="ssm-1")
        ssm_observation0 = ssm.Observations(case_id="case-0", ssm_id="ssm-0")
        ssm_observation1 = ssm.Observations(case_id="case-0", ssm_id="ssm-1")

        config = self.arrange_config()
        spark_session = self.arrange_spark_session()
        dataframe_util = self.arrange_dataframe_util()
        mappings_loader = self.arrange_mappings_loader()
        rdd_util = self.arrange_rdd_util()
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder(
            ssm_observations=(ssm_observation0, ssm_observation1)
        )
        inputs = self.arrange_inputs(mafs=(raw_maf0, raw_maf1), ascats=())
        builder = builders.CaseCentricBuilder(
            config,
            spark_session,
            dataframe_util,
            mappings_loader,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        result_df = builder.build(**inputs)

        result_case = more_itertools.one(result_df.collect())

        assert len(result_case.gene) == 1
        assert len(result_case.gene[0].ssm) == 2

    def test__build__cnv_group_by_gene_and_case_id(self) -> None:
        raw_ascat0 = ascat.ASCAT(cnv_id="cnv-0")
        raw_ascat1 = ascat.ASCAT(cnv_id="cnv-1")
        cnv_observation0 = cnv.Observations(cnv_id="cnv-0")
        cnv_observation1 = cnv.Observations(cnv_id="cnv-1")

        config = self.arrange_config()
        spark_session = self.arrange_spark_session()
        dataframe_util = self.arrange_dataframe_util()
        mappings_loader = self.arrange_mappings_loader()
        rdd_util = self.arrange_rdd_util()
        field_selector = self.arrange_field_selector()
        consequence_builder = self.arrange_consequence_builder()
        observation_builder = self.arrange_observation_builder(
            cnv_observations=(cnv_observation0, cnv_observation1)
        )
        inputs = self.arrange_inputs(mafs=(), ascats=(raw_ascat0, raw_ascat1))
        builder = builders.CaseCentricBuilder(
            config,
            spark_session,
            dataframe_util,
            mappings_loader,
            rdd_util,
            field_selector,
            consequence_builder,
            observation_builder,
        )

        result_df = builder.build(**inputs)

        result_case = more_itertools.one(result_df.collect())

        assert len(result_case.gene) == 1
        assert len(result_case.gene[0].cnv) == 2
