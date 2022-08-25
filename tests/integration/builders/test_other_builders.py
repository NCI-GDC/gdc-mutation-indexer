import json

import pytest

import deepdiff
from tests.integration import config
from exports import builders, es_utils
from pyspark import sql
from pyspark.sql import functions as F

conf = config.TestConfig()


@pytest.mark.usefixtures("maf_df", "cnv_df")
class TestOtherBase:
    """
    Code to reuse throughout all tests in this file
    """

    @pytest.fixture(scope="function")
    def get_inputs(self, maf_df, cnv_df, request):
        """
        Returns (build_function, input_df, id_field) according to build_type
        NOTE: build_type is inferred from argument of the test function where
        this fixture is used

        """
        build_type = request.getfixturevalue("build_type")
        id_field = "{}_id".format(build_type)
        build_function = "build_for_{}".format(build_type)
        if build_type == "ssm":
            input_df = maf_df
        else:
            input_df = cnv_df
        return build_function, input_df, id_field

    @classmethod
    def params(cls):
        """Get params for all observation test cases.

        All tests are parametrized by 'index_name,build_type'.
        """
        return cls.ssm_params() + cls.cnv_params()

    @classmethod
    def ssm_params(cls):
        """Get params for SSM observation test cases."""
        return [(index_name, "ssm") for index_name in conf.ssm_indices]

    @classmethod
    def cnv_params(cls):
        """Get params for CNV observation test cases."""
        return [(index_name, "cnv") for index_name in conf.cnv_indices]


@pytest.mark.usefixtures("maf_df", "cnv_df")
class TestObservationBuilder:
    """Test intermediate result from the observation builder"""

    @pytest.fixture(scope="class")
    def builder(self):
        yield builders.ObservationBuilder()

    @pytest.mark.parametrize(
        ("index_name",), (("ssm_centric",), ("ssm_occurrence_centric",))
    )
    def test__build_for_ssm__join_columns(
        self,
        index_name: str,
        builder: builders.ObservationBuilder,
        maf_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
    ):
        result_df = builder.build_for_ssm(maf_df, primary_aliquot_df, index_name)

        assert set(result_df.columns) == {
            "case_id",
            "ssm_id",
            "observation",
            "occurrence_id",
        }

    @pytest.mark.parametrize(
        ("index_name",), (("cnv_centric",), ("cnv_occurrence_centric",))
    )
    def test__build_for_cnv__join_columns(
        self,
        index_name: str,
        builder: builders.ObservationBuilder,
        cnv_df: sql.DataFrame,
    ):
        result_df = builder.build_for_cnv(cnv_df, index_name)

        assert set(result_df.columns) == {
            "case_id",
            "cnv_id",
            "observation",
            "occurrence_id",
        }

    @pytest.mark.parametrize(
        ("index_name",), (("ssm_centric",), ("ssm_occurrence_centric",))
    )
    def test__build_for_ssm__observation_id(
        self,
        index_name: str,
        builder: builders.ObservationBuilder,
        maf_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
    ):
        result_df = builder.build_for_ssm(maf_df, primary_aliquot_df, index_name)

        assert "observation_id" in (
            result_df.select(F.explode("observation").alias("observation"))
            .select("observation.*")
            .columns
        )

    @pytest.mark.parametrize(
        ("index_name",), (("cnv_centric",), ("cnv_occurrence_centric",))
    )
    def test__build_for_cnv__observation_id(
        self,
        index_name: str,
        builder: builders.ObservationBuilder,
        cnv_df: sql.DataFrame,
    ):
        result_df = builder.build_for_cnv(cnv_df, index_name)

        assert "observation_id" in (
            result_df.select(F.explode("observation").alias("observation"))
            .select("observation.*")
            .columns
        )

    @pytest.mark.parametrize(
        ("index_name",), (("ssm_centric",), ("ssm_occurrence_centric",))
    )
    def test__build_for_ssm__observation_count(
        self,
        index_name: str,
        builder: builders.ObservationBuilder,
        maf_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
    ):
        observation_count = maf_df.select("case_id", "ssm_id").distinct().count()

        result_df = builder.build_for_ssm(maf_df, primary_aliquot_df, index_name)

        assert result_df.count() == observation_count

    @pytest.mark.parametrize(
        ("index_name",), (("cnv_centric",), ("cnv_occurrence_centric",))
    )
    def test__build_for_cnv__observation_count(
        self,
        index_name: str,
        builder: builders.ObservationBuilder,
        cnv_df: sql.DataFrame,
    ):
        observation_count = cnv_df.select("case_id", "cnv_id").distinct().count()

        result_df = builder.build_for_cnv(cnv_df, index_name)

        assert result_df.count() == observation_count

    @pytest.mark.parametrize(
        ("index_name",), (("ssm_centric",), ("ssm_occurrence_centric",))
    )
    def test__build_for_ssm__observation_values(
        self,
        index_name: str,
        builder: builders.ObservationBuilder,
        maf_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
    ):
        expected_opservations = map(
            json.loads,
            (maf_df.select("case_id", "ssm_id").distinct().toJSON().collect()),
        )

        result_df = builder.build_for_ssm(maf_df, primary_aliquot_df, index_name)

        actual_observations = map(
            json.loads, (result_df.select("case_id", "ssm_id").toJSON().collect())
        )
        assert not deepdiff.DeepDiff(
            expected_opservations, actual_observations, ignore_order=True
        )

    @pytest.mark.parametrize(
        ("index_name",), (("cnv_centric",), ("cnv_occurrence_centric",))
    )
    def test__build_for_cnv__observation_values(
        self,
        index_name: str,
        builder: builders.ObservationBuilder,
        cnv_df: sql.DataFrame,
    ):
        expected_opservations = map(
            json.loads,
            (cnv_df.select("case_id", "cnv_id").distinct().toJSON().collect()),
        )

        result_df = builder.build_for_cnv(cnv_df, index_name)

        actual_observations = map(
            json.loads, (result_df.select("case_id", "cnv_id").toJSON().collect())
        )
        assert not deepdiff.DeepDiff(
            expected_opservations, actual_observations, ignore_order=True
        )

    @pytest.mark.parametrize(
        ("index_name",), (("ssm_centric",), ("ssm_occurrence_centric",))
    )
    def test__build_for_ssm__variant_caller(
        self,
        index_name: str,
        builder: builders.ObservationBuilder,
        maf_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
        exploded_variant_caller_counts: int,
    ):
        result_df = builder.build_for_ssm(maf_df, primary_aliquot_df, index_name)

        actual_counts = dict(
            result_df.select(F.explode("observation").alias("observation"))
            .groupBy("observation.variant_calling.variant_caller")
            .count()
            .collect()
        )

        assert actual_counts == exploded_variant_caller_counts


@pytest.mark.usefixtures("sqlContext", "maf_df")
class TestConsequenceBuilder(TestOtherBase):
    """Test intermediate result from the transcript builder"""

    @pytest.fixture(scope="class")
    def builder(self, sqlContext):
        yield builders.ConsequenceBuilder(conf, sqlContext)

    @pytest.mark.parametrize("index_name,build_type", TestOtherBase.params())
    def test_consequence_count(self, builder, index_name, build_type, get_inputs):
        build_function, input_df, id_field = get_inputs
        cons_df = getattr(builder, build_function)(input_df, index_name)

        n_consequences = input_df.select(id_field).distinct().count()
        assert cons_df.count() == n_consequences

    @pytest.mark.parametrize("index_name", conf.ssm_indices)
    def test_transcript_annotation_link(self, builder, maf_df, index_name):
        cons_df = builder.build_for_ssm(maf_df, index_name)

        # Explode consequences
        tran_df = cons_df.select(F.explode("consequence").alias("c")).select(
            "c.consequence_id",
            "c.transcript.transcript_id",
            "c.transcript.annotation",
            "c.transcript.consequence_type",
        )

        # Get data as json and choose only annotated transcripts
        data = tran_df.toJSON().collect()
        data = [json.loads(d) for d in data]
        data = [d for d in data if "annotation" in d]

        # According to Kyle Hernandez, consequence_type has to match impact:
        impacts = {
            "intergenic_variant": "MODIFIER",
            "upstream_gene_variant": "MODIFIER",
            "downstream_gene_variant": "MODIFIER",
            "splice_donor_variant": "HIGH",
            "splice_acceptor_variant": "HIGH",
            "splice_region_variant": "LOW",
            "intron_variant": "MODIFIER",
            "5_prime_UTR_variant": "MODIFIER",
            "3_prime_UTR_variant": "MODIFIER",
            "synonymous_variant": "LOW",
            "missense_variant": "MODERATE",
            "inframe_insertion": "MODERATE",
            "inframe_deletion": "MODERATE",
            "stop_gained": "HIGH",
            "stop_lost": "HIGH",
            "stop_retained_variant": "LOW",
            "start_lost": "HIGH",
            "frameshift_variant": "HIGH",
            "incomplete_terminal_codon_variant": "LOW",
            "NMD_transcript_variant": "MODIFIER",
            "non_coding_transcript_variant": "MODIFIER",
            "non_coding_transcript_exon_variant": "MODIFIER",
            "mature_miRNA_variant": "MODIFIER",
            "coding_sequence_variant": "MODIFIER",
            "regulatory_region_variant": "MODIFIER",
            "TF_binding_site_variant": "MODIFIER",
            "transcript_ablation": "HIGH",
            "transcript_amplification": "HIGH",
            "TFBS_ablation": "MODERATE",
            "TFBS_amplification": "MODIFIER",
            "regulatory_region_ablation": "MODERATE",
            "regulatory_region_amplification": "MODIFIER",
            "feature_elongation": "MODIFIER",
            "feature_truncation": "MODIFIER",
            "protein_altering_variant": "MODERATE",
        }

        for row in data:
            assert row["annotation"]["vep_impact"] == impacts[row["consequence_type"]]

    @pytest.mark.parametrize("index_name", conf.ssm_indices)
    def test_consequence_no_gene(self, builder, maf_df, index_name):
        cons_df = builder.build_for_ssm(maf_df, index_name)
        transcripts = cons_df.select(
            F.explode("consequence.transcript").alias("transcript")
        ).select("transcript.*")

        # Check that gene not in transctipts
        assert "gene" not in transcripts.columns

    @pytest.mark.parametrize("index_name", conf.ssm_indices)
    def test_consequence_with_gene(self, builder, maf_df, index_name):
        cons_df = builder.build_for_ssm(maf_df, index_name, join_gene=True)
        transcripts = cons_df.select(
            F.explode("consequence.transcript").alias("transcript")
        ).select("transcript.*")

        assert "symbol" in (
            cons_df.select(F.explode("consequence.transcript.gene").alias("gene"))
            .select("gene.*")
            .columns
        )

        if index_name != "case_centric":
            cytobands = transcripts.select("gene.cytoband").collect()
            cytobands = [t["cytoband"] for t in cytobands]
            assert all([type(c) is list for c in cytobands])

    @pytest.mark.parametrize("index_name", conf.ssm_indices)
    def test_consequence_with_gene_aa_change(self, builder, maf_df, index_name):
        cons_df = builder.build_for_ssm(maf_df, index_name, add_gene_aa_change=True)

        assert "gene_aa_change" in cons_df.columns

        data = cons_df.select(
            "gene_aa_change",
            "consequence.transcript.aa_change",
            "consequence.transcript.gene.symbol",
        ).collect()
        for row in data:
            expected_list = [x for x in zip(row.symbol, row.aa_change) if None not in x]
            expected_list = map(lambda x: "{} {}".format(*x), expected_list)
            expected_list = sorted(list(set(expected_list)))

            assert sorted(row.gene_aa_change) == expected_list

    def test_all_effects_cols(self, builder, maf_df):
        effects = [
            "consequence_type",
            "aa_change",
            "transcript_id",
            "ref_seq_accession",
            "polyphen_impact",
            "polyphen_score",
            "sift_impact",
            "sift_score",
            "hgvsc",
            "vep_impact",
        ]
        ssm_tran = builder.build_all_effects_cols(maf_df)

        for e in effects:
            assert e in ssm_tran.columns

        # Check that scores are DoubleType
        for col in ["sift_score", "polyphen_score"]:
            assert ssm_tran.select(col).dtypes[0][1] == "double"

        # Check that secondary transcripts exploded correctly:
        # First gather ssm-transcript-effect map
        effects_map = {}
        for row in ssm_tran.select("ssm_id", *effects).toJSON().collect():
            row = json.loads(row)
            effects_map.setdefault(row["ssm_id"], {})
            effects_map[row["ssm_id"]].setdefault(row["transcript_id"], {})
            for effect in effects:
                effects_map[row["ssm_id"]][row["transcript_id"]].setdefault(
                    effect, set()
                )
                effects_map[row["ssm_id"]][row["transcript_id"]][effect].add(
                    row.get(effect, None)
                )

        # Now sanity check
        for ssm_id, transcripts in effects_map.items():
            for transcript_id, transcript in transcripts.items():
                # Make sure all transcripts have vep_impact and it is not None:
                assert list(transcript["vep_impact"])[
                    0
                ], "Transcript {} has no vep_impact".format(transcript_id)

                for effect, values in transcript.items():
                    # Make sure that effects are same for particular ssm-transcript combination
                    assert (
                        len(values) == 1
                    ), "{}/{}/{} unexpected effect values set of length {} != 1".format(
                        ssm_id, transcript_id, effect, len(values)
                    )

        # Check that some fields are None for all non-selected transcripts:
        null_fields = [
            "amino_acids",
            "cdna_position",
            "cds_end",
            "cds_length",
            "cds_position",
            "cds_start",
            "clin_sig",
            "codons",
            "domains",
            "ensp",
            "hgvsp",
            "hgvsp_short",
            "protein_position",
            "swissprot",
            "trembl",
            "uniparc",
        ]
        null_df = ssm_tran[ssm_tran.transcript_id != ssm_tran.selected_transcript_id]
        failed_fields = set()
        for row in null_df.toJSON().collect():
            data = json.loads(row)
            for field in null_fields:
                if field not in data:  # This is expected
                    continue
                else:
                    failed_fields.add(field)

        assert failed_fields == set()

        # Check that there are some mutations with multiple transcripts
        assert max([len(transcripts) for transcripts in effects_map.values()]) > 1

    @pytest.mark.parametrize("index_name", conf.ssm_indices)
    def test_consequence_id(self, builder, maf_df, index_name):
        """Test that consequence_id is created correctly"""
        cons_df = builder.build_for_ssm(maf_df, index_name, join_gene=False)

        assert "consequence_id" in cons_df.first().asDict()["consequence"][0]


@pytest.mark.usefixtures("sqlContext", "case_df", "source_es_client", "all_cases")
class TestCaseBuilder:
    """Test the CaseBuilder functionality for extracting the graph index"""

    def test_case_build(self, sqlContext, source_es_client, case_df):
        expected_count = source_es_client.count(index=conf.graph_case_index)["count"]
        assert case_df.count() == expected_count

    def test_case_columns(self, sqlContext, case_df):
        """Test that the right properties were loaded from case docs"""
        assert "case_id" in case_df.columns
        assert "files" not in case_df.columns
        # Make sure the sample_ids, slide_ids are not present
        assert "_ids" not in ",".join(case_df.columns)

    def test_number_of_cases(self, sqlContext, case_df, all_cases):
        """
        Check if case_df has correct number of lines

        It should include all cases in the GDC graph
        """
        assert case_df.count() == len(all_cases)

    @pytest.mark.parametrize(
        "projects, expected_count",
        [
            (["TCGA-KICH"], 6),
            (["TCGA-KIRP", "TCGA-SKCM"], 9),
            (["TCGA-TEST-NO-DATA"], 0),
        ],
    )
    def test_project_filter(
        self, projects, expected_count, sqlContext, maf_metadata_df, maf_df, cnv_df
    ):
        """Test filtering the projects included in the case DF.

        Confirm that the expected number of cases are extracted and that
        all cases are in one of the expected projects.
        """
        local_conf = config.TestConfig()
        local_conf.projects = projects

        es_dataframe_util = es_utils.DataFrameUtil(local_conf, sqlContext)
        df = builders.CaseBuilder(local_conf, sqlContext, es_dataframe_util).build(
            maf_metadata_df=maf_metadata_df, maf_df=maf_df, ascat_df=cnv_df
        )

        assert df.count() == expected_count
        for row in df.collect():
            assert row.project.project_id in projects
