import json

import pytest
from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer.viz import builders

CNV_INDICES = ("cnv_centric", "cnv_occurrence_centric")
SSM_INDICES = ("ssm_centric", "ssm_occurrence_centric")


class TestConsequenceBuilder:
    """Test intermediate result from the transcript builder"""

    @pytest.fixture(scope="class")
    def builder(self) -> builders.ConsequenceBuilder:
        return builders.ConsequenceBuilder(None, None)

    @pytest.mark.parametrize(
        "index_name",
        CNV_INDICES,
    )
    def test__build_for_cnv__consequence_count(
        self,
        builder: builders.ConsequenceBuilder,
        cnv_df: sql.DataFrame,
        index_name: str,
    ) -> None:
        cons_df = builder.build_for_cnv(cnv_df, index_name)

        n_consequences = cnv_df.select("cnv_id").distinct().count()
        assert cons_df.count() == n_consequences

    @pytest.mark.parametrize(
        "index_name",
        SSM_INDICES,
    )
    def test__build_for_ssm__consequence_count(
        self,
        builder: builders.ConsequenceBuilder,
        maf_df: sql.DataFrame,
        index_name: str,
    ) -> None:
        cons_df = builder.build_for_ssm(maf_df, index_name)

        n_consequences = maf_df.select("ssm_id").distinct().count()
        assert cons_df.count() == n_consequences

    @pytest.mark.parametrize("index_name", SSM_INDICES)
    def test_transcript_annotation_link(
        self,
        builder: builders.ConsequenceBuilder,
        maf_df: sql.DataFrame,
        index_name: str,
    ) -> None:
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

    @pytest.mark.parametrize("index_name", SSM_INDICES)
    def test_consequence_no_gene(
        self,
        builder: builders.ConsequenceBuilder,
        maf_df: sql.DataFrame,
        index_name: str,
    ) -> None:
        cons_df = builder.build_for_ssm(maf_df, index_name)
        transcripts = cons_df.select(
            F.explode("consequence.transcript").alias("transcript")
        ).select("transcript.*")

        # Check that gene not in transctipts
        assert "gene" not in transcripts.columns

    @pytest.mark.parametrize("index_name", SSM_INDICES)
    def test_consequence_with_gene(
        self,
        builder: builders.ConsequenceBuilder,
        maf_df: sql.DataFrame,
        index_name: str,
    ) -> None:
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

    @pytest.mark.parametrize("index_name", SSM_INDICES)
    def test_consequence_with_gene_aa_change(
        self,
        builder: builders.ConsequenceBuilder,
        maf_df: sql.DataFrame,
        index_name: str,
    ) -> None:
        cons_df = builder.build_for_ssm(
            maf_df, index_name, add_gene_aa_change=True, join_gene=True
        )

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

    def test_all_effects_cols(
        self, builder: builders.ConsequenceBuilder, maf_df: sql.DataFrame
    ) -> None:
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
                effects_map[row["ssm_id"]][row["transcript_id"]].setdefault(effect, set())
                effects_map[row["ssm_id"]][row["transcript_id"]][effect].add(
                    row.get(effect, None)
                )

        # Now sanity check
        for ssm_id, transcripts in effects_map.items():
            for transcript_id, transcript in transcripts.items():
                # Make sure all transcripts have vep_impact and it is not None:
                assert list(transcript["vep_impact"])[0], (
                    f"Transcript {transcript_id} has no vep_impact"
                )

                for effect, values in transcript.items():
                    # Make sure that effects are same for particular ssm-transcript combination
                    assert len(values) == 1, (
                        f"{ssm_id}/{transcript_id}/{effect} unexpected effect values set of length {len(values)} != 1"
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

    @pytest.mark.parametrize("index_name", SSM_INDICES)
    def test_consequence_id(
        self,
        builder: builders.ConsequenceBuilder,
        maf_df: sql.DataFrame,
        index_name: str,
    ) -> None:
        """Test that consequence_id is created correctly"""
        cons_df = builder.build_for_ssm(maf_df, index_name, join_gene=False)

        assert "consequence_id" in cons_df.first().asDict()["consequence"][0]
