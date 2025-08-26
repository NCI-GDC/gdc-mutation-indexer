from collections.abc import Iterable

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

from mutation_indexer.builders import df_builders
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import viz as models


@pytest.fixture(scope="class")
def maf_schema() -> types.StructType:
    return schemas.Viz.Builders.MAF.FINAL.load()


@pytest.fixture(scope="class")
def consequence_schema() -> types.StructType:
    return schemas.Viz.Builders.Consequence.SSM.FINAL.load()


@pytest.fixture(scope="class")
def observation_schema() -> types.StructType:
    return schemas.Viz.Builders.Observation.SSM.FINAL.load()


@pytest.fixture(scope="class")
def final_occurrence_schema() -> types.StructType:
    return schemas.Viz.Builders.DFBuilders.SSM.Occurrence.FINAL.load()


@pytest.fixture(scope="class")
def final_other_schema() -> types.StructType:
    return schemas.Viz.Builders.DFBuilders.SSM.Other.FINAL.load()


@pytest.fixture(scope="class")
def final_ssm_schema() -> types.StructType:
    return schemas.Viz.Builders.DFBuilders.SSM.FINAL.load()


class TestGetSSMDataFrame:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        create_dataframe: utils.CreateDataFrame,
        maf_schema: types.StructType,
        final_ssm_schema: types.StructType,
    ) -> None:
        self.create_dataframe = create_dataframe
        self.maf_schema = maf_schema
        self.final_ssm_schema = final_ssm_schema

    def _arrange_maf_df(self, mafs: Iterable[models.MAF] = (models.MAF(),)) -> sql.DataFrame:
        return self.create_dataframe(mafs, self.maf_schema)

    def test__single_row(self) -> None:
        maf = models.MAF()
        maf_df = self._arrange_maf_df((maf,))

        result_df = df_builders.get_ssm_df(maf_df, "ssm_centric", unique_fields=["ssm_id"])

        assert result_df.count() == 1
        assert result_df.schema == self.final_ssm_schema

        result_row = more_itertools.one(result_df.collect())

        assert result_row.ssm_id == maf.ssm_id
        assert result_row.chromosome == maf.chromosome
        assert tuple(result_row.cosmic_id) == maf.cosmic_id
        assert result_row.end_position == maf.end_position
        assert result_row.genomic_dna_change == maf.genomic_dna_change
        assert result_row.mutation_subtype == maf.mutation_subtype
        assert result_row.mutation_type == maf.mutation_type
        assert result_row.ncbi_build == maf.ncbi_build
        assert result_row.reference_allele == maf.reference_allele
        assert result_row.start_position == maf.start_position
        assert result_row.tumor_allele == maf.tumor_allele
        assert result_row.clinical_annotations.civic.gene_id == maf.civic_gene_id
        assert result_row.clinical_annotations.civic.variant_id == maf.civic_variant_id


class TestBuildSSMSubtree:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        create_dataframe: utils.CreateDataFrame,
        maf_schema: types.StructType,
        consequence_schema: types.StructType,
        observation_schema: types.StructType,
        final_occurrence_schema: types.StructType,
        final_other_schema: types.StructType,
    ) -> None:
        self.create_dataframe = create_dataframe
        self.maf_schema = maf_schema
        self.consequence_schema = consequence_schema
        self.observation_schema = observation_schema
        self.final_occurrence_schema = final_occurrence_schema
        self.final_other_schema = final_other_schema

    def _arrange_maf_df(self, mafs: Iterable[models.MAF] = (models.MAF(),)) -> sql.DataFrame:
        return self.create_dataframe(mafs, self.maf_schema)

    def _arrange_consequence_df(
        self,
        consequences: Iterable[models.consequence.SSM] = (models.consequence.SSM(),),
        drop_aa_change: bool = False,
        drop_genes: bool = False,
    ) -> sql.DataFrame:
        df: sql.DataFrame = self.create_dataframe(consequences, self.consequence_schema)

        if drop_genes:
            df = df.withColumn(
                "consequence",
                F.transform("consequence", lambda c: c.dropFields("transcript.gene")),
            )

        if drop_aa_change:
            df = df.drop("gene_aa_change")

        return df

    def _arrange_observation_df(
        self,
        observations: Iterable[models.observation.SSM] = (models.observation.SSM(),),
    ) -> sql.DataFrame:
        df = self.create_dataframe(observations, self.observation_schema)

        return df

    def test__occurrence(self) -> None:
        maf = models.MAF()
        maf_df = self._arrange_maf_df((maf,))
        consequence_wrapper = models.consequence.SSM()
        consequence_df = self._arrange_consequence_df((consequence_wrapper,))

        result_df = df_builders.build_ssm_subtree(
            maf_df, consequence_df, "ssm_occurrence_centric"
        )

        assert result_df.count() == 1
        assert result_df.schema == self.final_occurrence_schema

        result_row = more_itertools.one(result_df.collect())

        assert result_row.ssm_id == maf.ssm_id
        assert result_row.gene_id == maf.gene_id
        assert result_row.case_id == maf.case_id
        assert result_row.chromosome == maf.chromosome
        assert tuple(result_row.cosmic_id) == maf.cosmic_id
        assert result_row.end_position == maf.end_position
        assert result_row.genomic_dna_change == maf.genomic_dna_change
        assert result_row.mutation_subtype == maf.mutation_subtype
        assert result_row.mutation_type == maf.mutation_type
        assert result_row.ncbi_build == maf.ncbi_build
        assert result_row.reference_allele == maf.reference_allele
        assert result_row.start_position == maf.start_position
        assert result_row.tumor_allele == maf.tumor_allele
        assert result_row.clinical_annotations.civic.gene_id == maf.civic_gene_id
        assert result_row.clinical_annotations.civic.variant_id == maf.civic_variant_id

        result_consequence = more_itertools.one(result_row.consequence)
        consequence = more_itertools.one(consequence_wrapper.consequence or ())
        assert result_consequence.consequence_id == consequence.consequence_id

        result_transcript = result_consequence.transcript
        transcript = consequence.transcript
        assert result_transcript and transcript
        assert result_transcript.transcript_id == transcript.transcript_id
        assert result_transcript.aa_change == transcript.aa_change
        assert result_transcript.aa_end == transcript.aa_end
        assert result_transcript.aa_start == transcript.aa_start
        assert result_transcript.consequence_type == transcript.consequence_type
        assert result_transcript.is_canonical == transcript.is_canonical
        assert result_transcript.ref_seq_accession == transcript.ref_seq_accession

        result_annotation = result_transcript.annotation
        annotation = transcript.annotation
        assert result_annotation and annotation
        assert result_annotation.amino_acids == annotation.amino_acids
        assert result_annotation.ccds == annotation.ccds
        assert result_annotation.cdna_position == annotation.cdna_position
        assert result_annotation.cds_end == annotation.cds_end
        assert result_annotation.cds_length == annotation.cds_length
        assert result_annotation.cds_position == annotation.cds_position
        assert result_annotation.cds_start == annotation.cds_start
        assert result_annotation.clin_sig == annotation.clin_sig
        assert result_annotation.codons == annotation.codons
        assert result_annotation.dbsnp_rs == annotation.dbsnp_rs
        assert result_annotation.dbsnp_val_status == annotation.dbsnp_val_status
        assert result_annotation.domains == annotation.domains
        assert result_annotation.hgvsc == annotation.hgvsc
        assert result_annotation.hgvsp == annotation.hgvsp
        assert result_annotation.hgvsp_short == annotation.hgvsp_short
        assert result_annotation.polyphen_impact == annotation.polyphen_impact
        assert result_annotation.polyphen_score == annotation.polyphen_score
        assert result_annotation.protein_position == annotation.protein_position
        assert result_annotation.pubmed == annotation.pubmed
        assert result_annotation.sift_impact == annotation.sift_impact
        assert result_annotation.sift_score == annotation.sift_score
        assert result_annotation.swissprot == annotation.swissprot
        assert result_annotation.transcript_id == annotation.transcript_id
        assert result_annotation.trembl == annotation.trembl
        assert result_annotation.uniparc == annotation.uniparc
        assert result_annotation.vep_impact == annotation.vep_impact

        result_gene = result_transcript.gene
        gene = transcript.gene
        assert result_gene and gene
        assert result_gene.biotype == gene.biotype
        assert result_gene.canonical_transcript_id == gene.canonical_transcript_id
        assert tuple(result_gene.cytoband) == gene.cytoband
        assert result_gene.gene_chromosome == gene.gene_chromosome
        assert result_gene.gene_end == gene.gene_end
        assert result_gene.gene_id == gene.gene_id
        assert result_gene.gene_start == gene.gene_start
        assert result_gene.gene_strand == gene.gene_strand
        assert result_gene.is_cancer_gene_census == gene.is_cancer_gene_census
        assert result_gene.symbol == gene.symbol
        assert tuple(result_gene.synonyms) == gene.synonyms

        result_external_db_ids = result_gene.external_db_ids
        external_db_ids = gene.external_db_ids
        assert result_external_db_ids and external_db_ids
        assert tuple(result_external_db_ids.entrez_gene) == external_db_ids.entrez_gene
        assert tuple(result_external_db_ids.hgnc) == external_db_ids.hgnc
        assert tuple(result_external_db_ids.omim_gene) == external_db_ids.omim_gene
        assert (
            tuple(result_external_db_ids.uniprotkb_swissprot)
            == external_db_ids.uniprotkb_swissprot
        )

        result_aa_change = result_row.gene_aa_change
        aa_change = consequence_wrapper.gene_aa_change
        assert result_aa_change and aa_change
        assert tuple(result_aa_change) == aa_change

    @pytest.mark.parametrize("index_name", ("case_centric", "gene_centric"))
    def test__other(self, index_name: str) -> None:
        maf = models.MAF()
        maf_df = self._arrange_maf_df((maf,))
        consequence_wrapper = models.consequence.SSM()
        consequence_df = self._arrange_consequence_df(
            (consequence_wrapper,), drop_aa_change=True, drop_genes=True
        )
        observation_wrapper = models.observation.SSM()
        observation_df = self._arrange_observation_df((observation_wrapper,))

        result_df = df_builders.build_ssm_subtree(
            maf_df, consequence_df, index_name, observation_df
        )

        assert result_df.count() == 1
        assert result_df.schema == self.final_other_schema

        result_row = more_itertools.one(result_df.collect())

        assert result_row.occurrence_id == observation_wrapper.occurrence_id

        assert result_row.ssm_id == maf.ssm_id
        assert result_row.gene_id == maf.gene_id
        assert result_row.case_id == maf.case_id
        assert result_row.chromosome == maf.chromosome
        assert tuple(result_row.cosmic_id) == maf.cosmic_id
        assert result_row.end_position == maf.end_position
        assert result_row.genomic_dna_change == maf.genomic_dna_change
        assert result_row.mutation_subtype == maf.mutation_subtype
        assert result_row.mutation_type == maf.mutation_type
        assert result_row.ncbi_build == maf.ncbi_build
        assert result_row.reference_allele == maf.reference_allele
        assert result_row.start_position == maf.start_position
        assert result_row.tumor_allele == maf.tumor_allele
        assert result_row.clinical_annotations.civic.gene_id == maf.civic_gene_id
        assert result_row.clinical_annotations.civic.variant_id == maf.civic_variant_id

        result_consequence = more_itertools.one(result_row.consequence)
        consequence = more_itertools.one(consequence_wrapper.consequence or ())
        assert result_consequence.consequence_id == consequence.consequence_id

        result_transcript = result_consequence.transcript
        transcript = consequence.transcript
        assert result_transcript and transcript
        assert result_transcript.transcript_id == transcript.transcript_id
        assert result_transcript.aa_change == transcript.aa_change
        assert result_transcript.aa_end == transcript.aa_end
        assert result_transcript.aa_start == transcript.aa_start
        assert result_transcript.consequence_type == transcript.consequence_type
        assert result_transcript.is_canonical == transcript.is_canonical
        assert result_transcript.ref_seq_accession == transcript.ref_seq_accession

        result_annotation = result_transcript.annotation
        annotation = transcript.annotation
        assert result_annotation and annotation
        assert result_annotation.amino_acids == annotation.amino_acids
        assert result_annotation.ccds == annotation.ccds
        assert result_annotation.cdna_position == annotation.cdna_position
        assert result_annotation.cds_end == annotation.cds_end
        assert result_annotation.cds_length == annotation.cds_length
        assert result_annotation.cds_position == annotation.cds_position
        assert result_annotation.cds_start == annotation.cds_start
        assert result_annotation.clin_sig == annotation.clin_sig
        assert result_annotation.codons == annotation.codons
        assert result_annotation.dbsnp_rs == annotation.dbsnp_rs
        assert result_annotation.dbsnp_val_status == annotation.dbsnp_val_status
        assert result_annotation.domains == annotation.domains
        assert result_annotation.hgvsc == annotation.hgvsc
        assert result_annotation.hgvsp == annotation.hgvsp
        assert result_annotation.hgvsp_short == annotation.hgvsp_short
        assert result_annotation.polyphen_impact == annotation.polyphen_impact
        assert result_annotation.polyphen_score == annotation.polyphen_score
        assert result_annotation.protein_position == annotation.protein_position
        assert result_annotation.pubmed == annotation.pubmed
        assert result_annotation.sift_impact == annotation.sift_impact
        assert result_annotation.sift_score == annotation.sift_score
        assert result_annotation.swissprot == annotation.swissprot
        assert result_annotation.transcript_id == annotation.transcript_id
        assert result_annotation.trembl == annotation.trembl
        assert result_annotation.uniparc == annotation.uniparc
        assert result_annotation.vep_impact == annotation.vep_impact

        result_observation = more_itertools.one(result_row.observation)
        observation = more_itertools.one(observation_wrapper.observation or ())
        assert result_observation.center == observation.center
        assert result_observation.input_bam_file and observation.input_bam_file
        assert (
            result_observation.input_bam_file.normal_bam_uuid
            == observation.input_bam_file.normal_bam_uuid
        )
        assert (
            result_observation.input_bam_file.tumor_bam_uuid
            == observation.input_bam_file.tumor_bam_uuid
        )
        assert result_observation.mutation_status == observation.mutation_status
        assert result_observation.normal_genotype and observation.normal_genotype
        assert (
            result_observation.normal_genotype.match_norm_seq_allele1
            == observation.normal_genotype.match_norm_seq_allele1
        )
        assert (
            result_observation.normal_genotype.match_norm_seq_allele2
            == observation.normal_genotype.match_norm_seq_allele2
        )
        assert result_observation.observation_id == observation.observation_id
        assert result_observation.read_depth and observation.read_depth
        assert result_observation.read_depth.n_depth == observation.read_depth.n_depth
        assert result_observation.read_depth.t_alt_count == observation.read_depth.t_alt_count
        assert result_observation.read_depth.t_depth == observation.read_depth.t_depth
        assert result_observation.read_depth.t_ref_count == observation.read_depth.t_ref_count
        assert result_observation.sample and observation.sample
        assert (
            result_observation.sample.matched_norm_sample_barcode
            == observation.sample.matched_norm_sample_barcode
        )
        assert (
            result_observation.sample.matched_norm_sample_uuid
            == observation.sample.matched_norm_sample_uuid
        )
        assert (
            result_observation.sample.tumor_sample_barcode
            == observation.sample.tumor_sample_barcode
        )
        assert (
            result_observation.sample.tumor_sample_uuid == observation.sample.tumor_sample_uuid
        )
        assert result_observation.tumor_genotype and observation.tumor_genotype
        assert (
            result_observation.tumor_genotype.tumor_seq_allele1
            == observation.tumor_genotype.tumor_seq_allele1
        )
        assert (
            result_observation.tumor_genotype.tumor_seq_allele2
            == observation.tumor_genotype.tumor_seq_allele2
        )
        assert result_observation.variant_calling and observation.variant_calling
        assert (
            result_observation.variant_calling.variant_caller
            == observation.variant_calling.variant_caller
        )
        assert (
            result_observation.variant_calling.variant_process
            == observation.variant_calling.variant_process
        )
