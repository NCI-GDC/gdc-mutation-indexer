import dataclasses
from typing import Any, Dict, Iterable, List, Optional, Tuple
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from exports import builders
from exports.configuration.builders import common, viz
from exports.constants import build
from tests.unit import utils
from tests.unit.data import schemas


@dataclasses.dataclass(frozen=True)
class MAF:
    Hugo_Symbol: str = "CSMD2"
    Entrez_Gene_Id: str = "114784"
    Center: str = "BI"
    NCBI_Build: str = "GRCh38"
    Chromosome: str = "chr1"
    Start_Position: str = "33772590"
    End_Position: str = "33772590"
    Strand: str = "+"
    Variant_Classification: str = "Missense_Mutation"
    Variant_Type: str = "SNP"
    Reference_Allele: str = "C"
    Tumor_Seq_Allele1: str = "C"
    Tumor_Seq_Allele2: str = "A"
    dbSNP_RS: str = "novel"
    dbSNP_Val_Status: Optional[str] = None
    Tumor_Sample_Barcode: Optional[str] = "MBCProject_3808_T1_WES_1"
    Matched_Norm_Sample_Barcode: str = "MBCProject_3808_SALIVA_1"
    Match_Norm_Seq_Allele1: Optional[str] = None
    Match_Norm_Seq_Allele2: Optional[str] = None
    Tumor_Validation_Allele1: Optional[str] = None
    Tumor_Validation_Allele2: Optional[str] = None
    Match_Norm_Validation_Allele1: Optional[str] = None
    Match_Norm_Validation_Allele2: Optional[str] = None
    Verification_Status: Optional[str] = None
    Validation_Status: Optional[str] = None
    Mutation_Status: str = "Somatic"
    Sequencing_Phase: Optional[str] = None
    Sequence_Source: Optional[str] = None
    Validation_Method: Optional[str] = None
    Score: Optional[str] = None
    BAM_File: Optional[str] = None
    Sequencer: str = "Illumina HiSeq 4000"
    Tumor_Sample_UUID: str = "c004a75a-448b-440c-bd8f-46cfc6d8dd2a"
    Matched_Norm_Sample_UUID: str = "0e4ad056-bfba-4ff3-a41f-d3655009f544"
    HGVSc: str = "c.1705G>T"
    HGVSp: str = "p.Ala569Ser"
    HGVSp_Short: str = "p.A569S"
    Transcript_ID: str = "ENST00000241312"
    Exon_Number: str = "13/70"
    t_depth: str = "29"
    t_ref_count: str = "24"
    t_alt_count: str = "5"
    n_depth: str = "38"
    n_ref_count: Optional[str] = None
    n_alt_count: Optional[str] = None
    all_effects: str = "CSMD2,missense_variant,p.A609S,ENST00000373381,NM_001281956.2,c.1825G>T,MODERATE,YES,tolerated(0.14),benign(0.305),-1;CSMD2,missense_variant,p.A569S,ENST00000619121,,c.1705G>T,MODERATE,,tolerated(0.13),benign(0.02),-1;CSMD2,missense_variant,p.A569S,ENST00000373388,NM_052896.4,c.1705G>T,MODERATE,,tolerated(0.12),benign(0.305),-1;CSMD2,missense_variant,p.A217S,ENST00000338325,,c.649G>T,MODERATE,,tolerated(0.18),benign(0.264),-1;CSMD2,missense_variant,p.A569S,ENST00000241312,,c.1705G>T,MODERATE,,tolerated(0.12),benign(0.305),-1"
    Allele: str = "A"
    Gene: str = "ENSG00000121904"
    Feature: str = "ENST00000241312"
    Feature_type: str = "Transcript"
    One_Consequence: str = "missense_variant"
    Consequence: str = "missense_variant;NMD_transcript_variant"
    cDNA_position: str = "1734/13108"
    CDS_position: str = "1705/10464"
    Protein_position: str = "569/3487"
    Amino_acids: str = "A/S"
    Codons: str = "Gct/Tct"
    Existing_variation: Optional[str] = None
    DISTANCE: Optional[str] = None
    TRANSCRIPT_STRAND: str = "-1"
    SYMBOL: str = "CSMD2"
    SYMBOL_SOURCE: str = "HGNC"
    HGNC_ID: str = "HGNC:19290"
    BIOTYPE: str = "nonsense_mediated_decay"
    CANONICAL: Optional[str] = None
    CCDS: str = "CCDS380.1"
    ENSP: str = "ENSP00000241312"
    SWISSPROT: str = "Q7Z408.146"
    TREMBL: Optional[str] = None
    UNIPARC: str = "UPI00004561AB"
    UNIPROT_ISOFORM: str = "Q7Z408-1"
    RefSeq: Optional[str] = None
    MANE: Optional[str] = None
    APPRIS: Optional[str] = None
    FLAGS: Optional[str] = None
    SIFT: Optional[str] = "tolerated(0.12)"
    PolyPhen: Optional[str] = "benign(0.305)"
    EXON: str = "13/70"
    INTRON: Optional[str] = None
    DOMAINS: Optional[str] = None
    ThousandG_AF: Optional[str] = None
    ThousandG_AFR_AF: Optional[str] = None
    ThousandG_AMR_AF: Optional[str] = None
    ThousandG_EAS_AF: Optional[str] = None
    ThousandG_EUR_AF: Optional[str] = None
    ThousandG_SAS_AF: Optional[str] = None
    ESP_AA_AF: Optional[str] = None
    ESP_EA_AF: Optional[str] = None
    gnomAD_AF: Optional[str] = None
    gnomAD_AFR_AF: Optional[str] = None
    gnomAD_AMR_AF: Optional[str] = None
    gnomAD_ASJ_AF: Optional[str] = None
    gnomAD_EAS_AF: Optional[str] = None
    gnomAD_FIN_AF: Optional[str] = None
    gnomAD_NFE_AF: Optional[str] = None
    gnomAD_OTH_AF: Optional[str] = None
    gnomAD_SAS_AF: Optional[str] = None
    MAX_AF: Optional[str] = None
    MAX_AF_POPS: Optional[str] = None
    gnomAD_non_cancer_AF: Optional[str] = None
    gnomAD_non_cancer_AFR_AF: Optional[str] = None
    gnomAD_non_cancer_AMI_AF: Optional[str] = None
    gnomAD_non_cancer_AMR_AF: Optional[str] = None
    gnomAD_non_cancer_ASJ_AF: Optional[str] = None
    gnomAD_non_cancer_EAS_AF: Optional[str] = None
    gnomAD_non_cancer_FIN_AF: Optional[str] = None
    gnomAD_non_cancer_MID_AF: Optional[str] = None
    gnomAD_non_cancer_NFE_AF: Optional[str] = None
    gnomAD_non_cancer_OTH_AF: Optional[str] = None
    gnomAD_non_cancer_SAS_AF: Optional[str] = None
    gnomAD_non_cancer_MAX_AF_adj: Optional[str] = None
    gnomAD_non_cancer_MAX_AF_POPS_adj: Optional[str] = None
    CLIN_SIG: Optional[str] = None
    SOMATIC: Optional[str] = None
    PUBMED: Optional[str] = None
    TRANSCRIPTION_FACTORS: Optional[str] = None
    MOTIF_NAME: Optional[str] = None
    MOTIF_POS: Optional[str] = None
    HIGH_INF_POS: Optional[str] = None
    MOTIF_SCORE_CHANGE: Optional[str] = None
    miRNA: Optional[str] = None
    IMPACT: str = "MODERATE"
    PICK: Optional[str] = None
    VARIANT_CLASS: str = "SNV"
    TSL: str = "1"
    HGVS_OFFSET: Optional[str] = None
    PHENO: Optional[str] = None
    GENE_PHENO: Optional[str] = None
    CONTEXT: str = "CTTAGCCGACC"
    tumor_bam_uuid: str = "9fa1ff4d-230d-477b-91d6-e2dc3896b6c4"
    normal_bam_uuid: str = "604c11f1-ab8b-48a7-909e-982e873e02e5"
    case_id: Optional[str] = "3680a87f-f493-42f0-abf7-741df6a7c9e7"
    GDC_FILTER: Optional[str] = None
    COSMIC: Optional[str] = None
    hotspot: str = "N"
    RNA_Support: str = "Unknown"
    RNA_depth: Optional[str] = None
    RNA_ref_count: Optional[str] = None
    RNA_alt_count: Optional[str] = None
    callers: str = "muse;varscan2"
    ALLELE_NUM: int = 0
    MINIMISED: str = ""
    src_vcf_id: str = "src-0"
    Disease_type: Optional[str] = None
    FMI_STATUS: Optional[str] = None
    FMI_GENE: Optional[str] = None
    FMI_TRANSCRIPT: Optional[str] = None
    FMI_FUNCTIONAL_EFFECT: Optional[str] = None

    def to_sql_row(self) -> sql.Row:
        data = dataclasses.asdict(self)

        data["1000G_AF"] = data.pop("ThousandG_AF")
        data["1000G_AFR_AF"] = data.pop("ThousandG_AFR_AF")
        data["1000G_AMR_AF"] = data.pop("ThousandG_AMR_AF")
        data["1000G_EAS_AF"] = data.pop("ThousandG_EAS_AF")
        data["1000G_EUR_AF"] = data.pop("ThousandG_EUR_AF")
        data["1000G_SAS_AF"] = data.pop("ThousandG_SAS_AF")

        return sql.Row(**data)


@dataclasses.dataclass(frozen=True)
class Domain:
    description: str = "G protein-coupled receptor, rhodopsin-like"
    end: int = 280
    gff_source: str = "pfam"
    hit_name: str = "PF00001"
    interpro_id: str = "IPR000276"
    start: int = 34


@dataclasses.dataclass(frozen=True)
class Exon:
    cdna_coding_end: int = 0
    cdna_coding_start: int = 0
    cdna_end: int = 359
    cdna_start: int = 1
    end: int = 12227
    end_phase: int = -1
    genomic_coding_end: int = 0
    genomic_coding_stairt: int = 0
    genomic_coding_start: int = 0
    start: int = 11869
    start_phase: int = -1


@dataclasses.dataclass(frozen=True)
class Transcript:
    biotype: str = "processed_transcript"
    cdna_coding_end: int = 0
    cdna_coding_start: int = 0
    coding_region_end: int = 0
    coding_region_start: int = 0
    domains: Tuple[Domain, ...] = (Domain(),)
    end: int = 14409
    end_exon: Optional[int] = None
    exons: Tuple[Exon, ...] = (Exon(),)
    transcript_id: str = "ENST00000456328"
    is_canonical: bool = False
    length: int = 1657
    length_amino_acid: Optional[int] = None
    length_cds: Optional[int] = None
    name: str = "DDX11L1-002"
    number_of_exons: int = 6
    seq_exon_end: Optional[int] = None
    seq_exon_start: Optional[int] = None
    start: int = 11869
    start_exon: Optional[int] = None
    translation_id: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class GeneModel:
    _gene_id: str = "ENSG00000121904"
    _id: Dict[str, str] = dataclasses.field(
        default_factory=lambda: {"$oid": "589c87ca0ef75875ed614a40"}
    )
    biotype: str = "transcribed_unprocessed_pseudogene"
    canonical_transcript_id: str = "ENST00000456328"
    chromosome: str = "1"
    cytoband: Tuple[Optional[str], ...] = ("1p36.33",)
    description: str = "DISCONTINUED: This record has been withdrawn by NCBI because the model on which it was based was not predicted in a later annotation."
    entrez_gene: Tuple[str, ...] = ("100287596", "100287102", "727856", "84771")
    gene_end: int = 14409
    gene_start: int = 11869
    gene_strand: int = 1
    hgnc: Tuple[str, ...] = ("HGNC:37102",)
    is_cancer_gene_census: str = "true"
    omim_gene: Tuple[str, ...] = ()
    uniprotkb_swissprot: Tuple[str, ...] = ()
    name: str = "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
    symbol: str = "DDX11L1"
    synonyms: Tuple[str, ...] = ()
    transcripts: Tuple[Transcript, ...] = (Transcript(),)


@pytest.fixture(scope="class")
def gene_model_schema() -> types.StructType:
    return schemas.load_schema("builders/maf/input_gene_model.json")


@pytest.fixture(scope="class")
def masked_somatic_mutation_schema() -> types.StructType:
    return schemas.load_schema("builders/maf/masked_somatic_mutation.yaml")


@pytest.fixture(scope="class")
def aggregated_somatic_mutation_schema() -> types.StructType:
    return schemas.load_schema("builders/maf/aggregated_somatic_mutation.yaml")


@pytest.fixture(scope="class")
def final_maf_schema() -> types.StructType:
    return schemas.load_schema("builders/maf/final_maf.json")


def assert_domains_equal(result_domain: sql.Row, domain: Domain) -> None:
    assert result_domain.description == domain.description
    assert result_domain.end == domain.end
    assert result_domain.gff_source == domain.gff_source
    assert result_domain.hit_name == domain.hit_name
    assert result_domain.interpro_id == domain.interpro_id
    assert result_domain.start == domain.start


def assert_exons_equal(result_exon: sql.Row, exon: Exon) -> None:
    assert result_exon.cdna_coding_end == exon.cdna_coding_end
    assert result_exon.cdna_coding_start == exon.cdna_coding_start
    assert result_exon.cdna_end == exon.cdna_end
    assert result_exon.cdna_start == exon.cdna_start
    assert result_exon.end == exon.end
    assert result_exon.end_phase == exon.end_phase
    assert result_exon.genomic_coding_end == exon.genomic_coding_end
    assert result_exon.genomic_coding_stairt == exon.genomic_coding_stairt
    assert result_exon.genomic_coding_start == exon.genomic_coding_start
    assert result_exon.start == exon.start
    assert result_exon.start_phase == exon.start_phase


def assert_transcripts_equal(
    result_transcript: sql.Row, transcript: Transcript
) -> None:
    assert result_transcript.biotype == transcript.biotype
    assert result_transcript.cdna_coding_end == transcript.cdna_coding_end
    assert result_transcript.cdna_coding_start == transcript.cdna_coding_start
    assert result_transcript.coding_region_end == transcript.coding_region_end
    assert result_transcript.coding_region_start == transcript.coding_region_start
    assert result_transcript.end == transcript.end
    assert result_transcript.end_exon == transcript.end_exon
    assert result_transcript.transcript_id == transcript.transcript_id
    assert result_transcript.is_canonical == transcript.is_canonical
    assert result_transcript.length == transcript.length
    assert result_transcript.length_amino_acid == transcript.length_amino_acid
    assert result_transcript.length_cds == transcript.length_cds
    assert result_transcript.name == transcript.name
    assert result_transcript.number_of_exons == transcript.number_of_exons
    assert result_transcript.seq_exon_end == transcript.seq_exon_end
    assert result_transcript.seq_exon_start == transcript.seq_exon_start
    assert result_transcript.start == transcript.start
    assert result_transcript.start_exon == transcript.start_exon
    assert result_transcript.translation_id == transcript.translation_id

    for result_domain, domain in more_itertools.zip_equal(
        result_transcript.domains, transcript.domains
    ):
        assert_domains_equal(result_domain, domain)

    for result_exon, exon in more_itertools.zip_equal(
        result_transcript.exons, transcript.exons
    ):
        assert_exons_equal(result_exon, exon)


def assert_core_maf_transformed(
    result_maf: sql.Row, maf: MAF, gene_model: GeneModel
) -> None:
    assert result_maf._id.asDict() == gene_model._id
    assert result_maf.aa_change == maf.ESP_AA_AF
    assert result_maf.aa_end == maf.ESP_AA_AF
    assert result_maf.aa_start == maf.ESP_AA_AF
    assert result_maf.all_effects == maf.all_effects
    assert result_maf.amino_acids == maf.Amino_acids
    assert result_maf.biotype == gene_model.biotype
    assert result_maf.canonical_transcript_id == gene_model.canonical_transcript_id
    assert result_maf.case_id == maf.case_id
    assert result_maf.ccds == maf.CCDS
    assert result_maf.cdna_position == maf.cDNA_position
    assert result_maf.cds_position == maf.CDS_position
    assert result_maf.center == maf.Center
    assert result_maf.clin_sig == maf.CLIN_SIG
    assert result_maf.codons == maf.Codons
    assert result_maf.consequence_type == maf.Consequence
    assert tuple(result_maf.cytoband) == gene_model.cytoband
    assert result_maf.dbsnp_rs == maf.dbSNP_RS
    assert result_maf.dbsnp_val_status == maf.dbSNP_Val_Status
    assert result_maf.description == gene_model.description
    assert result_maf.domains == maf.DOMAINS
    assert result_maf.ensp == maf.ENSP
    assert tuple(result_maf.entrez_gene) == gene_model.entrez_gene
    assert result_maf.existing_variation == maf.Existing_variation
    assert result_maf.gene_end == gene_model.gene_end
    assert result_maf.gene_id == maf.Gene
    assert result_maf.gene_start == gene_model.gene_start
    assert result_maf.gene_strand == gene_model.gene_strand
    assert tuple(result_maf.hgnc) == gene_model.hgnc
    assert result_maf.hgvsc == maf.HGVSc
    assert result_maf.hgvsp == maf.HGVSp
    assert result_maf.hgvsp_short == maf.HGVSp_Short
    assert result_maf.is_cancer_gene_census == gene_model.is_cancer_gene_census
    assert result_maf.match_norm_seq_allele1 == maf.Match_Norm_Seq_Allele1
    assert result_maf.match_norm_seq_allele2 == maf.Match_Norm_Seq_Allele2
    assert result_maf.matched_norm_sample_barcode == maf.Matched_Norm_Sample_Barcode
    assert result_maf.matched_norm_sample_uuid == maf.Matched_Norm_Sample_UUID
    assert result_maf.mutation_status == maf.Mutation_Status
    assert result_maf.name == gene_model.name
    assert result_maf.ncbi_build == maf.NCBI_Build
    assert tuple(result_maf.omim_gene) == gene_model.omim_gene
    assert result_maf.protein_position == maf.Protein_position
    assert result_maf.pubmed == maf.PUBMED
    assert result_maf.ref_seq_accession == maf.ESP_AA_AF
    assert result_maf.reference_allele == maf.Reference_Allele
    assert result_maf.swissprot == maf.SWISSPROT
    assert result_maf.symbol == gene_model.symbol
    assert tuple(result_maf.synonyms) == gene_model.synonyms
    assert result_maf.transcript_id == maf.Transcript_ID
    assert result_maf.trembl == maf.TREMBL
    assert result_maf.tumor_allele == maf.Allele
    assert result_maf.tumor_sample_barcode == maf.Tumor_Sample_Barcode
    assert result_maf.tumor_sample_uuid == maf.Tumor_Sample_UUID
    assert result_maf.tumor_seq_allele1 == maf.Tumor_Seq_Allele1
    assert result_maf.tumor_seq_allele2 == maf.Tumor_Seq_Allele2
    assert result_maf.tumor_validation_allele1 == maf.Tumor_Validation_Allele1
    assert result_maf.tumor_validation_allele2 == maf.Tumor_Validation_Allele2
    assert result_maf.uniparc == maf.UNIPARC
    assert tuple(result_maf.uniprotkb_swissprot) == gene_model.uniprotkb_swissprot
    assert result_maf.validation_method == maf.Validation_Method
    assert result_maf.variant_type == maf.Variant_Type
    assert result_maf.vep_impact == maf.IMPACT

    for result_transcript, expected_transcript in more_itertools.zip_equal(
        result_maf.transcripts, gene_model.transcripts
    ):
        assert_transcripts_equal(result_transcript, expected_transcript)


class TestMAFBuilder:
    @pytest.fixture(autouse=True)
    def load_fixtures(
        self,
        spark_session: sql.SparkSession,
        gene_model_schema: types.StructType,
        masked_somatic_mutation_schema: types.StructType,
        aggregated_somatic_mutation_schema: types.StructType,
        final_maf_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.gene_model_schema = gene_model_schema
        self.masked_somatic_mutation_schema = masked_somatic_mutation_schema
        self.aggregated_somatic_mutation_schema = aggregated_somatic_mutation_schema
        self.final_maf_schema = final_maf_schema

    def arrange_builder(
        self,
        masked_somatic_mutation_mafs: Tuple[MAF, ...] = (MAF(),),
        aggregated_somatic_mutation_mafs: Tuple[MAF, ...] = (),
        annotation_builders: Iterable[mock.MagicMock] = (),
    ) -> builders.MAFBuilder:
        masked_somatic_mutation_df = self.spark_session.createDataFrame(
            tuple(maf.to_sql_row() for maf in masked_somatic_mutation_mafs),
            schema=self.masked_somatic_mutation_schema,
        )
        aggregated_somatic_mutation_df = self.spark_session.createDataFrame(
            tuple(maf.to_sql_row() for maf in aggregated_somatic_mutation_mafs),
            schema=self.aggregated_somatic_mutation_schema,
        )

        config = viz.MAFBuilder(
            is_cached=False,
            backup=common.Backup(mode=build.BackupMode.NEITHER, path=""),
            projects=(),
            repartition_size=2048,
        )
        sql_context = mock.MagicMock()

        doc_dataframe_util = mock.MagicMock()
        doc_dataframe_util.get_dataframe.side_effect = (
            masked_somatic_mutation_df,
            aggregated_somatic_mutation_df,
        )

        return builders.MAFBuilder(
            config, sql_context, doc_dataframe_util, annotation_builders
        )

    def arrange_inputs(
        self, gene_model: Tuple[GeneModel, ...] = (GeneModel(),)
    ) -> Dict[str, sql.DataFrame]:
        gene_model_df = self.spark_session.createDataFrame(
            gene_model, self.gene_model_schema
        )
        maf_metadata_df = mock.MagicMock()

        return {"gene_model_df": gene_model_df, "maf_metadata_df": maf_metadata_df}

    def test__build_from_scratch__joins_succeed(self) -> None:
        inputs = self.arrange_inputs()
        builder = self.arrange_builder()

        result_df = builder.build_from_scratch(**inputs)

        assert result_df.count() == 1
        assert result_df.schema == self.final_maf_schema

    def test__build_from_scratch__masked_somatic_mutation_maf_transformed(self) -> None:
        gene_model = GeneModel()
        maf = MAF()
        inputs = self.arrange_inputs(gene_model=(gene_model,))
        builder = self.arrange_builder(masked_somatic_mutation_mafs=(maf,))

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert_core_maf_transformed(result_row, maf, gene_model)

        assert result_row.normal_bam_uuid == maf.normal_bam_uuid
        assert result_row.tumor_bam_uuid == maf.tumor_bam_uuid
        assert result_row.variant_caller == maf.callers

    def test__build_from_scratch__aggregated_somatic_mutation_maf_transformed(
        self,
    ) -> None:
        gene_model = GeneModel()
        maf = MAF()
        inputs = self.arrange_inputs(gene_model=(gene_model,))
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(), aggregated_somatic_mutation_mafs=(maf,)
        )

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert_core_maf_transformed(result_row, maf, gene_model)

        assert result_row.normal_bam_uuid is None
        assert result_row.tumor_bam_uuid is None
        assert result_row.variant_caller == "FM Simple Somatic Mutation"

    def test__build_from_scratch__cast_str_to_int(self) -> None:
        maf = MAF()
        inputs = self.arrange_inputs()
        builder = self.arrange_builder(masked_somatic_mutation_mafs=(maf,))

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.end_position == int(maf.End_Position)
        assert result_row.start_position == int(maf.Start_Position)
        assert result_row.n_depth == int(maf.n_depth)
        assert result_row.t_alt_count == int(maf.t_alt_count)
        assert result_row.t_depth == int(maf.t_depth)
        assert result_row.t_ref_count == int(maf.t_ref_count)

    @pytest.mark.parametrize(
        ("bool_value", "expected_value"),
        (("True", True), ("False", False), ("", None), (None, None)),
        ids=("true", "false", "empty", "null"),
    )
    def test__build_from_scratch__cast_str_to_bool(
        self, bool_value: Optional[str], expected_value: Optional[bool]
    ) -> None:
        maf = MAF(CANONICAL=bool_value)
        inputs = self.arrange_inputs()
        builder = self.arrange_builder(masked_somatic_mutation_mafs=(maf,))

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.is_canonical == expected_value

    def test__build_from_scratch__joins_fail(self) -> None:
        inputs = self.arrange_inputs((GeneModel(),))
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(MAF(Gene="GENE0"),)
        )

        result_df = builder.build_from_scratch(**inputs)

        assert result_df.count() == 0

    @pytest.mark.parametrize(
        ("tumor_sample_barcode", "case_id", "available_variation_data"),
        (
            ("MBCProject_3808_T1_WES_1", None, ["ssm"]),
            ("MBCProject_3808_T1_WES_1", "case-0", ["ssm"]),
            (None, None, ["ssm"]),
            (None, "case-0", []),
        ),
        ids=(
            "barcode_only",
            "barcode_and_case_id",
            "neither_barcode_nor_case_id",
            "only_case_id",
        ),
    )
    def test__build_from_scratch__available_variation_data_added(
        self,
        tumor_sample_barcode: Optional[str],
        case_id: Optional[str],
        available_variation_data: List[str],
    ) -> None:
        inputs = self.arrange_inputs()
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(
                MAF(Tumor_Sample_Barcode=tumor_sample_barcode, case_id=case_id),
            )
        )

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.available_variation_data == available_variation_data

    @pytest.mark.parametrize(
        ("variant_type", "genomic_dna_change"),
        (
            ("SNP", "chr1:g.1C>A"),
            ("DNP", "chr1:g.1_100delinsA"),
            ("TNP", "chr1:g.1_100delinsA"),
            ("ONP", "chr1:g.1_100delinsA"),
            ("DEL", "chr1:g.1delC"),
            ("INS", "chr1:g.1_100insA"),
            ("OTHER", "1"),
        ),
        ids=("SNP", "DNP", "TNP", "ONP", "DEL", "INS", "OTHER"),
    )
    def test__build_from_scratch__genomic_dna_change(
        self, variant_type: str, genomic_dna_change: str
    ) -> None:
        maf = MAF(
            Variant_Type=variant_type,
            Start_Position="1",
            End_Position="100",
            Allele="A",
            Reference_Allele="C",
            Chromosome="chr1",
        )
        inputs = self.arrange_inputs()
        builder = self.arrange_builder(masked_somatic_mutation_mafs=(maf,))

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.genomic_dna_change == genomic_dna_change

    @pytest.mark.parametrize(
        ("mutation_status", "mutation_type"),
        (
            ("Somatic", "Simple Somatic Mutation"),
            ("Normal", None),
        ),
    )
    def test__build_from_scratch__mutation_type(
        self, mutation_status: str, mutation_type: Optional[str]
    ) -> None:
        maf = MAF(Mutation_Status=mutation_status)
        inputs = self.arrange_inputs()
        builder = self.arrange_builder(masked_somatic_mutation_mafs=(maf,))

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        result_row.mutation_type == mutation_type

    @pytest.mark.parametrize(
        ("variant_type", "mutation_subtype"),
        (
            ("SNP", "Single base substitution"),
            ("DNP", "Di-nucleotide polymorphism"),
            ("TNP", "Tri-nucleotide polymorphism"),
            ("ONP", "Oligo-nucleotide polymorphism"),
            ("DEL", "Small deletion"),
            ("INS", "Small insertion"),
            ("OTHER", None),
        ),
        ids=("SNP", "DNP", "TNP", "ONP", "DEL", "INS", "OTHER"),
    )
    def test__build_from_scratch__mutation_subtype(
        self, variant_type: str, mutation_subtype: str
    ) -> None:
        maf = MAF(
            Variant_Type=variant_type,
        )
        inputs = self.arrange_inputs()
        builder = self.arrange_builder(masked_somatic_mutation_mafs=(maf,))

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.mutation_subtype == mutation_subtype

    def test__build_from_scratch__uuids_generated(self):
        maf = MAF()

        inputs = self.arrange_inputs()
        builder = self.arrange_builder(masked_somatic_mutation_mafs=(maf,))

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())
        ssm_id = utils.generate_uuid5(
            "ssm",
            maf.NCBI_Build,
            maf.Chromosome,
            maf.Start_Position,
            maf.End_Position,
            result_row.mutation_subtype,
            maf.Reference_Allele,
            maf.Allele,
        )

        assert result_row.ssm_id == ssm_id
        assert result_row.occurrence_id == utils.generate_uuid5(
            "ssm_occurrence", ssm_id, maf.case_id
        )

    @pytest.mark.parametrize(
        ("cds_position", "cds_start", "cds_end", "cds_length"),
        (
            ("1273/2112", 1273, 3385, 2112),
            ("1270-1274/2110", 1270, 3380, 2110),
            ("2-?/569", 2, 571, 569),
            ("?-2/569", -1, -1, 569),
            ("", -1, -1, -1),
            (None, -1, -1, -1),
        ),
        ids=(
            "basic",
            "range_start",
            "range_start_with_unknown_end",
            "range_start_with_unknown_start",
            "empty",
            "null",
        ),
    )
    def test__build_from_scratch__cds_lengths(
        self, cds_position: str, cds_start: int, cds_end: int, cds_length: int
    ) -> None:
        inputs = self.arrange_inputs()
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(MAF(CDS_position=cds_position),)
        )

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.cds_start == cds_start
        assert result_row.cds_end == cds_end
        assert result_row.cds_length == cds_length

    @pytest.mark.parametrize(
        ("polyphen", "polyphen_impact", "polyphen_score"),
        (
            ("benign(0.305)", "benign", 0.305),
            ("benign(3)", "benign", 3),
            ("", "", None),
            (None, None, None),
        ),
        ids=("decimal", "whole_number", "empty", "null"),
    )
    def test__build_from_scratch__polyphen_impact_and_score(
        self,
        polyphen: Optional[str],
        polyphen_impact: Optional[str],
        polyphen_score: Optional[float],
    ) -> None:
        inputs = self.arrange_inputs()
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(MAF(PolyPhen=polyphen),)
        )

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.polyphen_impact == polyphen_impact
        assert result_row.polyphen_score == polyphen_score

    @pytest.mark.parametrize(
        ("sift", "sift_impact", "sift_score"),
        (
            ("tolerated(0.12)", "tolerated", 0.12),
            ("tolerated(1)", "tolerated", 1),
            ("", "", None),
            (None, None, None),
        ),
        ids=("decimal", "whole_number", "empty", "null"),
    )
    def test__build_from_scratch__sift_impact_and_score(
        self,
        sift: Optional[str],
        sift_impact: Optional[str],
        sift_score: Optional[float],
    ) -> None:
        inputs = self.arrange_inputs()
        builder = self.arrange_builder(masked_somatic_mutation_mafs=(MAF(SIFT=sift),))

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.sift_impact == sift_impact
        assert result_row.sift_score == sift_score

    def test__build_from_scratch__canonical_transcript_lengths_added(self) -> None:
        canonical_transcript = Transcript(
            length=100, length_cds=30, end=1222, start=1000, is_canonical=True
        )
        other_transcript = Transcript(length=10, length_cds=3, end=122, start=100)
        gene_model = (GeneModel(transcripts=(canonical_transcript, other_transcript)),)

        inputs = self.arrange_inputs(gene_model=gene_model)
        builder = self.arrange_builder()

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.canonical_transcript_length == canonical_transcript.length
        assert (
            result_row.canonical_transcript_length_cds
            == canonical_transcript.length_cds
        )
        assert (
            result_row.canonical_transcript_length_genomic
            == canonical_transcript.end - canonical_transcript.start + 1
        )

    def test__build_from_scratch__canonical_transcript_lengths_no_canonical_transcipt(
        self,
    ) -> None:
        other_transcript = Transcript(length=10, length_cds=3, end=122, start=100)
        gene_model = (GeneModel(transcripts=(other_transcript,)),)

        inputs = self.arrange_inputs(gene_model=gene_model)
        builder = self.arrange_builder()

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.canonical_transcript_length == None
        assert result_row.canonical_transcript_length_cds == None
        assert result_row.canonical_transcript_length_genomic == None

    def test__build_from_scratch__normal_genotype(self) -> None:
        maf = MAF()

        inputs = self.arrange_inputs()
        builder = self.arrange_builder(masked_somatic_mutation_mafs=(maf,))

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.normal_genotype.allele_id == utils.generate_uuid5(
            maf.Match_Norm_Seq_Allele1, maf.Match_Norm_Seq_Allele2
        )

    def test__build_from_scratch__static_fields(self) -> None:
        inputs = self.arrange_inputs()
        builder = self.arrange_builder()

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.variant_process == "masked"
        assert result_row.empty == None

    def test__build_from_scratch__gene_chromosome(self) -> None:
        inputs = self.arrange_inputs()
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(MAF(Chromosome="chr1"),)
        )

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.gene_chromosome == "1"

    def test__build_from_scratch__chromosome(self) -> None:
        inputs = self.arrange_inputs(gene_model=(GeneModel(chromosome="1"),))
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(MAF(Chromosome="chr1"),)
        )

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.chromosome == "chr1"

    @pytest.mark.parametrize(
        ("cosmic", "cosmic_id"),
        (
            ("one;two", ["one", "two"]),
            ("single", ["single"]),
            ("", [""]),
            (None, None),
        ),
        ids=("multiple", "single", "empty", "null"),
    )
    def test__build_from_scratch__cosmic_id(
        self, cosmic: Optional[str], cosmic_id: Optional[List[str]]
    ) -> None:
        inputs = self.arrange_inputs()
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(MAF(COSMIC=cosmic),)
        )

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.cosmic_id == cosmic_id

    def test__build_from_scratch__annotation_builders_called(self) -> None:
        def pass_through(df: sql.DataFrame) -> sql.DataFrame:
            return df

        annotation_builder0 = mock.MagicMock()
        annotation_builder0.merge_with_maf.side_effect = pass_through
        annotation_builder1 = mock.MagicMock()
        annotation_builder1.merge_with_maf.side_effect = pass_through

        inputs = self.arrange_inputs()
        builder = self.arrange_builder(
            annotation_builders=(annotation_builder0, annotation_builder1)
        )

        _ = builder.build_from_scratch(**inputs)

        annotation_builder0.merge_with_maf.assert_called_once()
        annotation_builder1.merge_with_maf.assert_called_once()

    def test__build_from_scratch__strip_domains(self) -> None:
        inputs = self.arrange_inputs()
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(
                MAF(
                    DOMAINS="Gene3D:2.60.40.10;PDB-ENSP_mappings:4l29.b;PDB-ENSP_mappings:4l3c.b;PDB-ENSP_mappings:6nca.a;PDB-ENSP_mappings:6nca.b;PDB-ENSP_mappings:6nca.c;PDB-ENSP_mappings:6nca.d;PDB-ENSP_mappings:6nca.e;PDB-ENSP_mappings:6nca.f;PDB-ENSP_mappings:6nca.g;PDB-ENSP_mappings:6nca.h;PDB-ENSP_mappings:6nca.i;PDB-ENSP_mappings:6nca.j;PDB-ENSP_mappings:6nca.k;PDB-ENSP_mappings:6nca.l;PDB-ENSP_mappings:6nca.m;PDB-ENSP_mappings:6nca.n;PDB-ENSP_mappings:6nca.o;PDB-ENSP_mappings:6nca.p;PDB-ENSP_mappings:6nca.q;PDB-ENSP_mappings:6nca.r;PDB-ENSP_mappings:6nca.s;PDB-ENSP_mappings:6nca.t;Pfam:PF07654;PROSITE_profiles:PS50835;PANTHER:PTHR19944;PANTHER:PTHR19944:SF62;SMART:SM00407;Superfamily:SSF48726;CDD:cd05770"
                ),
            )
        )

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert (
            result_row.domains
            == "Gene3D:2.60.40.10;Pfam:PF07654;PROSITE_profiles:PS50835;PANTHER:PTHR19944;PANTHER:PTHR19944:SF62;SMART:SM00407;Superfamily:SSF48726;CDD:cd05770"
        )
