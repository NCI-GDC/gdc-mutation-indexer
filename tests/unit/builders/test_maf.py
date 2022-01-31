from os import path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from unittest import mock

import attr
import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from exports import builders
from exports.builders import base_input_builder
from tests.unit import utils

DEFAULT_CONFIG_VALUES = {
    "maf_urls": ("fake_url0",),
    "cache_dataframes": {"mafs": False},
    "df_repartition": 2048,
    "debug": False,
}


@attr.s(frozen=True)
class MAF:
    Hugo_Symbol = attr.ib(type=str, default="CSMD2")
    Entrez_Gene_Id = attr.ib(type=str, default="114784")
    Center = attr.ib(type=str, default="BI")
    NCBI_Build = attr.ib(type=str, default="GRCh38")
    Chromosome = attr.ib(type=str, default="chr1")
    Start_Position = attr.ib(type=str, default="33772590")
    End_Position = attr.ib(type=str, default="33772590")
    Strand = attr.ib(type=str, default="+")
    Variant_Classification = attr.ib(type=str, default="Missense_Mutation")
    Variant_Type = attr.ib(type=str, default="SNP")
    Reference_Allele = attr.ib(type=str, default="C")
    Tumor_Seq_Allele1 = attr.ib(type=str, default="C")
    Tumor_Seq_Allele2 = attr.ib(type=str, default="A")
    dbSNP_RS = attr.ib(type=str, default="novel")
    dbSNP_Val_Status = attr.ib(type=Optional[str], default=None)
    Tumor_Sample_Barcode = attr.ib(
        type=Optional[str], default="MBCProject_3808_T1_WES_1"
    )
    Matched_Norm_Sample_Barcode = attr.ib(type=str, default="MBCProject_3808_SALIVA_1")
    Match_Norm_Seq_Allele1 = attr.ib(type=Optional[str], default=None)
    Match_Norm_Seq_Allele2 = attr.ib(type=Optional[str], default=None)
    Tumor_Validation_Allele1 = attr.ib(type=Optional[str], default=None)
    Tumor_Validation_Allele2 = attr.ib(type=Optional[str], default=None)
    Match_Norm_Validation_Allele1 = attr.ib(type=Optional[str], default=None)
    Match_Norm_Validation_Allele2 = attr.ib(type=Optional[str], default=None)
    Verification_Status = attr.ib(type=Optional[str], default=None)
    Validation_Status = attr.ib(type=Optional[str], default=None)
    Mutation_Status = attr.ib(type=str, default="Somatic")
    Sequencing_Phase = attr.ib(type=Optional[str], default=None)
    Sequence_Source = attr.ib(type=Optional[str], default=None)
    Validation_Method = attr.ib(type=Optional[str], default=None)
    Score = attr.ib(type=Optional[str], default=None)
    BAM_File = attr.ib(type=Optional[str], default=None)
    Sequencer = attr.ib(type=str, default="Illumina HiSeq 4000")
    Tumor_Sample_UUID = attr.ib(
        type=str, default="c004a75a-448b-440c-bd8f-46cfc6d8dd2a"
    )
    Matched_Norm_Sample_UUID = attr.ib(
        type=str, default="0e4ad056-bfba-4ff3-a41f-d3655009f544"
    )
    HGVSc = attr.ib(type=str, default="c.1705G>T")
    HGVSp = attr.ib(type=str, default="p.Ala569Ser")
    HGVSp_Short = attr.ib(type=str, default="p.A569S")
    Transcript_ID = attr.ib(type=str, default="ENST00000241312")
    Exon_Number = attr.ib(type=str, default="13/70")
    t_depth = attr.ib(type=str, default="29")
    t_ref_count = attr.ib(type=str, default="24")
    t_alt_count = attr.ib(type=str, default="5")
    n_depth = attr.ib(type=str, default="38")
    n_ref_count = attr.ib(type=Optional[str], default=None)
    n_alt_count = attr.ib(type=Optional[str], default=None)
    all_effects = attr.ib(
        type=str,
        default="CSMD2,missense_variant,p.A609S,ENST00000373381,NM_001281956.2,c.1825G>T,MODERATE,YES,tolerated(0.14),benign(0.305),-1;CSMD2,missense_variant,p.A569S,ENST00000619121,,c.1705G>T,MODERATE,,tolerated(0.13),benign(0.02),-1;CSMD2,missense_variant,p.A569S,ENST00000373388,NM_052896.4,c.1705G>T,MODERATE,,tolerated(0.12),benign(0.305),-1;CSMD2,missense_variant,p.A217S,ENST00000338325,,c.649G>T,MODERATE,,tolerated(0.18),benign(0.264),-1;CSMD2,missense_variant,p.A569S,ENST00000241312,,c.1705G>T,MODERATE,,tolerated(0.12),benign(0.305),-1",
    )
    Allele = attr.ib(type=str, default="A")
    Gene = attr.ib(type=str, default="ENSG00000121904")
    Feature = attr.ib(type=str, default="ENST00000241312")
    Feature_type = attr.ib(type=str, default="Transcript")
    One_Consequence = attr.ib(type=str, default="missense_variant")
    Consequence = attr.ib(type=str, default="missense_variant;NMD_transcript_variant")
    cDNA_position = attr.ib(type=str, default="1734/13108")
    CDS_position = attr.ib(type=str, default="1705/10464")
    Protein_position = attr.ib(type=str, default="569/3487")
    Amino_acids = attr.ib(type=str, default="A/S")
    Codons = attr.ib(type=str, default="Gct/Tct")
    Existing_variation = attr.ib(type=Optional[str], default=None)
    DISTANCE = attr.ib(type=Optional[str], default=None)
    TRANSCRIPT_STRAND = attr.ib(type=str, default="-1")
    SYMBOL = attr.ib(type=str, default="CSMD2")
    SYMBOL_SOURCE = attr.ib(type=str, default="HGNC")
    HGNC_ID = attr.ib(type=str, default="HGNC:19290")
    BIOTYPE = attr.ib(type=str, default="nonsense_mediated_decay")
    CANONICAL = attr.ib(type=Optional[str], default=None)
    CCDS = attr.ib(type=str, default="CCDS380.1")
    ENSP = attr.ib(type=str, default="ENSP00000241312")
    SWISSPROT = attr.ib(type=str, default="Q7Z408.146")
    TREMBL = attr.ib(type=Optional[str], default=None)
    UNIPARC = attr.ib(type=str, default="UPI00004561AB")
    UNIPROT_ISOFORM = attr.ib(type=str, default="Q7Z408-1")
    RefSeq = attr.ib(type=Optional[str], default=None)
    MANE = attr.ib(type=Optional[str], default=None)
    APPRIS = attr.ib(type=Optional[str], default=None)
    FLAGS = attr.ib(type=Optional[str], default=None)
    SIFT = attr.ib(type=Optional[str], default="tolerated(0.12)")
    PolyPhen = attr.ib(type=Optional[str], default="benign(0.305)")
    EXON = attr.ib(type=str, default="13/70")
    INTRON = attr.ib(type=Optional[str], default=None)
    DOMAINS = attr.ib(type=Optional[str], default=None)
    ThousandG_AF = attr.ib(type=Optional[str], default=None)
    ThousandG_AFR_AF = attr.ib(type=Optional[str], default=None)
    ThousandG_AMR_AF = attr.ib(type=Optional[str], default=None)
    ThousandG_EAS_AF = attr.ib(type=Optional[str], default=None)
    ThousandG_EUR_AF = attr.ib(type=Optional[str], default=None)
    ThousandG_SAS_AF = attr.ib(type=Optional[str], default=None)
    ESP_AA_AF = attr.ib(type=Optional[str], default=None)
    ESP_EA_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_AFR_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_AMR_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_ASJ_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_EAS_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_FIN_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_NFE_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_OTH_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_SAS_AF = attr.ib(type=Optional[str], default=None)
    MAX_AF = attr.ib(type=Optional[str], default=None)
    MAX_AF_POPS = attr.ib(type=Optional[str], default=None)
    gnomAD_non_cancer_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_non_cancer_AFR_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_non_cancer_AMI_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_non_cancer_AMR_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_non_cancer_ASJ_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_non_cancer_EAS_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_non_cancer_FIN_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_non_cancer_MID_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_non_cancer_NFE_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_non_cancer_OTH_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_non_cancer_SAS_AF = attr.ib(type=Optional[str], default=None)
    gnomAD_non_cancer_MAX_AF_adj = attr.ib(type=Optional[str], default=None)
    gnomAD_non_cancer_MAX_AF_POPS_adj = attr.ib(type=Optional[str], default=None)
    CLIN_SIG = attr.ib(type=Optional[str], default=None)
    SOMATIC = attr.ib(type=Optional[str], default=None)
    PUBMED = attr.ib(type=Optional[str], default=None)
    TRANSCRIPTION_FACTORS = attr.ib(type=Optional[str], default=None)
    MOTIF_NAME = attr.ib(type=Optional[str], default=None)
    MOTIF_POS = attr.ib(type=Optional[str], default=None)
    HIGH_INF_POS = attr.ib(type=Optional[str], default=None)
    MOTIF_SCORE_CHANGE = attr.ib(type=Optional[str], default=None)
    miRNA = attr.ib(type=Optional[str], default=None)
    IMPACT = attr.ib(type=str, default="MODERATE")
    PICK = attr.ib(type=Optional[str], default=None)
    VARIANT_CLASS = attr.ib(type=str, default="SNV")
    TSL = attr.ib(type=str, default="1")
    HGVS_OFFSET = attr.ib(type=Optional[str], default=None)
    PHENO = attr.ib(type=Optional[str], default=None)
    GENE_PHENO = attr.ib(type=Optional[str], default=None)
    CONTEXT = attr.ib(type=str, default="CTTAGCCGACC")
    tumor_bam_uuid = attr.ib(type=str, default="9fa1ff4d-230d-477b-91d6-e2dc3896b6c4")
    normal_bam_uuid = attr.ib(type=str, default="604c11f1-ab8b-48a7-909e-982e873e02e5")
    case_id = attr.ib(
        type=Optional[str], default="3680a87f-f493-42f0-abf7-741df6a7c9e7"
    )
    GDC_FILTER = attr.ib(type=Optional[str], default=None)
    COSMIC = attr.ib(type=Optional[str], default=None)
    hotspot = attr.ib(type=str, default="N")
    RNA_Support = attr.ib(type=str, default="Unknown")
    RNA_depth = attr.ib(type=Optional[str], default=None)
    RNA_ref_count = attr.ib(type=Optional[str], default=None)
    RNA_alt_count = attr.ib(type=Optional[str], default=None)
    callers = attr.ib(type=str, default="muse;varscan2")

    def to_sql_row(self) -> sql.Row:
        data = attr.asdict(self)

        data["1000G_AF"] = data.pop("ThousandG_AF")
        data["1000G_AFR_AF"] = data.pop("ThousandG_AFR_AF")
        data["1000G_AMR_AF"] = data.pop("ThousandG_AMR_AF")
        data["1000G_EAS_AF"] = data.pop("ThousandG_EAS_AF")
        data["1000G_EUR_AF"] = data.pop("ThousandG_EUR_AF")
        data["1000G_SAS_AF"] = data.pop("ThousandG_SAS_AF")

        return sql.Row(**data)


@attr.s(frozen=True)
class Domain:
    description = attr.ib(
        type=str, default="G protein-coupled receptor, rhodopsin-like"
    )
    end = attr.ib(type=int, default=280)
    gff_source = attr.ib(type=str, default="pfam")
    hit_name = attr.ib(type=str, default="PF00001")
    interpro_id = attr.ib(type=str, default="IPR000276")
    start = attr.ib(type=int, default=34)


@attr.s(frozen=True)
class Exon:
    cdna_coding_end = attr.ib(type=int, default=0)
    cdna_coding_start = attr.ib(type=int, default=0)
    cdna_end = attr.ib(type=int, default=359)
    cdna_start = attr.ib(type=int, default=1)
    end = attr.ib(type=int, default=12227)
    end_phase = attr.ib(type=int, default=-1)
    genomic_coding_end = attr.ib(type=int, default=0)
    genomic_coding_stairt = attr.ib(type=int, default=0)
    genomic_coding_start = attr.ib(type=int, default=0)
    start = attr.ib(type=int, default=11869)
    start_phase = attr.ib(type=int, default=-1)


@attr.s(frozen=True)
class Transcript:
    biotype = attr.ib(type=str, default="processed_transcript")
    cdna_coding_end = attr.ib(type=int, default=0)
    cdna_coding_start = attr.ib(type=int, default=0)
    coding_region_end = attr.ib(type=int, default=0)
    coding_region_start = attr.ib(type=int, default=0)
    domains = attr.ib(type=Tuple[Domain, ...], default=(Domain(),))
    end = attr.ib(type=int, default=14409)
    end_exon = attr.ib(type=Optional[int], default=None)
    exons = attr.ib(type=Tuple[Exon, ...], default=(Exon(),))
    transcript_id = attr.ib(type=str, default="ENST00000456328")
    is_canonical = attr.ib(type=bool, default=False)
    length = attr.ib(type=int, default=1657)
    length_amino_acid = attr.ib(type=Optional[int], default=None)
    length_cds = attr.ib(type=Optional[int], default=None)
    name = attr.ib(type=str, default="DDX11L1-002")
    number_of_exons = attr.ib(type=int, default=6)
    seq_exon_end = attr.ib(type=Optional[int], default=None)
    seq_exon_start = attr.ib(type=Optional[int], default=None)
    start = attr.ib(type=int, default=11869)
    start_exon = attr.ib(type=Optional[int], default=None)
    translation_id = attr.ib(type=Optional[str], default=None)


@attr.s(frozen=True)
class GeneModel:
    _gene_id = attr.ib(type=str, default="ENSG00000121904")
    _id = attr.ib(
        type=Dict[str, str],
        default=attr.Factory(lambda: {"$oid": "589c87ca0ef75875ed614a40"}),
    )
    biotype = attr.ib(type=str, default="transcribed_unprocessed_pseudogene")
    canonical_transcript_id = attr.ib(type=str, default="ENST00000456328")
    chromosome = attr.ib(type=str, default="1")
    cytoband = attr.ib(type=Tuple[Optional[str], ...], default=("1p36.33",))
    description = attr.ib(
        type=str,
        default="DISCONTINUED: This record has been withdrawn by NCBI because the model on which it was based was not predicted in a later annotation.",
    )
    entrez_gene = attr.ib(
        type=Tuple[str, ...], default=("100287596", "100287102", "727856", "84771")
    )
    gene_end = attr.ib(type=int, default=14409)
    gene_start = attr.ib(type=int, default=11869)
    gene_strand = attr.ib(type=int, default=1)
    hgnc = attr.ib(type=Tuple[str, ...], default=("HGNC:37102",))
    is_cancer_gene_census = attr.ib(type=str, default="true")
    omim_gene = attr.ib(type=Tuple[str, ...], default=())
    uniprotkb_swissprot = attr.ib(type=Tuple[str, ...], default=())
    name = attr.ib(
        type=str, default="DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
    )
    symbol = attr.ib(type=str, default="DDX11L1")
    synonyms = attr.ib(type=Tuple[str, ...], default=())
    transcripts = attr.ib(type=Tuple[Transcript, ...], default=(Transcript(),))


@pytest.fixture(scope="class")
def schema_dir(data_dir: str) -> str:
    return path.join(data_dir, "schemas", "builders", "maf")


@pytest.fixture(scope="class")
def gene_model_schema(schema_dir: str) -> types.StructType:
    return utils.load_schema(schema_dir, "input_gene_model.json")


@pytest.fixture(scope="class")
def raw_maf_schema(schema_dir: str) -> types.StructType:
    return utils.load_schema(schema_dir, "raw_maf.json")


def arrange_config(config_values: Optional[Dict[str, Any]]) -> mock.MagicMock:
    values = dict(DEFAULT_CONFIG_VALUES)

    values.update(config_values or {})

    return mock.MagicMock(**values)


class TestMAFBuilder:
    @pytest.fixture(autouse=True)
    def load_fixtures(
        self,
        spark_session: sql.SparkSession,
        gene_model_schema: types.StructType,
        raw_maf_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.gene_model_schema = gene_model_schema
        self.raw_maf_schema = raw_maf_schema

    def arrange_builder(
        self,
        mafs: Tuple[MAF, ...] = (MAF(),),
        config_values: Optional[Dict[str, Any]] = None,
        annotation_builders: Iterable[mock.MagicMock] = (),
        drop_optional_cols: bool = False,
    ) -> base_input_builder.BaseInputBuilder:
        maf_df = self.spark_session.createDataFrame(
            tuple(maf.to_sql_row() for maf in mafs), self.raw_maf_schema
        )

        if drop_optional_cols:
            maf_df = maf_df.drop("callers", "normal_bam_uuid", "tumor_bam_uuid")

        config = arrange_config(config_values)
        sql_context = mock.MagicMock()
        sql_context.read = sql_context
        sql_context.format.return_value = sql_context
        sql_context.options.return_value = sql_context
        sql_context.load.return_value = maf_df

        return builders.MAFBuilder(config, sql_context, annotation_builders)

    def arrange_inputs(
        self, gene_model: Tuple[GeneModel, ...] = (GeneModel(),)
    ) -> Dict[str, sql.DataFrame]:
        gene_model_df = self.spark_session.createDataFrame(
            gene_model, self.gene_model_schema
        )

        return {"gene_model_df": gene_model_df}

    def test__build_from_scratch__joins_succeed(self) -> None:
        inputs = self.arrange_inputs()
        builder = self.arrange_builder()

        result_df = builder.build_from_scratch(**inputs)

        assert result_df.count() == 1

    def test__build_from_scratch__input_schema_transformed(self) -> None:
        gene_model = GeneModel()
        maf = MAF()
        inputs = self.arrange_inputs(gene_model=(gene_model,))
        builder = self.arrange_builder(mafs=(maf,))

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row._id.asDict() == gene_model._id
        assert result_row.aa_change == maf.ESP_AA_AF
        assert result_row.aa_end == maf.ESP_AA_AF
        assert result_row.aa_start == maf.ESP_AA_AF
        assert result_row.all_effects == maf.all_effects
        assert result_row.amino_acids == maf.Amino_acids
        assert result_row.biotype == gene_model.biotype
        assert result_row.canonical_transcript_id == gene_model.canonical_transcript_id
        assert result_row.case_id == maf.case_id
        assert result_row.ccds == maf.CCDS
        assert result_row.cdna_position == maf.cDNA_position
        assert result_row.cds_position == maf.CDS_position
        assert result_row.center == maf.Center
        assert result_row.clin_sig == maf.CLIN_SIG
        assert result_row.codons == maf.Codons
        assert result_row.consequence_type == maf.Consequence
        assert tuple(result_row.cytoband) == gene_model.cytoband
        assert result_row.dbsnp_rs == maf.dbSNP_RS
        assert result_row.dbsnp_val_status == maf.dbSNP_Val_Status
        assert result_row.description == gene_model.description
        assert result_row.domains == maf.DOMAINS
        assert result_row.ensp == maf.ENSP
        assert tuple(result_row.entrez_gene) == gene_model.entrez_gene
        assert result_row.existing_variation == maf.Existing_variation
        assert result_row.gene_end == gene_model.gene_end
        assert result_row.gene_id == maf.Gene
        assert result_row.gene_start == gene_model.gene_start
        assert result_row.gene_strand == gene_model.gene_strand
        assert tuple(result_row.hgnc) == gene_model.hgnc
        assert result_row.hgvsc == maf.HGVSc
        assert result_row.hgvsp == maf.HGVSp
        assert result_row.hgvsp_short == maf.HGVSp_Short
        assert result_row.is_cancer_gene_census == gene_model.is_cancer_gene_census
        assert result_row.match_norm_seq_allele1 == maf.Match_Norm_Seq_Allele1
        assert result_row.match_norm_seq_allele2 == maf.Match_Norm_Seq_Allele2
        assert result_row.matched_norm_sample_barcode == maf.Matched_Norm_Sample_Barcode
        assert result_row.matched_norm_sample_uuid == maf.Matched_Norm_Sample_UUID
        assert result_row.mutation_status == maf.Mutation_Status
        assert result_row.name == gene_model.name
        assert result_row.ncbi_build == maf.NCBI_Build
        assert result_row.normal_bam_uuid == maf.normal_bam_uuid
        assert tuple(result_row.omim_gene) == gene_model.omim_gene
        assert result_row.protein_position == maf.Protein_position
        assert result_row.pubmed == maf.PUBMED
        assert result_row.ref_seq_accession == maf.ESP_AA_AF
        assert result_row.reference_allele == maf.Reference_Allele
        assert result_row.swissprot == maf.SWISSPROT
        assert result_row.symbol == gene_model.symbol
        assert tuple(result_row.synonyms) == gene_model.synonyms
        assert result_row.transcript_id == maf.Transcript_ID
        assert result_row.trembl == maf.TREMBL
        assert result_row.tumor_allele == maf.Allele
        assert result_row.tumor_bam_uuid == maf.tumor_bam_uuid
        assert result_row.tumor_sample_barcode == maf.Tumor_Sample_Barcode
        assert result_row.tumor_sample_uuid == maf.Tumor_Sample_UUID
        assert result_row.tumor_seq_allele1 == maf.Tumor_Seq_Allele1
        assert result_row.tumor_seq_allele2 == maf.Tumor_Seq_Allele2
        assert result_row.tumor_validation_allele1 == maf.Tumor_Validation_Allele1
        assert result_row.tumor_validation_allele2 == maf.Tumor_Validation_Allele2
        assert result_row.uniparc == maf.UNIPARC
        assert tuple(result_row.uniprotkb_swissprot) == gene_model.uniprotkb_swissprot
        assert result_row.validation_method == maf.Validation_Method
        assert result_row.variant_caller == maf.callers
        assert result_row.variant_type == maf.Variant_Type
        assert result_row.vep_impact == maf.IMPACT

        result_transcripts = tuple(
            transcript.asDict(recursive=True) for transcript in result_row.transcripts
        )
        expected_transcripts = tuple(
            attr.asdict(transcript, recurse=True)
            for transcript in gene_model.transcripts
        )

        assert len(result_transcripts) == len(expected_transcripts)

        for result_transcript, expected_transcript in zip(
            result_transcripts, expected_transcripts
        ):
            assert result_transcript == expected_transcript

    def test__build_from_scratch__cast_str_to_int(self) -> None:
        maf = MAF()
        inputs = self.arrange_inputs()
        builder = self.arrange_builder(mafs=(maf,))

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
        builder = self.arrange_builder(mafs=(maf,))

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.is_canonical == expected_value

    def test__build_from_scratch__joins_fail(self) -> None:
        inputs = self.arrange_inputs((GeneModel(),))
        builder = self.arrange_builder(mafs=(MAF(Gene="GENE0"),))

        result_df = builder.build_from_scratch(**inputs)

        assert result_df.count() == 0

    def test__build_from_scratch__urls_is_none_raises_exception(self) -> None:
        inputs = self.arrange_inputs()
        builder = self.arrange_builder(config_values={"maf_urls": None})

        with pytest.raises(Exception):
            builder.build_from_scratch(**inputs)

    def test__build_from_scratch__urls_is_empty_raises_assertion_exception(
        self,
    ) -> None:
        inputs = self.arrange_inputs()
        builder = self.arrange_builder(config_values={"maf_urls": ()})

        with pytest.raises(AssertionError):
            builder.build_from_scratch(**inputs)

    def test__build_from_scratch__multiple_urls_unioned(self) -> None:
        inputs = self.arrange_inputs()
        builder = self.arrange_builder(
            config_values={"maf_urls": ("fake_url0", "fake_url1")}
        )

        result_df = builder.build_from_scratch(**inputs)
        result_rows = result_df.collect()

        assert len(result_rows) == 2
        assert result_rows[0] == result_rows[1]

    def test__build_from_scratch__debug_true(self) -> None:
        inputs = self.arrange_inputs()
        builder = self.arrange_builder(config_values={"debug": True})
        config = builder.config

        _ = builder.build_from_scratch(**inputs)

        assert config.nb_mutations == 1

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
            mafs=(MAF(Tumor_Sample_Barcode=tumor_sample_barcode, case_id=case_id),)
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
        builder = self.arrange_builder(mafs=(maf,))

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
        builder = self.arrange_builder(mafs=(maf,))

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
        builder = self.arrange_builder(mafs=(maf,))

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.mutation_subtype == mutation_subtype

    def test__build_from_scratch__uuids_generated(self):
        maf = MAF()

        inputs = self.arrange_inputs()
        builder = self.arrange_builder(mafs=(maf,))

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
        builder = self.arrange_builder(mafs=(MAF(CDS_position=cds_position),))

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
        builder = self.arrange_builder(mafs=(MAF(PolyPhen=polyphen),))

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
        builder = self.arrange_builder(mafs=(MAF(SIFT=sift),))

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
        builder = self.arrange_builder(mafs=(maf,))

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
        builder = self.arrange_builder(mafs=(MAF(Chromosome="chr1"),))

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.gene_chromosome == "1"

    def test__build_from_scratch__chromosome(self) -> None:
        inputs = self.arrange_inputs(gene_model=(GeneModel(chromosome="1"),))
        builder = self.arrange_builder(mafs=(MAF(Chromosome="chr1"),))

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
        builder = self.arrange_builder(mafs=(MAF(COSMIC=cosmic),))

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
            mafs=(
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

    def test__build_from_scratch__default_columns(self) -> None:
        inputs = self.arrange_inputs()
        builder = self.arrange_builder(
            mafs=(
                MAF(callers="test", normal_bam_uuid="value0", tumor_bam_uuid="value1"),
            ),
            drop_optional_cols=True,
        )

        result_df = builder.build_from_scratch(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.variant_caller == "FM Simple Somatic Mutation"
        assert result_row.normal_bam_uuid is None
        assert result_row.tumor_bam_uuid is None
