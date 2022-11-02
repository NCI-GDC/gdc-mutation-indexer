import dataclasses
from typing import Dict, Optional, Tuple

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from exports import builders
from tests.unit.data import schemas


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
class Allele:
    allele_id: str = "03b61092-4545-526e-9b39-fc8005c40af5"


@dataclasses.dataclass(frozen=True)
class MAF:
    _id: Dict[str, str] = dataclasses.field(
        default_factory=lambda: {"$oid": "589c87ca0ef75875ed614a40"}
    )
    aa_change = None
    aa_end = None
    aa_start = None
    all_effects: str = "CSMD2,missense_variant,p.A609S,ENST00000373381,NM_001281956.2,c.1825G>T,MODERATE,YES,tolerated(0.14),benign(0.305),-1;CSMD2,missense_variant,p.A569S,ENST00000619121,,c.1705G>T,MODERATE,,tolerated(0.13),benign(0.02),-1;CSMD2,missense_variant,p.A569S,ENST00000373388,NM_052896.4,c.1705G>T,MODERATE,,tolerated(0.12),benign(0.305),-1;CSMD2,missense_variant,p.A217S,ENST00000338325,,c.649G>T,MODERATE,,tolerated(0.18),benign(0.264),-1;CSMD2,missense_variant,p.A569S,ENST00000241312,,c.1705G>T,MODERATE,,tolerated(0.12),benign(0.305),-1"
    amino_acids: str = "A/S"
    available_variation_data: Tuple[str, ...] = ("ssm",)
    biotype: str = "transcribed_unprocessed_pseudogene"
    canonical_transcript_id: str = "ENST00000456328"
    canonical_transcript_length: Optional[int] = None
    canonical_transcript_length_cds: Optional[int] = None
    canonical_transcript_length_genomic: Optional[int] = None
    case_id: str = "3680a87f-f493-42f0-abf7-741df6a7c9e7"
    ccds: str = "CCDS380.1"
    cdna_position: str = "1734/13108"
    cds_end: int = 12169
    cds_length: int = 10464
    cds_position: int = "1705/10464"
    cds_start: int = 1705
    center: str = "BI"
    chromosome: str = "chr1"
    clin_sig: Optional[str] = None
    codons: str = "Gct/Tct"
    consequence_type: str = "missense_variant;NMD_transcript_variant"
    cosmic_id: Optional[str] = None
    cytoband: Tuple[str, ...] = ("1p36.33",)
    dbsnp_rs: str = "novel"
    dbsnp_val_status: Optional[str] = None
    description: str = "DISCONTINUED: This record has been withdrawn by NCBI because the model on which it was based was not predicted in a later annotation."
    domains: Optional[str] = None
    empty: None = None
    end_position: int = 33772590
    ensp: str = "ENSP00000241312"
    entrez_gene: Tuple[str, ...] = ("100287596", "100287102", "727856", "84771")
    existing_variation: Optional[str] = None
    gene_chromosome: str = "1"
    gene_end: int = 14409
    gene_id: str = "ENSG00000121904"
    gene_start: int = 11869
    gene_strand: int = 1
    genomic_dna_change: str = "chr1:g.33772590C>A"
    hgnc: Tuple[str, ...] = ("HGNC:37102",)
    hgvsc: str = "c.1705G>T"
    hgvsp: str = "p.Ala569Ser"
    hgvsp_short: str = "p.A569S"
    is_cancer_gene_census: str = "true"
    is_canonical: Optional[bool] = None
    match_norm_seq_allele1: Optional[str] = None
    match_norm_seq_allele2: Optional[str] = None
    matched_norm_sample_barcode: str = "MBCProject_3808_SALIVA_1"
    matched_norm_sample_uuid: str = "0e4ad056-bfba-4ff3-a41f-d3655009f544"
    mutation_status: str = "Somatic"
    mutation_subtype: str = "Single base substitution"
    mutation_type: str = "Simple Somatic Mutation"
    n_depth: int = 38
    name: str = "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
    ncbi_build: str = "GRCh38"
    normal_bam_uuid: str = "604c11f1-ab8b-48a7-909e-982e873e02e5"
    normal_genotype: Allele = Allele()
    occurrence_id: str = "1746a06f-2052-5fec-8150-8da04c939ee4"
    omim_gene: Tuple[str, ...] = ()
    polyphen_impact: str = "benign"
    polyphen_score: float = 0.305
    protein_position: str = "569/3487"
    pubmed: Optional[str] = None
    ref_seq_accession: Optional[str] = None
    reference_allele: str = "C"
    sift_impact: str = "tolerated"
    sift_score: float = 0.12
    ssm_id: str = "d19178f1-c785-5c52-9e2b-26e106e03853"
    start_position: int = 33772590
    swissprot: str = "Q7Z408.146"
    symbol: str = "DDX11L1"
    synonyms: Tuple[str, ...] = ()
    t_alt_count: int = 5
    t_depth: int = 29
    t_ref_count: int = 24
    transcript_id: str = "ENST00000241312"
    transcripts: Tuple[Transcript, ...] = (Transcript(),)
    trembl: Optional[str] = None
    tumor_allele: str = "A"
    tumor_bam_uuid: str = "9fa1ff4d-230d-477b-91d6-e2dc3896b6c4"
    tumor_sample_barcode: str = "MBCProject_3808_T1_WES_1"
    tumor_sample_uuid: str = "c004a75a-448b-440c-bd8f-46cfc6d8dd2a"
    tumor_seq_allele1: str = "C"
    tumor_seq_allele2: str = "A"
    tumor_validation_allele1: Optional[str] = None
    tumor_validation_allele2: Optional[str] = None
    uniparc: str = "UPI00004561AB"
    uniprotkb_swissprot: Tuple[str, ...] = ()
    validation_method: Optional[str] = None
    variant_caller: str = "muse;varscan2"
    variant_process: str = "masked"
    variant_type: str = "SNP"
    vep_impact: str = "MODERATE"


@dataclasses.dataclass(frozen=True)
class PrimaryAliquot:
    entity: str = "case"
    entity_id: str = "case-0"
    case_id: str = "case-0"
    file_id: str = "file-0"
    alqiuot_id: str = "aliquot-0"
    experiemental_strategy: str = "WXS"


@dataclasses.dataclass(frozen=True)
class ASCAT:
    _id: Optional[dict] = dataclasses.field(
        default_factory=lambda: {"$oid": "589c87ca0ef75875ed614a40"}
    )
    aliquot_id: Optional[str] = "aliquot-0"
    available_variation_data: Optional[str] = "cnv"
    biotype: Optional[str] = "protein_coding"
    canonical_transcript_id: Optional[str] = "ENST00000456328"
    canonical_transcript_length: Optional[int] = None
    canonical_transcript_length_cds: Optional[int] = None
    canonical_transcript_length_genomic: Optional[int] = None
    case_id: Optional[str] = "case-0"
    chromosome: Optional[str] = "1"
    cnv_change: Optional[str] = "Gain"
    cnv_id: Optional[str] = "e9ea684a-d522-5baf-9b1a-a515b589cb7f"
    consequence_id: Optional[str] = "377b6f05-34e8-51d0-81a6-3a8781032253"
    cytoband: Optional[Tuple[str, ...]] = ("1p36.33",)
    description: Optional[
        str
    ] = "DISCONTINUED: This record has been withdrawn by NCBI because the model on which it was based was not predicted in a later annotation."
    end_position: Optional[int] = 14409
    entrez_gene: Optional[Tuple[str, ...]] = (
        "100287596",
        "100287102",
        "727856",
        "84771",
    )
    gene_chromosome: Optional[str] = "1"
    gene_end: Optional[int] = 14409
    gene_id: Optional[str] = "ENSG00000238009"
    gene_level_cn: Optional[bool] = True
    gene_start: Optional[int] = 11869
    gene_strand: Optional[int] = 1
    hgnc: Optional[Tuple[str, ...]] = ("HGNC:37102",)
    is_cancer_gene_census: Optional[str] = "true"
    name: Optional[str] = "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
    ncbi_build: Optional[str] = "GRCh38"
    observation_id: Optional[str] = "b1627f65-d28b-568c-9f76-1a24bd4fe82d"
    occurrence_id: Optional[str] = "2d7b55e0-9122-5a30-9a42-81c06fe5183f"
    omim_gene: Optional[Tuple[str, ...]] = ()
    start_position: Optional[int] = 11869
    symbol: Optional[str] = "DDX11L1"
    synonyms: Optional[Tuple[str, ...]] = ()
    transcripts: Optional[Tuple[Transcript, ...]] = (Transcript(),)
    uniprotkb_swissprot: Optional[Tuple[str]] = ()
    variant_caller: Optional[str] = "ASCAT"
    variant_status: Optional[str] = "Tumor Only"
    civic_gene_id: str = "1"
    civic_variant_id: str = "3"


@pytest.fixture(scope="class")
def maf_schema() -> types.StructType:
    return schemas.load_schema("builders/observation/input_maf.yaml")


@pytest.fixture(scope="class")
def ascat_schema() -> types.StructType:
    return schemas.load_schema("builders/observation/input_ascat.json")


@pytest.fixture(scope="class")
def primary_aliquot_schema() -> types.StructType:
    return schemas.load_schema("builders/observation/input_primary_aliquot.json")


@pytest.fixture(scope="class")
def ssm_observation_schema() -> types.StructType:
    return schemas.load_schema("builders/observation/final_ssm_observation.json")


@pytest.fixture(scope="class")
def other_ssm_observation_schema() -> types.StructType:
    return schemas.load_schema("builders/observation/final_other_ssm_observation.json")


@pytest.fixture(scope="class")
def cnv_observation_schema() -> types.StructType():
    return schemas.load_schema("builders/observation/final_cnv_observation.yaml")


class TestObservationBuilder:
    @pytest.fixture(autouse=True)
    def fixture_set_up(
        self,
        spark_session: sql.SparkSession,
        maf_schema: types.StructType,
        ascat_schema: types.StructType,
        primary_aliquot_schema: types.StructType,
        ssm_observation_schema: types.StructType,
        other_ssm_observation_schema: types.StructType,
        cnv_observation_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.maf_schema = maf_schema
        self.ascat_schema = ascat_schema
        self.primary_aliquot_schema = primary_aliquot_schema
        self.ssm_schemas = {
            "ssm": ssm_observation_schema,
            "other": other_ssm_observation_schema,
        }
        self.cnv_observation_schema = cnv_observation_schema

    def arrange_maf_df(self, mafs: Tuple[MAF, ...] = (MAF(),)) -> sql.DataFrame:
        return self.spark_session.createDataFrame(mafs, schema=self.maf_schema)

    def arrange_ascat_df(self, ascats: Tuple[ASCAT, ...] = (ASCAT(),)) -> sql.DataFrame:
        return self.spark_session.createDataFrame(ascats, schema=self.ascat_schema)

    def arrange_primary_aliquot_df(
        self, primary_aliquots: Tuple[PrimaryAliquot, ...] = (PrimaryAliquot(),)
    ) -> sql.DataFrame:
        return self.spark_session.createDataFrame(
            primary_aliquots, schema=self.primary_aliquot_schema
        )

    def arrange_builder(self) -> builders.ObservationBuilder:
        return builders.ObservationBuilder()

    @pytest.mark.parametrize(
        ("index_name", "selector", "final_schema"),
        (
            pytest.param("case_centric", "ssm", "other", id="case_centric"),
            pytest.param("gene_centric", "ssm", "other", id="gene_centric"),
            pytest.param("ssm_centric", None, "ssm", id="ssm_centric"),
            pytest.param(
                "ssm_occurrence_centric", None, "ssm", id="ssm_occurrence_centric"
            ),
        ),
    )
    def test__build_for_ssm__final_schema(
        self, index_name: str, selector: Optional[str], final_schema: str
    ) -> None:
        maf_df = self.arrange_maf_df()
        primary_aliquot_df = self.arrange_primary_aliquot_df()
        builder = self.arrange_builder()

        result_df = builder.build_for_ssm(
            maf_df, primary_aliquot_df, index_name, selector
        )

        assert result_df.count() == 1
        assert result_df.schema == self.ssm_schemas[final_schema]

    def test__build_for_ssm__caller_split(self) -> None:
        maf_df = self.arrange_maf_df((MAF(variant_caller="muse;varscan2"),))
        primary_aliquot_df = self.arrange_primary_aliquot_df()
        builder = self.arrange_builder()

        result_df = builder.build_for_ssm(
            maf_df, primary_aliquot_df, "case_centric", "ssm"
        )
        result_row = more_itertools.one(result_df.collect())

        assert len(result_row.observation) == 2
        assert not frozenset(
            observation.variant_calling.variant_caller
            for observation in result_row.observation
        ) ^ frozenset(("muse", "varscan2"))

    def test__build_for_ssm__caller_stripped(self) -> None:
        maf_df = self.arrange_maf_df((MAF(variant_caller="***varscan2**"),))
        primary_aliquot_df = self.arrange_primary_aliquot_df()
        builder = self.arrange_builder()

        result_df = builder.build_for_ssm(
            maf_df, primary_aliquot_df, "case_centric", "ssm"
        )
        result_row = more_itertools.one(result_df.collect())
        result_observation = more_itertools.one(result_row.observation)

        assert result_observation.variant_calling.variant_caller == "varscan2"

    def test__build_for_ssm__filter_somatic_snipper(self) -> None:
        maf_df = self.arrange_maf_df((MAF(variant_caller="muse;somaticsniper"),))
        primary_aliquot_df = self.arrange_primary_aliquot_df()
        builder = self.arrange_builder()

        result_df = builder.build_for_ssm(
            maf_df, primary_aliquot_df, "case_centric", "ssm"
        )
        result_row = more_itertools.one(result_df.collect())

        assert not any(
            observation.variant_calling.variant_caller == "somaticsniper"
            for observation in result_row.observation
        )

    @pytest.mark.parametrize(
        ("index", "selector"),
        (
            pytest.param("case_centric", "cnv", id="case_centric"),
            pytest.param("cnv_centric", None, id="cnv_centric"),
            pytest.param("cnv_occurrence_centric", None, id="cnv_occurrence_centric"),
            pytest.param("gene_centric", "cnv", id="gene_centric"),
        ),
    )
    def test__build_for_cnv__final_schema(
        self, index: str, selector: Optional[str]
    ) -> None:
        ascat_df = self.arrange_ascat_df()
        builder = self.arrange_builder()

        result_df = builder.build_for_cnv(ascat_df, index, selector)

        assert result_df.count() == 1
        assert result_df.schema == self.cnv_observation_schema

    def test__build_for_cnv__data_translated(self) -> None:
        ascat = ASCAT()
        ascat_df = self.arrange_ascat_df((ascat,))
        builder = self.arrange_builder()

        result_df = builder.build_for_cnv(ascat_df, "cnv_centric")
        result_row = more_itertools.one(result_df.collect())
        result_observation = more_itertools.one(result_row.observation)

        assert result_row.cnv_id == ascat.cnv_id
        assert result_row.case_id == ascat.case_id
        assert result_row.occurrence_id == ascat.occurrence_id
        assert result_observation.observation_id == ascat.observation_id
        assert result_observation.variant_calling.variant_caller == ascat.variant_caller
        assert result_observation.variant_status == ascat.variant_status

    def test__build_for_cnv__observation_grouped_by_cnv_case_occurence_ids(
        self,
    ) -> None:
        base_group = ("cnv-0", "case-0", "occ-0")
        cnv_group = ("cnv-1", "case-0", "occ-0")
        case_group = ("cnv-0", "case-1", "occ-0")
        occ_group = ("cnv-0", "case-0", "occ-1")
        ascats = (
            ASCAT(
                cnv_id="cnv-0",
                case_id="case-0",
                occurrence_id="occ-0",
                variant_status="other-0",
            ),
            ASCAT(
                cnv_id="cnv-0",
                case_id="case-0",
                occurrence_id="occ-0",
                variant_status="other-1",
            ),
            ASCAT(cnv_id="cnv-1", case_id="case-0", occurrence_id="occ-0"),
            ASCAT(cnv_id="cnv-0", case_id="case-1", occurrence_id="occ-0"),
            ASCAT(cnv_id="cnv-0", case_id="case-0", occurrence_id="occ-1"),
        )
        ascat_df = self.arrange_ascat_df(ascats)
        builder = self.arrange_builder()

        result_df = builder.build_for_cnv(ascat_df, "cnv_centric")
        result_rows = {
            (r.cnv_id, r.case_id, r.occurrence_id): r for r in result_df.collect()
        }

        assert len(result_rows) == 4
        assert base_group in result_rows
        assert cnv_group in result_rows
        assert case_group in result_rows
        assert occ_group in result_rows
        assert len(result_rows[base_group].observation) == 2
        assert len(result_rows[cnv_group].observation) == 1
        assert len(result_rows[case_group].observation) == 1
        assert len(result_rows[occ_group].observation) == 1

    def test__build_for_cnv__observation_aggregates_no_duplicates(self) -> None:
        ascats = (
            ASCAT(
                cnv_id="cnv-0",
                case_id="case-0",
                occurrence_id="occ-0",
                variant_status="other-0",
            ),
            ASCAT(
                cnv_id="cnv-0",
                case_id="case-0",
                occurrence_id="occ-0",
                variant_status="other-0",
            ),
        )
        ascat_df = self.arrange_ascat_df(ascats)
        builder = self.arrange_builder()

        result_df = builder.build_for_cnv(ascat_df, "cnv_centric")
        result_rows = result_df.collect()

        assert len(result_rows) == 1
