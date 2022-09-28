import dataclasses
import decimal
from typing import Dict, Iterable, List, Optional, Tuple

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from exports import builders
from tests.unit import utils
from tests.unit.data.schemas import viz_schemas

DECIMAL_CONTEXT = decimal.Context(prec=5)


def _convert_to_decimal(value: Optional[float]) -> Optional[decimal.Decimal]:
    if value is None:
        return None

    return DECIMAL_CONTEXT.create_decimal_from_float(value)


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
    clin_sig: Optional[str] = "ss"
    codons: str = "Gct/Tct"
    consequence_type: str = "missense_variant;NMD_transcript_variant"
    cosmic_id: Optional[str] = None
    cytoband: Tuple[str, ...] = ("1p36.33",)
    dbsnp_rs: str = "novel"
    dbsnp_val_status: Optional[str] = None
    description: str = "DISCONTINUED: This record has been withdrawn by NCBI because the model on which it was based was not predicted in a later annotation."
    domains: Optional[str] = "dfjks;sksk"
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
    symbol: str = "CSMD2"
    synonyms: Tuple[str, ...] = ()
    t_alt_count: int = 5
    t_depth: int = 29
    t_ref_count: int = 24
    transcript_id: str = "ENST00000241312"
    transcripts: Tuple[Transcript, ...] = (Transcript(),)
    trembl: Optional[str] = "DKD"
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
class AllEffects:
    do_not_use: Optional[str] = "CSMD2"
    consequence_type: Optional[str] = "missense_variant"
    aa_change: Optional[str] = "p.A609S"
    transcript_id: Optional[str] = "ENST00000373381"
    ref_seq_accession: Optional[str] = "NM_001281956.2"
    hgvsc: Optional[str] = "c.1825G>T"
    vep_impact: Optional[str] = "MODERATE"
    is_canonical: Optional[str] = "YES"
    sift: Optional[str] = "tolerated(0.14)"
    polyphen: Optional[str] = "benign(0.305)"
    transcript_strand: Optional[str] = "-1"

    def __str__(self) -> str:
        return ",".join(
            map(
                lambda x: str(x) if x else "",
                (
                    self.do_not_use,
                    self.consequence_type,
                    self.aa_change,
                    self.transcript_id,
                    self.ref_seq_accession,
                    self.hgvsc,
                    self.vep_impact,
                    self.is_canonical,
                    self.sift,
                    self.polyphen,
                    self.transcript_strand,
                ),
            )
        )


@pytest.fixture(scope="class")
def maf_schema() -> types.StructType:
    return viz_schemas.MAF.FINAL.load_schema()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return viz_schemas.Consequence.FINAL.load_schema()


@pytest.fixture(scope="class")
def final_with_genes_schema() -> types.StructType:
    return viz_schemas.Consequence.FINAL_WITH_GENES.load_schema()


@pytest.fixture(scope="class")
def final_with_aa_change_schema() -> types.StructType:
    return viz_schemas.Consequence.FINALWITH_AA_CHANGE.load_schema()


class TestConsequenceBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        spark_session: sql.SparkSession,
        maf_schema: types.StructType,
        final_schema: types.StructType,
        final_with_genes_schema: types.StructType,
        final_with_aa_change_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.maf_schema = maf_schema
        self.final_schemas = {
            "case_centric": final_schema,
            "gene_centric": final_schema,
            "ssm_centric": final_with_aa_change_schema,
            "ssm_occurrence_centric": final_with_genes_schema,
        }

    def arrange_maf_df(self, mafs: Tuple[MAF, ...] = (MAF(),)) -> sql.DataFrame:
        return self.spark_session.createDataFrame(mafs, schema=self.maf_schema)

    @pytest.mark.parametrize(
        ("index_name", "join_gene", "add_gene_aa_change"),
        (
            pytest.param("case_centric", False, False, id="case_centric"),
            pytest.param("gene_centric", False, False, id="gene_centric"),
            pytest.param("ssm_centric", True, True, id="ssm_centric"),
            pytest.param(
                "ssm_occurrence_centric", True, False, id="ssm_occurrence_centric"
            ),
        ),
    )
    def test__build_for_ssm__single_row(
        self, index_name: str, join_gene: bool, add_gene_aa_change: bool
    ) -> None:
        maf_df = self.arrange_maf_df()
        builder = builders.ConsequenceBuilder()

        result_df = builder.build_for_ssm(
            maf_df, index_name, join_gene, add_gene_aa_change
        )

        assert result_df.count() == 1
        assert result_df.schema == self.final_schemas[index_name]

    def test__build_for_ssm__data_translated(self) -> None:
        all_effects = AllEffects(
            consequence_type="all_effect-ct",
            transcript_id="t-0",
            ref_seq_accession="all_effects-rsa",
            hgvsc="all_effects-hgvsc",
            vep_impact="all_effects-vi",
            sift="all_effects_sift(0.1)",
            polyphen="all_effects_polyphen(0.2)",
            transcript_strand="-1",
        )
        maf = MAF(
            all_effects=str(all_effects),
            consequence_type="maf-ct",
            transcript_id="t-0",
            ref_seq_accession="maf-rsa",
            hgvsc="maf-hgvsc",
            vep_impact="maf-vi",
            is_canonical=False,
            sift_impact="maf-si",
            sift_score=0.3,
            polyphen_impact="maf-pi",
            polyphen_score=0.4,
        )
        maf_df = self.arrange_maf_df(mafs=(maf,))
        builder = builders.ConsequenceBuilder()

        result_df = builder.build_for_ssm(maf_df, "ssm_occurrence_centric", True, False)
        result_row = more_itertools.one(result_df.collect())
        result_consequence = more_itertools.one(result_row.consequence)
        result_transcript = result_consequence.transcript
        result_annotation = result_transcript.annotation
        result_gene = result_transcript.gene

        assert result_row.ssm_id == maf.ssm_id
        # TRANSCRIPT
        assert result_transcript.transcript_id == all_effects.transcript_id
        assert result_transcript.consequence_type == all_effects.consequence_type
        assert result_transcript.ref_seq_accession == all_effects.ref_seq_accession
        # ANNOTATION
        assert result_annotation.amino_acids == maf.amino_acids
        assert result_annotation.cdna_position == maf.cdna_position
        assert result_annotation.cds_end == maf.cds_end
        assert result_annotation.cds_length == maf.cds_length
        assert result_annotation.cds_position == maf.cds_position
        assert result_annotation.cds_start == maf.cds_start
        assert result_annotation.clin_sig == maf.clin_sig
        assert result_annotation.codons == maf.codons
        assert result_annotation.domains == maf.domains
        assert result_annotation.ensp == maf.ensp
        assert result_annotation.hgvsc == all_effects.hgvsc
        assert result_annotation.hgvsp == maf.hgvsp
        assert result_annotation.hgvsp_short == maf.hgvsp_short
        assert result_annotation.polyphen_impact != maf.polyphen_impact
        assert _convert_to_decimal(
            result_annotation.polyphen_score
        ) != _convert_to_decimal(maf.polyphen_score)
        assert result_annotation.protein_position == maf.protein_position
        assert result_annotation.pubmed == maf.pubmed
        assert result_annotation.sift_impact != maf.sift_impact
        assert _convert_to_decimal(result_annotation.sift_score) != _convert_to_decimal(
            maf.sift_score
        )
        assert result_annotation.swissprot == maf.swissprot
        assert result_annotation.transcript_id == all_effects.transcript_id
        assert result_annotation.trembl == maf.trembl
        assert result_annotation.uniparc == maf.uniparc
        assert result_annotation.vep_impact == all_effects.vep_impact
        # GENE
        assert result_gene.biotype == maf.biotype
        assert result_gene.canonical_transcript_id == maf.canonical_transcript_id
        assert tuple(result_gene.cytoband) == maf.cytoband
        assert tuple(result_gene.external_db_ids.entrez_gene) == maf.entrez_gene
        assert tuple(result_gene.external_db_ids.hgnc) == maf.hgnc
        assert tuple(result_gene.external_db_ids.omim_gene) == maf.omim_gene
        assert (
            tuple(result_gene.external_db_ids.uniprotkb_swissprot)
            == maf.uniprotkb_swissprot
        )
        assert result_gene.gene_chromosome == maf.gene_chromosome
        assert result_gene.gene_end == maf.gene_end
        assert result_gene.gene_id == maf.gene_id
        assert result_gene.gene_start == maf.gene_start
        assert result_gene.gene_strand == maf.gene_strand
        assert result_gene.is_cancer_gene_census == maf.is_cancer_gene_census
        assert result_gene.symbol == all_effects.do_not_use
        assert tuple(result_gene.synonyms) == maf.synonyms

    def test__build_for_ssm__aa_change_without_genes_raises(self) -> None:
        maf_df = self.arrange_maf_df()
        builder = builders.ConsequenceBuilder()

        with pytest.raises(ValueError):
            builder.build_for_ssm(maf_df, "case_centric", False, True)

    def test__build_for_ssm__filter_non_matching_all_effects(self) -> None:
        all_effects = AllEffects(do_not_use="NOT_THE_SAME")
        maf_df = self.arrange_maf_df((MAF(all_effects=str(all_effects)),))
        builder = builders.ConsequenceBuilder()

        result_df = builder.build_for_ssm(maf_df, "case_centric")

        assert result_df.count() == 0

    def test__build_for_ssm__set_non_selected_transcripts_props_to_none(self) -> None:
        all_effects0 = AllEffects(transcript_id="t-0")
        all_effects1 = AllEffects(transcript_id="t-1")
        all_effects = (all_effects0, all_effects1)
        maf = MAF(
            all_effects=";".join(str(e) for e in all_effects), transcript_id="t-0"
        )
        maf_df = self.arrange_maf_df((maf,))
        builder = builders.ConsequenceBuilder()

        result_df = builder.build_for_ssm(maf_df, "case_centric")
        result_data = more_itertools.one(result_df.collect())

        transcripts = {
            c.transcript.transcript_id: c.transcript for c in result_data["consequence"]
        }
        transcript0 = transcripts.get("t-0")
        transcript1 = transcripts.get("t-1")

        assert transcript0
        assert transcript1
        assert transcript1.annotation.amino_acids is None
        assert transcript1.annotation.cdna_position is None
        assert transcript1.annotation.cds_end is None
        assert transcript1.annotation.cds_length is None
        assert transcript1.annotation.cds_position is None
        assert transcript1.annotation.cds_start is None
        assert transcript1.annotation.clin_sig is None
        assert transcript1.annotation.codons is None
        assert transcript1.annotation.domains is None
        assert transcript1.annotation.ensp is None
        assert transcript1.annotation.hgvsp is None
        assert transcript1.annotation.hgvsp_short is None
        assert transcript1.annotation.protein_position is None
        assert transcript1.annotation.swissprot is None
        assert transcript1.annotation.trembl is None
        assert transcript1.annotation.uniparc is None

    def test__build_for_ssm__aa_change_p_removed(self) -> None:
        all_effects = AllEffects(aa_change="p.A609S")
        maf = MAF(all_effects=str(all_effects))
        maf_df = self.arrange_maf_df((maf,))
        builder = builders.ConsequenceBuilder()

        result_df = builder.build_for_ssm(maf_df, "case_centric")
        result_row = more_itertools.one(result_df.collect())
        result_consequence = more_itertools.one(result_row.consequence)

        assert result_consequence.transcript.aa_change == "A609S"

    @pytest.mark.parametrize(
        ("aa_change", "expected_start"),
        (("p.A207T", 207), ("p.F114_I120del", 114), (None, None)),
        ids=("valid_start", "valid_start_with_end", "no_valid_start"),
    )
    def test__build_for_ssm__aa_start(
        self, aa_change: Optional[str], expected_start: Optional[int]
    ) -> None:
        all_effects = AllEffects(aa_change=aa_change)
        maf = MAF(all_effects=str(all_effects))
        maf_df = self.arrange_maf_df((maf,))
        builder = builders.ConsequenceBuilder()

        result_df = builder.build_for_ssm(maf_df, "ssm_occurrence_centric", True, False)
        result_row = more_itertools.one(result_df.collect())
        result_consequence = more_itertools.one(result_row.consequence)

        assert result_consequence.transcript.aa_start == expected_start

    @pytest.mark.parametrize(
        ("aa_change", "expected_end"),
        (
            pytest.param("p.F114_I120del", 120, id="valid_end"),
            pytest.param("p.A207T", 207, id="no_end_but_valid_start"),
            pytest.param("p.E1371Rfs*16", 1371, id="ignore_trailing_numeric_values"),
            pytest.param(None, None, id="no_valid_end_or_start"),
        ),
    )
    def test__build_for_ssm__aa_end(
        self, aa_change: Optional[str], expected_end: Optional[int]
    ) -> None:
        """TODO: FOLLOW UP ON E1371Rfs*16 not 16?"""
        all_effects = AllEffects(aa_change=aa_change)
        maf = MAF(all_effects=str(all_effects))
        maf_df = self.arrange_maf_df((maf,))
        builder = builders.ConsequenceBuilder()

        result_df = builder.build_for_ssm(maf_df, "ssm_occurrence_centric", True, False)
        result_row = more_itertools.one(result_df.collect())
        result_consequence = more_itertools.one(result_row.consequence)

        assert result_consequence.transcript.aa_end == expected_end

    def test__build_for_ssm__empty_aa_change_to_null(self) -> None:
        all_effects = AllEffects(aa_change="")
        maf = MAF(all_effects=str(all_effects))
        maf_df = self.arrange_maf_df((maf,))
        builder = builders.ConsequenceBuilder()

        result_df = builder.build_for_ssm(maf_df, "ssm_occurrence_centric", True, False)
        result_row = more_itertools.one(result_df.collect())
        result_consequence = more_itertools.one(result_row.consequence)

        assert result_consequence.transcript.aa_change is None

    @pytest.mark.parametrize(
        ("polyphen", "expected_impact"),
        (
            pytest.param("benign(0.007)", "benign"),
            pytest.param(None, "", id="None-empty"),
        ),
    )
    def test__build_for_ssm__extract_polyphen_impact_value(
        self, polyphen: Optional[str], expected_impact: str
    ) -> None:
        all_effects = AllEffects(polyphen=polyphen)
        maf = MAF(all_effects=str(all_effects))
        maf_df = self.arrange_maf_df((maf,))
        builder = builders.ConsequenceBuilder()

        result_df = builder.build_for_ssm(maf_df, "ssm_occurrence_centric", True, False)
        result_row = more_itertools.one(result_df.collect())
        result_consequence = more_itertools.one(result_row.consequence)
        result_annotation = result_consequence.transcript.annotation

        assert result_annotation.polyphen_impact == expected_impact

    @pytest.mark.parametrize(
        ("polyphen", "expected_score"),
        (
            pytest.param(
                "possibly_damaging(0.895)",
                decimal.Decimal("0.895"),
                id="possibly_damaging(0.895)-0.895",
            ),
            pytest.param(None, None),
        ),
    )
    def test__build_for_ssm__extract_polyphen_score_value(
        self, polyphen: Optional[str], expected_score: Optional[decimal.Decimal]
    ) -> None:
        all_effects = AllEffects(polyphen=polyphen)
        maf = MAF(all_effects=str(all_effects))
        maf_df = self.arrange_maf_df((maf,))
        builder = builders.ConsequenceBuilder()

        result_df = builder.build_for_ssm(maf_df, "ssm_occurrence_centric", True, False)
        result_row = more_itertools.one(result_df.collect())
        result_consequence = more_itertools.one(result_row.consequence)
        result_annotation = result_consequence.transcript.annotation

        assert _convert_to_decimal(result_annotation.polyphen_score) == expected_score

    @pytest.mark.parametrize(
        ("sift", "expected_impact"),
        (
            pytest.param("tolerated_low_confidence(1)", "tolerated_low_confidence"),
            pytest.param(None, "", id="None-empty"),
        ),
    )
    def test__build_for_ssm__extract_sift_impact_value(
        self, sift: Optional[str], expected_impact: str
    ) -> None:
        all_effects = AllEffects(sift=sift)
        maf = MAF(all_effects=str(all_effects))
        maf_df = self.arrange_maf_df((maf,))
        builder = builders.ConsequenceBuilder()

        result_df = builder.build_for_ssm(maf_df, "ssm_occurrence_centric", True, False)
        result_row = more_itertools.one(result_df.collect())
        result_consequence = more_itertools.one(result_row.consequence)
        result_annotation = result_consequence.transcript.annotation

        assert result_annotation.sift_impact == expected_impact

    @pytest.mark.parametrize(
        ("sift", "expected_score"),
        (
            pytest.param(
                "deleterious(0.02)",
                decimal.Decimal("0.02"),
                id="deleterious(0.02)-0.02",
            ),
            pytest.param(None, None),
        ),
    )
    def test__build_for_ssm__extract_sift_score_value(
        self, sift: Optional[str], expected_score: Optional[decimal.Decimal]
    ) -> None:
        all_effects = AllEffects(sift=sift)
        maf = MAF(all_effects=str(all_effects))
        maf_df = self.arrange_maf_df((maf,))
        builder = builders.ConsequenceBuilder()

        result_df = builder.build_for_ssm(maf_df, "ssm_occurrence_centric", True, False)
        result_row = more_itertools.one(result_df.collect())
        result_consequence = more_itertools.one(result_row.consequence)
        result_annotation = result_consequence.transcript.annotation

        assert _convert_to_decimal(result_annotation.sift_score) == expected_score

    def test__build_for_ssm__consequence_id(self) -> None:
        all_effects = AllEffects(transcript_id="t-0")
        maf = MAF(ssm_id="ssm-0", all_effects=str(all_effects))
        maf_df = self.arrange_maf_df((maf,))
        builder = builders.ConsequenceBuilder()

        result_df = builder.build_for_ssm(maf_df, "case_centric", False, False)
        result_row = more_itertools.one(result_df.collect())
        result_consequence = more_itertools.one(result_row.consequence)

        assert result_consequence.consequence_id == utils.generate_uuid5(
            "ssm_consequence", maf.ssm_id, all_effects.transcript_id
        )

    @pytest.mark.parametrize(
        ("index_name", "join_gene", "add_gene_aa_change"),
        (
            pytest.param("case_centric", False, False, id="case_centric"),
            pytest.param("ssm_centric", True, True, id="ssm_centric"),
        ),
    )
    def test__build_for_ssm__consequence_grouped_by_ssm_id(
        self, index_name: str, join_gene: bool, add_gene_aa_change: bool
    ) -> None:
        maf0 = MAF(
            ssm_id="ssm-0",
            transcript_id="t-0",
            all_effects=str(AllEffects(transcript_id="t-0")),
        )
        maf1 = MAF(
            ssm_id="ssm-1",
            transcript_id="t-1",
            all_effects=str(AllEffects(transcript_id="t-1")),
        )
        maf2 = MAF(
            ssm_id="ssm-1",
            transcript_id="t-2",
            all_effects=str(AllEffects(transcript_id="t-2")),
        )
        maf_df = self.arrange_maf_df((maf0, maf1, maf2))
        builder = builders.ConsequenceBuilder()

        result_df = builder.build_for_ssm(
            maf_df, index_name, join_gene, add_gene_aa_change
        )
        result_consequences = {r.ssm_id: r for r in result_df.collect()}

        assert result_consequences.keys() == frozenset({maf0.ssm_id, maf1.ssm_id})
        assert len(result_consequences["ssm-0"].consequence) == 1
        assert len(result_consequences["ssm-1"].consequence) == 2

    @pytest.mark.parametrize(
        ("aa_changes", "expected_output"),
        (
            pytest.param(("a",), ["gene a"], id="single_value"),
            pytest.param((None,), [], id="null_value"),
            pytest.param(("",), [], id="empty_value"),
            pytest.param(("a", "a"), ["gene a"], id="duplicate_values"),
            pytest.param(("b", "a"), ["gene a", "gene b"], id="unsorted_values"),
        ),
    )
    def test__build_for_ssm__gene_aa_change_all_effects(
        self, aa_changes: Iterable[Optional[str]], expected_output: List[str]
    ) -> None:
        all_effects = ";".join(
            str(AllEffects(do_not_use="gene", aa_change=c)) for c in aa_changes
        )
        maf = MAF(all_effects=all_effects, symbol="gene")
        maf_df = self.arrange_maf_df((maf,))
        builder = builders.ConsequenceBuilder()

        result_df = builder.build_for_ssm(maf_df, "ssm_centric", True, True)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.gene_aa_change == expected_output
