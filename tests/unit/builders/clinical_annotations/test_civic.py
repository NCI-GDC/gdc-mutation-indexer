import contextlib
import csv
import dataclasses
import functools
import io
from typing import (
    IO,
    Any,
    Callable,
    ContextManager,
    Iterable,
    Iterator,
    Optional,
    Tuple,
)
from unittest import mock

import importlib_resources as resources
import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from exports.builders.clinical_annotations import civic
from tests.unit import utils
from tests.unit.data.schemas import builders as schemas


@dataclasses.dataclass(frozen=True)
class DNACivic:
    civic_var_id: str = "1"
    civic_gene_id: str = "3"
    source: str = "gDNA"
    chromosome: str = "chr1"
    start_position: str = "33772590"
    reference_allele: str = "C"
    alternative_allele: str = "A"


@dataclasses.dataclass(frozen=True)
class ProtCivic:
    civic_var_id: str = "3"
    civic_gene_id: str = "1"
    hugo_symbol: str = "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
    gene: str = "ENSG00000181163.12"
    hgvsp: str = "p.A569S"
    source: str = "Protein"


@dataclasses.dataclass(frozen=True)
class Metadata:
    case_id: str = "case-0"
    data_type: str = "MAF"
    file_id: str = "f0"
    workflow: str = "MAF Workflow"


@dataclasses.dataclass(frozen=True)
class NormalGenotype:
    allele_id: Optional[str] = "03b61092-4545-526e-9b39-fc8005c40af5"


@dataclasses.dataclass(frozen=True)
class Domain:
    description: Optional[str] = "G protein-coupled receptor, rhodopsin-like"
    end: Optional[int] = 280
    gff_source: Optional[str] = "pfam"
    hit_name: Optional[str] = "PF00001"
    interpro_id: Optional[str] = "IPR000276"
    start: Optional[int] = 34


@dataclasses.dataclass(frozen=True)
class Exon:
    cdna_coding_end: Optional[int] = 0
    cdna_coding_start: Optional[int] = 0
    cdna_end: Optional[int] = 359
    cdna_start: Optional[int] = 1
    end: Optional[int] = 12227
    end_phase: Optional[int] = -1
    genomic_coding_end: Optional[int] = 0
    genomic_coding_stairt: Optional[int] = 0
    genomic_coding_start: Optional[int] = 0
    start: Optional[int] = 11869
    start_phase: Optional[int] = -1


@dataclasses.dataclass(frozen=True)
class Transcript:
    biotype: Optional[str] = "processed_transcript"
    cdna_coding_end: Optional[int] = 0
    cdna_coding_start: Optional[int] = 0
    coding_region_end: Optional[int] = 0
    coding_region_start: Optional[int] = 0
    domains: Optional[Tuple[Domain, ...]] = (Domain(),)
    end: Optional[int] = 14409
    end_exon: Optional[int] = None
    exons: Optional[Tuple[Exon, ...]] = (Exon(),)
    is_canonical: Optional[bool] = False
    length: Optional[int] = 1657
    length_amino_acid: Optional[int] = None
    length_cds: Optional[int] = None
    name: Optional[str] = "DDX11L1-002"
    number_of_exons: Optional[int] = 6
    seq_exon_end: Optional[int] = None
    seq_exon_start: Optional[int] = None
    start: Optional[int] = 11869
    start_exon: Optional[int] = None
    transcript_id: Optional[str] = "ENST00000456328"
    translation_id: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class MAF:
    _id: Optional[dict] = dataclasses.field(
        default_factory=lambda: {"$oid": "589c87ca0ef75875ed614a40"}
    )
    aa_change: Optional[str] = None
    aa_end: Optional[str] = None
    aa_start: Optional[str] = None
    all_effects: Optional[
        str
    ] = "CSMD2,missense_variant,p.A609S,ENST00000373381,NM_001281956.2,c.1825G>T,MODERATE,YES,tolerated(0.14),benign(0.305),-1;CSMD2,missense_variant,p.A569S,ENST00000619121,,c.1705G>T,MODERATE,,tolerated(0.13),benign(0.02),-1;CSMD2,missense_variant,p.A569S,ENST00000373388,NM_052896.4,c.1705G>T,MODERATE,,tolerated(0.12),benign(0.305),-1;CSMD2,missense_variant,p.A217S,ENST00000338325,,c.649G>T,MODERATE,,tolerated(0.18),benign(0.264),-1;CSMD2,missense_variant,p.A569S,ENST00000241312,,c.1705G>T,MODERATE,,tolerated(0.12),benign(0.305),-1"
    amino_acids: Optional[str] = "A/S"
    available_variation_data: Optional[Tuple[str, ...]] = ("ssm",)
    biotype: Optional[str] = "transcribed_unprocessed_pseudogene"
    canonical_transcript_id: Optional[str] = "ENST00000456328"
    canonical_transcript_length: Optional[int] = None
    canonical_transcript_length_cds: Optional[int] = None
    canonical_transcript_length_genomic: Optional[int] = None
    case_id: Optional[str] = "case-0"
    ccds: Optional[str] = "CCDS380.1"
    cdna_position: Optional[str] = "1734/13108"
    cds_end: Optional[int] = 12169
    cds_length: Optional[int] = 10464
    cds_position: Optional[str] = "1705/10464"
    cds_start: Optional[int] = 1705
    center: Optional[str] = "BI"
    chromosome: Optional[str] = "chr1"
    clin_sig: Optional[str] = None
    codons: Optional[str] = "Gct/Tct"
    consequence_type: Optional[str] = "missense_variant;NMD_transcript_variant"
    cosmic_id: Optional[str] = None
    cytoband: Optional[Tuple[str, ...]] = ("1p36.33",)
    dbsnp_rs: Optional[str] = "novel"
    dbsnp_val_status: Optional[str] = None
    description: Optional[
        str
    ] = "DISCONTINUED: This record has been withdrawn by NCBI because the model on which it was based was not predicted in a later annotation."
    domains: Optional[str] = None
    empty: None = None
    end_position: Optional[int] = 33772590
    ensp: Optional[str] = "ENSP00000241312"
    entrez_gene: Optional[Tuple[str, ...]] = (
        "100287596",
        "100287102",
        "727856",
        "84771",
    )
    existing_variation: Optional[str] = None
    gene_chromosome: Optional[str] = "1"
    gene_end: Optional[int] = 14409
    gene_id: Optional[str] = "ENSG00000121904"
    gene_start: Optional[int] = 11869
    gene_strand: Optional[int] = 1
    genomic_dna_change: Optional[str] = "chr1:g.33772590C>A"
    hgnc: Optional[Tuple[str, ...]] = ("HGNC:37102",)
    hgvsc: Optional[str] = "c.1705G>T"
    hgvsp: Optional[str] = "p.Ala569Ser"
    hgvsp_short: Optional[str] = "p.A569S"
    is_cancer_gene_census: Optional[str] = "true"
    is_canonical: Optional[str] = None
    match_norm_seq_allele1: Optional[str] = None
    match_norm_seq_allele2: Optional[str] = None
    matched_norm_sample_barcode: Optional[str] = "MBCProject_3808_SALIVA_1"
    matched_norm_sample_uuid: Optional[str] = "0e4ad056-bfba-4ff3-a41f-d3655009f544"
    mutation_status: Optional[str] = "Somatic"
    mutation_subtype: Optional[str] = "Single base substitution"
    mutation_type: Optional[str] = "Simple Somatic Mutation"
    n_depth: Optional[int] = 38
    name: Optional[str] = "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1"
    ncbi_build: Optional[str] = "GRCh38"
    normal_bam_uuid: Optional[str] = "604c11f1-ab8b-48a7-909e-982e873e02e5"
    normal_genotype: Optional[NormalGenotype] = NormalGenotype()
    occurrence_id: Optional[str] = "1746a06f-2052-5fec-8150-8da04c939ee4"
    omim_gene: Optional[Tuple[str, ...]] = ()
    polyphen_impact: Optional[str] = "benign"
    polyphen_score: Optional[float] = 0.305
    protein_position: Optional[str] = "569/3487"
    pubmed: Optional[str] = None
    ref_seq_accession: Optional[str] = None
    reference_allele: Optional[str] = "C"
    sift_impact: Optional[str] = "tolerated"
    sift_score: Optional[float] = 0.12
    ssm_id: Optional[str] = "d19178f1-c785-5c52-9e2b-26e106e03853"
    start_position: Optional[int] = 33772590
    swissprot: Optional[str] = "Q7Z408.146"
    symbol: Optional[str] = "DDX11L1"
    synonyms: Optional[Tuple[str, ...]] = ()
    t_alt_count: Optional[int] = 5
    t_depth: Optional[int] = 29
    t_ref_count: Optional[int] = 24
    transcript_id: Optional[str] = "ENST00000241312"
    transcripts: Optional[Tuple[Transcript, ...]] = (Transcript(),)
    trembl: Optional[str] = None
    tumor_allele: Optional[str] = "A"
    tumor_bam_uuid: Optional[str] = "9fa1ff4d-230d-477b-91d6-e2dc3896b6c4"
    tumor_sample_barcode: Optional[str] = "MBCProject_3808_T1_WES_1"
    tumor_sample_uuid: Optional[str] = "c004a75a-448b-440c-bd8f-46cfc6d8dd2a"
    tumor_seq_allele1: Optional[str] = "C"
    tumor_seq_allele2: Optional[str] = "A"
    tumor_validation_allele1: Optional[str] = None
    tumor_validation_allele2: Optional[str] = None
    uniparc: Optional[str] = "UPI00004561AB"
    uniprotkb_swissprot: Optional[Tuple[str, ...]] = ()
    validation_method: Optional[str] = None
    variant_caller: Optional[str] = "muse;varscan2"
    variant_process: Optional[str] = "masked"
    variant_type: Optional[str] = "SNP"
    vep_impact: Optional[str] = "MODERATE"


def create_mock_tsv(keys: Iterable[str], data: Iterable[Any]) -> io.StringIO:
    data = (dataclasses.asdict(d) for d in data)
    file = io.StringIO()
    writer = csv.DictWriter(file, fieldnames=frozenset(keys), delimiter="\t")

    writer.writeheader()
    writer.writerows(data)
    file.seek(0)

    return file


def mock_open(
    dna: Tuple[DNACivic, ...], prot: Tuple[ProtCivic, ...], path: str, mode: str = "r"
) -> IO:
    if path.endswith("civic.yml"):
        return open(path, mode)

    elif path.endswith("civic_gdcmaf_mapping_dna.tsv"):
        return create_mock_tsv(DNACivic.__dataclass_fields__.keys(), dna)

    elif path.endswith("civic_gdcmaf_mapping_prot.tsv"):
        return create_mock_tsv(ProtCivic.__dataclass_fields__.keys(), prot)

    else:
        raise AssertionError(f"Unknown file: {path} is being loaded.")


@contextlib.contextmanager
def arrange_mock_resource_filename() -> Iterator[Callable[[Any, str], str]]:
    with resources.as_file(
        resources.files("exports.schemas.clinical_annotations")
    ) as p:

        def mock(package: Any, path: str) -> str:
            if path.endswith("civic.yml"):
                return str(p.joinpath(path))

            return path

        yield mock


@pytest.fixture(scope="class")
def maf_schema() -> types.StructType:
    return schemas.Input.CIVIC.load()


@pytest.fixture(scope="class")
def final_schema() -> types.StructType:
    return schemas.Final.CIVIC.load()


class TestCivicBuilder:
    @pytest.fixture(autouse=True)
    def initialize_fixtures(
        self,
        spark_session: sql.SparkSession,
        maf_schema: types.StructType,
        final_schema: types.StructType,
    ) -> None:
        self.spark_session = spark_session
        self.maf_schema = maf_schema
        self.final_schema = final_schema

    def arrange_open(
        self,
        dna: Tuple[DNACivic, ...] = (DNACivic(),),
        prot: Tuple[ProtCivic, ...] = (ProtCivic(),),
    ) -> ContextManager[mock.MagicMock]:
        return mock.patch(
            "builtins.open", side_effect=functools.partial(mock_open, dna, prot)
        )

    def arrange_pkg_resources(self) -> ContextManager[contextlib.ExitStack]:
        stack = contextlib.ExitStack()

        resource_filename = stack.enter_context(arrange_mock_resource_filename())
        _ = stack.enter_context(
            mock.patch("pkg_resources.resource_filename", side_effect=resource_filename)
        )

        return stack

    def arrange_maf_df(self, mafs: Tuple[MAF, ...] = (MAF(),)) -> sql.DataFrame:
        return self.spark_session.createDataFrame(
            mafs,  # type: ignore
            schema=self.maf_schema,
        )

    def test__merge_with_maf__single_row(self) -> None:
        config = mock.MagicMock()
        sql_context = mock.MagicMock()
        sql_context.createDataFrame.side_effect = self.spark_session.createDataFrame
        maf_df = self.arrange_maf_df()
        builder = civic.CivicBuilder(config, sql_context)

        with self.arrange_open(), self.arrange_pkg_resources():
            result_df = builder.merge_with_maf(maf_df)

            assert result_df.count() == 1
            assert result_df.schema == self.final_schema

    def test__merge_with_maf__coalesces_dna_if_not_null(self) -> None:
        config = mock.MagicMock()
        sql_context = mock.MagicMock()
        sql_context.createDataFrame.side_effect = self.spark_session.createDataFrame
        maf_df = self.arrange_maf_df()
        builder = civic.CivicBuilder(config, sql_context)
        dna = DNACivic(civic_var_id="1", civic_gene_id="2")
        prot = ProtCivic(civic_var_id="5", civic_gene_id="6")

        with self.arrange_open((dna,), (prot,)), self.arrange_pkg_resources():
            result_df = builder.merge_with_maf(maf_df)
            result_row = more_itertools.one(result_df.collect())

        assert result_row.civic_variant_id == dna.civic_var_id
        assert result_row.civic_gene_id == dna.civic_gene_id

    def test__merge_with_maf__coalesces_prot_if_dna_null(self) -> None:
        config = mock.MagicMock()
        sql_context = mock.MagicMock()
        sql_context.createDataFrame.side_effect = self.spark_session.createDataFrame
        maf_df = self.arrange_maf_df()
        builder = civic.CivicBuilder(config, sql_context)
        dna = DNACivic(chromosome="chr2")
        prot = ProtCivic(civic_var_id="5", civic_gene_id="6")

        with self.arrange_open((dna,), (prot,)), self.arrange_pkg_resources():
            result_df = builder.merge_with_maf(maf_df)
            result_row = more_itertools.one(result_df.collect())

        assert result_row.civic_variant_id == prot.civic_var_id
        assert result_row.civic_gene_id == prot.civic_gene_id

    def test__merge_with_maf__coalesces_null_if_prot_and_dna_null(self) -> None:
        config = mock.MagicMock()
        sql_context = mock.MagicMock()
        sql_context.createDataFrame.side_effect = self.spark_session.createDataFrame
        maf_df = self.arrange_maf_df()
        builder = civic.CivicBuilder(config, sql_context)
        dna = DNACivic(chromosome="chr2")
        prot = ProtCivic(hugo_symbol="test")

        with self.arrange_open((dna,), (prot,)), self.arrange_pkg_resources():
            result_df = builder.merge_with_maf(maf_df)
            result_row = more_itertools.one(result_df.collect())

        assert result_row.civic_variant_id == None
        assert result_row.civic_gene_id == None
