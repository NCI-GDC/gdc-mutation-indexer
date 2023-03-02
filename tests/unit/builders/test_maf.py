from typing import Any, Callable, Iterable, List, Optional
from unittest import mock

import more_itertools
import pytest
from pyspark import sql
from pyspark.sql import types

from exports import builders
from exports.builders import maf
from exports.configuration.builders import viz
from exports.constants import build
from tests.unit import utils
from tests.unit.data import schemas
from tests.unit.data.models import gene_model
from tests.unit.data.models.viz.civic import dna, prot
from tests.unit.data.models.viz.maf import inputs


@pytest.fixture(scope="class")
def gene_model_schema() -> types.StructType:
    return schemas.Builders.GeneModel.FINAL.load()


@pytest.fixture(scope="class")
def masked_somatic_mutation_schema() -> types.StructType:
    return schemas.Viz.Builders.MAF.MASKED_SOMATIC_MUTATION.load()


@pytest.fixture(scope="class")
def aggregated_somatic_mutation_schema() -> types.StructType:
    return schemas.Viz.Builders.MAF.AGGREGATED_SOMATIC_MUTATION.load()


@pytest.fixture(scope="class")
def civic_dna_schema() -> types.StructType:
    return schemas.Viz.Builders.CIVIC.DNA.FINAL.load()


@pytest.fixture(scope="class")
def civic_prot_schema() -> types.StructType:
    return schemas.Viz.Builders.CIVIC.PROT.FINAL.load()


@pytest.fixture(scope="class")
def final_maf_schema() -> types.StructType:
    return schemas.Viz.Builders.MAF.FINAL.load()


def arrange_config() -> viz.MAFBuilder:
    return mock.MagicMock(
        spec=viz.MAFBuilder,
        is_cached=False,
        repartition_size=1,
        backup=mock.MagicMock(mode=build.BackupMode.NEITHER, path=""),
    )


def assert_core_maf_transformed(
    result_maf: sql.Row, maf: inputs.MAF, gm: gene_model.GeneModel
) -> None:
    assert result_maf._id.asDict() == gm._id
    assert result_maf.aa_change == maf.ESP_AA_AF
    assert result_maf.aa_end == maf.ESP_AA_AF
    assert result_maf.aa_start == maf.ESP_AA_AF
    assert result_maf.all_effects == maf.all_effects
    assert result_maf.amino_acids == maf.Amino_acids
    assert result_maf.biotype == gm.biotype
    assert result_maf.canonical_transcript_id == gm.canonical_transcript_id
    assert result_maf.case_id == maf.case_id
    assert result_maf.ccds == maf.CCDS
    assert result_maf.cdna_position == maf.cDNA_position
    assert result_maf.cds_position == maf.CDS_position
    assert result_maf.center == maf.Center
    assert result_maf.clin_sig == maf.CLIN_SIG
    assert result_maf.codons == maf.Codons
    assert result_maf.consequence_type == maf.Consequence
    assert tuple(result_maf.cytoband) == gm.cytoband
    assert result_maf.dbsnp_rs == maf.dbSNP_RS
    assert result_maf.dbsnp_val_status == maf.dbSNP_Val_Status
    assert result_maf.description == gm.description
    assert result_maf.domains == maf.DOMAINS
    assert result_maf.ensp == maf.ENSP
    assert tuple(result_maf.entrez_gene) == gm.entrez_gene
    assert result_maf.existing_variation == maf.Existing_variation
    assert result_maf.gene_end == gm.gene_end
    assert result_maf.gene_id == maf.Gene
    assert result_maf.gene_start == gm.gene_start
    assert result_maf.gene_strand == gm.gene_strand
    assert tuple(result_maf.hgnc) == gm.hgnc
    assert result_maf.hgvsc == maf.HGVSc
    assert result_maf.hgvsp == maf.HGVSp
    assert result_maf.hgvsp_short == maf.HGVSp_Short
    assert result_maf.is_cancer_gene_census == gm.is_cancer_gene_census
    assert result_maf.match_norm_seq_allele1 == maf.Match_Norm_Seq_Allele1
    assert result_maf.match_norm_seq_allele2 == maf.Match_Norm_Seq_Allele2
    assert result_maf.matched_norm_sample_barcode == maf.Matched_Norm_Sample_Barcode
    assert result_maf.matched_norm_sample_uuid == maf.Matched_Norm_Sample_UUID
    assert result_maf.mutation_status == maf.Mutation_Status
    assert result_maf.name == gm.name
    assert result_maf.ncbi_build == maf.NCBI_Build
    assert tuple(result_maf.omim_gene) == gm.omim_gene
    assert result_maf.protein_position == maf.Protein_position
    assert result_maf.pubmed == maf.PUBMED
    assert result_maf.ref_seq_accession == maf.ESP_AA_AF
    assert result_maf.reference_allele == maf.Reference_Allele
    assert result_maf.swissprot == maf.SWISSPROT
    assert result_maf.symbol == gm.symbol
    assert tuple(result_maf.synonyms) == gm.synonyms
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
    assert tuple(result_maf.uniprotkb_swissprot) == gm.uniprotkb_swissprot
    assert result_maf.validation_method == maf.Validation_Method
    assert result_maf.variant_type == maf.Variant_Type
    assert result_maf.vep_impact == maf.IMPACT

    for result_transcript, expected_transcript in more_itertools.zip_equal(
        result_maf.transcripts, gm.transcripts
    ):
        gene_model.assert_transcripts_equal(result_transcript, expected_transcript)


class TestMAFBuilder:
    @pytest.fixture(autouse=True)
    def load_fixtures(
        self,
        create_dataframe: Callable[[Iterable[Any], types.StructType], sql.DataFrame],
        gene_model_schema: types.StructType,
        masked_somatic_mutation_schema: types.StructType,
        aggregated_somatic_mutation_schema: types.StructType,
        civic_dna_schema: types.StructType,
        civic_prot_schema: types.StructType,
        final_maf_schema: types.StructType,
    ) -> None:
        self.create_dataframe = create_dataframe
        self.gene_model_schema = gene_model_schema
        self.masked_somatic_mutation_schema = masked_somatic_mutation_schema
        self.aggregated_somatic_mutation_schema = aggregated_somatic_mutation_schema
        self.civic_dna_schema = civic_dna_schema
        self.civic_prot_schema = civic_prot_schema
        self.final_maf_schema = final_maf_schema

    def arrange_builder(
        self,
        masked_somatic_mutation_mafs: Iterable[inputs.MAF] = (inputs.MAF(),),
        aggregated_somatic_mutation_mafs: Iterable[inputs.MAF] = (),
    ) -> builders.MAFBuilder:
        mafs = tuple(
            maf.to_sql_row(self.masked_somatic_mutation_schema.fields)
            for maf in masked_somatic_mutation_mafs
        )
        masked_somatic_mutation_df = self.create_dataframe(
            mafs, self.masked_somatic_mutation_schema
        )
        mafs = tuple(
            maf.to_sql_row(self.aggregated_somatic_mutation_schema.fields)
            for maf in aggregated_somatic_mutation_mafs
        )
        aggregated_somatic_mutation_df = self.create_dataframe(
            mafs, self.aggregated_somatic_mutation_schema
        )

        config = arrange_config()
        sql_context = mock.MagicMock()

        doc_dataframe_util = mock.MagicMock()
        doc_dataframe_util.get_dataframe.side_effect = (
            masked_somatic_mutation_df,
            aggregated_somatic_mutation_df,
        )

        return builders.MAFBuilder(config, sql_context, doc_dataframe_util)

    def arrange_inputs(
        self,
        gene_model: Iterable[gene_model.GeneModel] = (gene_model.GeneModel(),),
        dna_data: Iterable[dna.DNA] = (),
        prot_data: Iterable[prot.PROT] = (),
    ) -> maf.MAFInputs:
        gene_model_df = self.create_dataframe(gene_model, self.gene_model_schema)
        maf_metadata_df = mock.MagicMock()
        civic_dna_df = self.create_dataframe(dna_data, self.civic_dna_schema)
        civic_prot_df = self.create_dataframe(prot_data, self.civic_prot_schema)

        return {
            "gene_model_df": gene_model_df,
            "maf_metadata_df": maf_metadata_df,
            "civic_dna_df": civic_dna_df,
            "civic_prot_df": civic_prot_df,
        }

    def test__build__joins_succeed(self) -> None:
        inputs = self.arrange_inputs()
        builder = self.arrange_builder()

        result_df = builder.build(**inputs)

        assert result_df.count() == 1
        assert result_df.schema == self.final_maf_schema

    def test__build__masked_somatic_mutation_maf_transformed(self) -> None:
        gm = gene_model.GeneModel()
        maf = inputs.MAF()
        input_dfs = self.arrange_inputs(gene_model=(gm,))
        builder = self.arrange_builder(masked_somatic_mutation_mafs=(maf,))

        result_df = builder.build(**input_dfs)
        result_row = more_itertools.one(result_df.collect())

        assert_core_maf_transformed(result_row, maf, gm)

        assert result_row.normal_bam_uuid == maf.normal_bam_uuid
        assert result_row.tumor_bam_uuid == maf.tumor_bam_uuid
        assert result_row.variant_caller == maf.callers

    def test__build__aggregated_somatic_mutation_maf_transformed(
        self,
    ) -> None:
        gm = gene_model.GeneModel()
        maf = inputs.MAF()
        input_dfs = self.arrange_inputs(gene_model=(gm,))
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(), aggregated_somatic_mutation_mafs=(maf,)
        )

        result_df = builder.build(**input_dfs)
        result_row = more_itertools.one(result_df.collect())

        assert_core_maf_transformed(result_row, maf, gm)

        assert result_row.normal_bam_uuid is None
        assert result_row.tumor_bam_uuid is None
        assert result_row.variant_caller == "FM Simple Somatic Mutation"

    def test__build__cast_str_to_int(self) -> None:
        maf = inputs.MAF()
        input_dfs = self.arrange_inputs()
        builder = self.arrange_builder(masked_somatic_mutation_mafs=(maf,))

        result_df = builder.build(**input_dfs)
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
    def test__build__cast_str_to_bool(
        self, bool_value: Optional[str], expected_value: Optional[bool]
    ) -> None:
        maf = inputs.MAF(CANONICAL=bool_value)
        input_dfs = self.arrange_inputs()
        builder = self.arrange_builder(masked_somatic_mutation_mafs=(maf,))

        result_df = builder.build(**input_dfs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.is_canonical == expected_value

    def test__build__joins_fail(self) -> None:
        input_dfs = self.arrange_inputs((gene_model.GeneModel(),))
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(inputs.MAF(Gene="GENE0"),)
        )

        result_df = builder.build(**input_dfs)

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
    def test__build__available_variation_data_added(
        self,
        tumor_sample_barcode: Optional[str],
        case_id: Optional[str],
        available_variation_data: List[str],
    ) -> None:
        input_dfs = self.arrange_inputs()
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(
                inputs.MAF(Tumor_Sample_Barcode=tumor_sample_barcode, case_id=case_id),
            )
        )

        result_df = builder.build(**input_dfs)
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
    def test__build__genomic_dna_change(
        self, variant_type: str, genomic_dna_change: str
    ) -> None:
        maf = inputs.MAF(
            Variant_Type=variant_type,
            Start_Position="1",
            End_Position="100",
            Allele="A",
            Reference_Allele="C",
            Chromosome="chr1",
        )
        input_dfs = self.arrange_inputs()
        builder = self.arrange_builder(masked_somatic_mutation_mafs=(maf,))

        result_df = builder.build(**input_dfs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.genomic_dna_change == genomic_dna_change

    @pytest.mark.parametrize(
        ("mutation_status", "mutation_type"),
        (
            ("Somatic", "Simple Somatic Mutation"),
            ("Normal", None),
        ),
    )
    def test__build__mutation_type(
        self, mutation_status: str, mutation_type: Optional[str]
    ) -> None:
        maf = inputs.MAF(Mutation_Status=mutation_status)
        input_dfs = self.arrange_inputs()
        builder = self.arrange_builder(masked_somatic_mutation_mafs=(maf,))

        result_df = builder.build(**input_dfs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.mutation_type == mutation_type

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
    def test__build__mutation_subtype(
        self, variant_type: str, mutation_subtype: str
    ) -> None:
        maf = inputs.MAF(
            Variant_Type=variant_type,
        )
        input_dfs = self.arrange_inputs()
        builder = self.arrange_builder(masked_somatic_mutation_mafs=(maf,))

        result_df = builder.build(**input_dfs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.mutation_subtype == mutation_subtype

    def test__build__uuids_generated(self):
        maf = inputs.MAF()

        input_dfs = self.arrange_inputs()
        builder = self.arrange_builder(masked_somatic_mutation_mafs=(maf,))

        result_df = builder.build(**input_dfs)
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
    def test__build__cds_lengths(
        self, cds_position: str, cds_start: int, cds_end: int, cds_length: int
    ) -> None:
        input_dfs = self.arrange_inputs()
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(inputs.MAF(CDS_position=cds_position),)
        )

        result_df = builder.build(**input_dfs)
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
    def test__build__polyphen_impact_and_score(
        self,
        polyphen: Optional[str],
        polyphen_impact: Optional[str],
        polyphen_score: Optional[float],
    ) -> None:
        input_dfs = self.arrange_inputs()
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(inputs.MAF(PolyPhen=polyphen),)
        )

        result_df = builder.build(**input_dfs)
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
    def test__build__sift_impact_and_score(
        self,
        sift: Optional[str],
        sift_impact: Optional[str],
        sift_score: Optional[float],
    ) -> None:
        input_dfs = self.arrange_inputs()
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(inputs.MAF(SIFT=sift),)
        )

        result_df = builder.build(**input_dfs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.sift_impact == sift_impact
        assert result_row.sift_score == sift_score

    def test__build__canonical_transcript_lengths_added(self) -> None:
        canonical_transcript = gene_model.Transcript(
            length=100, length_cds=30, end=1222, start=1000, is_canonical=True
        )
        other_transcript = gene_model.Transcript(
            length=10, length_cds=3, end=122, start=100
        )
        gm = (
            gene_model.GeneModel(transcripts=(canonical_transcript, other_transcript)),
        )

        input_dfs = self.arrange_inputs(gene_model=gm)
        builder = self.arrange_builder()

        result_df = builder.build(**input_dfs)
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

    def test__build__canonical_transcript_lengths_no_canonical_transcipt(
        self,
    ) -> None:
        other_transcript = gene_model.Transcript(
            length=10, length_cds=3, end=122, start=100
        )
        gm = (gene_model.GeneModel(transcripts=(other_transcript,)),)

        inputs = self.arrange_inputs(gene_model=gm)
        builder = self.arrange_builder()

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.canonical_transcript_length == None
        assert result_row.canonical_transcript_length_cds == None
        assert result_row.canonical_transcript_length_genomic == None

    def test__build__normal_genotype(self) -> None:
        maf = inputs.MAF()

        input_dfs = self.arrange_inputs()
        builder = self.arrange_builder(masked_somatic_mutation_mafs=(maf,))

        result_df = builder.build(**input_dfs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.normal_genotype.allele_id == utils.generate_uuid5(
            maf.Match_Norm_Seq_Allele1, maf.Match_Norm_Seq_Allele2
        )

    def test__build__static_fields(self) -> None:
        inputs = self.arrange_inputs()
        builder = self.arrange_builder()

        result_df = builder.build(**inputs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.variant_process == "masked"
        assert result_row.empty == None

    def test__build__gene_chromosome(self) -> None:
        input_dfs = self.arrange_inputs()
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(inputs.MAF(Chromosome="chr1"),)
        )

        result_df = builder.build(**input_dfs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.gene_chromosome == "1"

    def test__build__chromosome(self) -> None:
        input_dfs = self.arrange_inputs(
            gene_model=(gene_model.GeneModel(chromosome="1"),)
        )
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(inputs.MAF(Chromosome="chr1"),)
        )

        result_df = builder.build(**input_dfs)
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
    def test__build__cosmic_id(
        self, cosmic: Optional[str], cosmic_id: Optional[List[str]]
    ) -> None:
        input_dfs = self.arrange_inputs()
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(inputs.MAF(COSMIC=cosmic),)
        )

        result_df = builder.build(**input_dfs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.cosmic_id == cosmic_id

    def test__build__strip_domains(self) -> None:
        input_dfs = self.arrange_inputs()
        builder = self.arrange_builder(
            masked_somatic_mutation_mafs=(
                inputs.MAF(
                    DOMAINS="Gene3D:2.60.40.10;PDB-ENSP_mappings:4l29.b;PDB-ENSP_mappings:4l3c.b;PDB-ENSP_mappings:6nca.a;PDB-ENSP_mappings:6nca.b;PDB-ENSP_mappings:6nca.c;PDB-ENSP_mappings:6nca.d;PDB-ENSP_mappings:6nca.e;PDB-ENSP_mappings:6nca.f;PDB-ENSP_mappings:6nca.g;PDB-ENSP_mappings:6nca.h;PDB-ENSP_mappings:6nca.i;PDB-ENSP_mappings:6nca.j;PDB-ENSP_mappings:6nca.k;PDB-ENSP_mappings:6nca.l;PDB-ENSP_mappings:6nca.m;PDB-ENSP_mappings:6nca.n;PDB-ENSP_mappings:6nca.o;PDB-ENSP_mappings:6nca.p;PDB-ENSP_mappings:6nca.q;PDB-ENSP_mappings:6nca.r;PDB-ENSP_mappings:6nca.s;PDB-ENSP_mappings:6nca.t;Pfam:PF07654;PROSITE_profiles:PS50835;PANTHER:PTHR19944;PANTHER:PTHR19944:SF62;SMART:SM00407;Superfamily:SSF48726;CDD:cd05770"
                ),
            )
        )

        result_df = builder.build(**input_dfs)
        result_row = more_itertools.one(result_df.collect())

        assert (
            result_row.domains
            == "Gene3D:2.60.40.10;Pfam:PF07654;PROSITE_profiles:PS50835;PANTHER:PTHR19944;PANTHER:PTHR19944:SF62;SMART:SM00407;Superfamily:SSF48726;CDD:cd05770"
        )

    def test__build__civic_annotations_both(self) -> None:
        dna_data = dna.DNA()
        prot_data = prot.PROT()
        maf = inputs.MAF(
            Allele=dna_data.tumor_allele,
            Chromosome=dna_data.chromosome,
            HGVSp_Short=prot_data.hgvsp_short,
            Start_Position=str(dna_data.start_position),
            Reference_Allele=dna_data.reference_allele,
        )
        gm = gene_model.GeneModel(
            name=prot_data.name, chromosome=dna_data.chromosome.replace("chr", "")
        )

        input_dfs = self.arrange_inputs((gm,), (dna_data,), (prot_data,))
        builder = self.arrange_builder((maf,))

        result_df = builder.build(**input_dfs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.civic_gene_id == dna_data.dna_civic_gene_id
        assert result_row.civic_variant_id == dna_data.dna_civic_var_id

    def test__build__civic_annotations_dna_only(self) -> None:
        dna_data = dna.DNA()
        maf = inputs.MAF(
            Allele=dna_data.tumor_allele,
            Chromosome=dna_data.chromosome,
            Start_Position=str(dna_data.start_position),
            Reference_Allele=dna_data.reference_allele,
        )
        gm = gene_model.GeneModel(chromosome=dna_data.chromosome.replace("chr", ""))

        input_dfs = self.arrange_inputs((gm,), dna_data=(dna_data,), prot_data=())
        builder = self.arrange_builder((maf,))

        result_df = builder.build(**input_dfs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.civic_gene_id == dna_data.dna_civic_gene_id
        assert result_row.civic_variant_id == dna_data.dna_civic_var_id

    def test__build__civic_annotations_prot_only(self) -> None:
        prot_data = prot.PROT()
        maf = inputs.MAF(HGVSp_Short=prot_data.hgvsp_short)
        gm = gene_model.GeneModel(name=prot_data.name)

        input_dfs = self.arrange_inputs((gm,), dna_data=(), prot_data=(prot_data,))
        builder = self.arrange_builder((maf,))

        result_df = builder.build(**input_dfs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.civic_gene_id == prot_data.prot_civic_gene_id
        assert result_row.civic_variant_id == prot_data.prot_civic_var_id

    def test__build__civic_annotations_neither(self) -> None:
        maf = inputs.MAF()
        gm = gene_model.GeneModel()

        input_dfs = self.arrange_inputs((gm,), dna_data=(), prot_data=())
        builder = self.arrange_builder((maf,))

        result_df = builder.build(**input_dfs)
        result_row = more_itertools.one(result_df.collect())

        assert result_row.civic_gene_id is None
        assert result_row.civic_variant_id is None
