import functools
import logging
from typing import Iterable, Optional

from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

import config
from exports import pyspark_extensions, schemas
from exports.builders import base_input_builder, utils
from exports.builders.clinical_annotations import civic

logging.basicConfig(format=config.LOG_FORMAT)


class MAFBuilder(base_input_builder.BaseInputBuilder):
    """
    Class responsible for assembling maf files into a single dataframe with
    uniform features
    """

    def __init__(
        self,
        config,
        sqlContext,
        annotation_builders: Iterable[civic.CivicBuilder],
    ):
        super(MAFBuilder, self).__init__(config, sqlContext, "maf")
        self.annotation_builders = annotation_builders
        self._raw_maf_schema: Optional[types.StructType] = None

    @property
    def raw_maf_schema(self) -> types.StructType:
        if not self._raw_maf_schema:
            self._raw_maf_schema = schemas.load_schema("builders/maf/raw_maf.yaml")

        return self._raw_maf_schema

    def build_from_cache(self, df):
        return df

    def build_from_scratch(
        self, gene_model_df: sql.DataFrame, **kwargs: sql.DataFrame
    ) -> sql.DataFrame:
        """
        Builds a master MAF dataframe by combining individual MAFs and
        augmenting them with additional features

        Args:
            gene_model_df: The output of the GeneModelbuilder.
        """

        df = self.combine()

        df = self.add_available_variation_data(df)
        # Add label identifying the mutation
        df = self.add_genomic_dna_change(df)
        # Add mutation_type
        df = self.add_mutation_type(df)
        # Add mutation_subtype
        df = self.add_mutation_subtype(df)
        # ssm_id from hashing unique columns in the maf
        df = self.add_ssm_id(df)
        # Create occurrence_id
        df = self.add_occurrence_id(df)
        # Get cds columns from cds_position
        df = self.extract_cds_position(df)
        # Extract sift and polyphen columns
        df = utils.extract_sift_polyphen(df)

        df = df.join(gene_model_df, df.gene_id == gene_model_df._gene_id, "inner")
        df = df.drop("_gene_id")
        df = self.add_null(df)
        df = self.add_canonical_transcript_lengths(df)
        df = self.add_normal_genotype(df)
        df = df.withColumn("chromosome", F.concat(F.lit("chr"), "chromosome"))
        df = df.withColumn("variant_process", F.lit("masked"))
        df = self.format_chr(df)
        df = self.format_cosmic_id(df)
        df = df.withColumn(
            "domains", F.regexp_replace("domains", r"PDB-ENSP_mappings:\w{4}\.\w;?", "")
        )
        for builder in self.annotation_builders:
            df = builder.merge_with_maf(df)

        self.logger.info("Repartitioning MAF dataframe")
        df = df.repartition(self.config.df_repartition, "ssm_id")

        if self.config.cache_dataframes["mafs"]:
            self.logger.info("Caching repartitioned MAF dataframe")
            df.cache().count()

        return df.select(
            "_id",
            "aa_change",
            "aa_end",
            "aa_start",
            "all_effects",
            "amino_acids",
            "available_variation_data",
            "biotype",
            "canonical_transcript_id",
            "canonical_transcript_length",
            "canonical_transcript_length_cds",
            "canonical_transcript_length_genomic",
            "case_id",
            "ccds",
            "cdna_position",
            "cds_end",
            "cds_length",
            "cds_position",
            "cds_start",
            "center",
            "chromosome",
            "clin_sig",
            "codons",
            "consequence_type",
            "cosmic_id",
            "cytoband",
            "dbsnp_rs",
            "dbsnp_val_status",
            "description",
            "domains",
            "empty",
            "end_position",
            "ensp",
            "entrez_gene",
            "existing_variation",
            "gene_chromosome",
            "gene_end",
            "gene_id",
            "gene_start",
            "gene_strand",
            "genomic_dna_change",
            "hgnc",
            "hgvsc",
            "hgvsp",
            "hgvsp_short",
            "is_cancer_gene_census",
            "is_canonical",
            "match_norm_seq_allele1",
            "match_norm_seq_allele2",
            "matched_norm_sample_barcode",
            "matched_norm_sample_uuid",
            "mutation_status",
            "mutation_subtype",
            "mutation_type",
            "n_depth",
            "name",
            "ncbi_build",
            "normal_bam_uuid",
            "normal_genotype",
            "occurrence_id",
            "omim_gene",
            "polyphen_impact",
            "polyphen_score",
            "protein_position",
            "pubmed",
            "ref_seq_accession",
            "reference_allele",
            "sift_impact",
            "sift_score",
            "ssm_id",
            "start_position",
            "swissprot",
            "symbol",
            "synonyms",
            "t_alt_count",
            "t_depth",
            "t_ref_count",
            "transcript_id",
            "transcripts",
            "trembl",
            "tumor_allele",
            "tumor_bam_uuid",
            "tumor_sample_barcode",
            "tumor_sample_uuid",
            "tumor_seq_allele1",
            "tumor_seq_allele2",
            "tumor_validation_allele1",
            "tumor_validation_allele2",
            "uniparc",
            "uniprotkb_swissprot",
            "validation_method",
            "variant_caller",
            "variant_process",
            "variant_type",
            "vep_impact",
        )

    def add_null(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Adds a null column to use as defaults for mappings.
        """
        return df.withColumn("empty", F.lit(None).cast(types.StringType()))

    def format_cosmic_id(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Turns StringType() cosmic_id field to ArrayType(StringType()) field
        """

        def to_array(cosmic_string):
            if cosmic_string is not None:
                if ";" in cosmic_string:
                    cosmic_string = cosmic_string.split(";")
                else:
                    cosmic_string = [cosmic_string]
            return cosmic_string

        to_array = F.udf(to_array, types.ArrayType(types.StringType()))
        df = df.withColumn("cosmic_id", to_array(df["cosmic_id"]))
        return df

    def add_available_variation_data(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Populates available_variation_data with ['ssm']
        for all cases with mutations
        WARNING: Requires that cases that have been tested in the calling
        pipelines be present in the MAF. If a case was tested but was not
        called, it should have an empty row with only the case_id
        """
        avd_udf = F.udf(
            lambda x, y: [] if (x is None and y is not None) else ["ssm"],
            types.ArrayType(types.StringType()),
        )
        return df.withColumn(
            "available_variation_data",
            avd_udf(F.col("tumor_sample_barcode"), F.col("case_id")),
        )

    def add_mutation_type(self, df: sql.DataFrame) -> sql.DataFrame:
        def mutation_type(mut_type):
            types = {"Somatic": "Simple Somatic Mutation"}
            if mut_type in types:
                return types[mut_type]
            else:
                return None

        mut_type_udf = F.udf(mutation_type, types.StringType())
        df = df.withColumn("mutation_type", mut_type_udf("mutation_type"))
        return df

    def format_chr(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Removes 'chr' from chromosome columns
        chr1 -> 1
        """
        return df.withColumn(
            "gene_chromosome",
            F.udf(lambda x: x.replace("chr", ""), types.StringType())(
                F.col("gene_chromosome")
            ),
        )

    def add_mutation_subtype(self, df: sql.DataFrame) -> sql.DataFrame:
        def subtype(variant_type):
            subtypes = {
                "SNP": "Single base substitution",
                "DEL": "Small deletion",
                "INS": "Small insertion",
                "DNP": "Di-nucleotide polymorphism",
                "TNP": "Tri-nucleotide polymorphism",
                "ONP": "Oligo-nucleotide polymorphism",
            }
            if variant_type in subtypes:
                return subtypes[variant_type]
            else:
                return None

        sub_type_udf = F.udf(subtype, types.StringType())
        df = df.withColumn("mutation_subtype", sub_type_udf("variant_type"))

        return df

    def add_normal_genotype(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Adds normal_genotype column to the MAF dataframe
        """
        maf_df = df.withColumn(
            "normal_genotype",
            F.struct(
                utils.uuid5_col(
                    F.col("match_norm_seq_allele1"), F.col("match_norm_seq_allele2")
                ).alias("allele_id")
            ),
        )
        return maf_df

    def add_ssm_id(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Adds ssm_id column to the MAF dataframe
        """
        maf_df = df.withColumn(
            "ssm_id",
            utils.uuid5_col(
                F.lit("ssm"),
                F.col("ncbi_build"),
                F.col("gene_chromosome"),
                F.col("start_position"),
                F.col("end_position"),
                F.col("mutation_subtype"),
                F.col("reference_allele"),
                F.col("tumor_allele"),
            ),
        )
        return maf_df

    def add_occurrence_id(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Adds the occurrence_id, a uuid hash of:
        'ssm_occurrence' + ssm_id + case_id
        """
        df = df.withColumn(
            "occurrence_id",
            utils.uuid5_col(F.lit("ssm_occurrence"), F.col("ssm_id"), F.col("case_id")),
        )
        return df

    def add_genomic_dna_change(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Adds the genomic_dna_change column
        """
        maf_df = df.withColumn(
            "genomic_dna_change",
            utils.ssm_label_col(
                F.col("gene_chromosome"),
                F.col("variant_type"),
                F.col("start_position"),
                F.col("end_position"),
                F.col("reference_allele"),
                F.col("tumor_allele"),
            ),
        )
        return maf_df

    def extract_cds_position(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Extracts cds_start, cds_length and cds_end from the cds_position column
        cds_position: 1273/2112 -> cds_start: 1273,
                                   cds_length: 2112,
                                   cds_end: 1273 + 2112
        cds_position: 1273-1274/2112 -> cds_start: 1273,
                                        cds_length: 2112,
                                        cds_end: 1273 + 2112
        """

        def start(s):
            s = (s and s.split("/")[0].split("-")[0].strip()) or -1
            if s in [-1, "?"]:
                return -1
            return int(s.split("/")[0].split("-")[0])

        def length(s):
            if not (s and s.split("/")[1].strip()):
                return -1
            return int(s.split("/")[1])

        def end(s):
            if -1 in [start(s), length(s)]:
                return -1
            return start(s) + length(s)

        df = df.withColumn(
            "cds_start", F.udf(start, types.IntegerType())(F.col("cds_position"))
        )
        df = df.withColumn(
            "cds_end", F.udf(end, types.IntegerType())(F.col("cds_position"))
        )
        df = df.withColumn(
            "cds_length", F.udf(length, types.IntegerType())(F.col("cds_position"))
        )
        return df

    def _get_urls(self, urls: Optional[Iterable[str]]) -> Iterable[str]:
        urls = urls or self.urls

        if urls is None:
            self.logger.error("Urls not passed")
            raise Exception("No MAF urls were found to load.")

        return urls

    def _load_url(self, url: str) -> Optional[sql.DataFrame]:
        try:
            return pyspark_extensions.default_columns(
                self.file_to_df(url),
                (
                    pyspark_extensions.DefaultColumn(name="normal_bam_uuid"),
                    pyspark_extensions.DefaultColumn(name="tumor_bam_uuid"),
                    pyspark_extensions.DefaultColumn(
                        name="callers", value="FM Simple Somatic Mutation"
                    ),
                ),
            )

        except Exception as e:
            self.logger.error(e)

        return None

    def combine(self, urls=None) -> sql.DataFrame:
        """
        Combines data frames from a list of urls
        """
        urls = self._get_urls(urls)
        base_df = self.sqlContext.createDataFrame((), schema=self.raw_maf_schema)
        dfs = filter(None, (self._load_url(url) for url in urls))
        df = functools.reduce(lambda df0, df1: df0.unionByName(df1), dfs, base_df)

        df = df.select(
            F.col("ESP_AA_AF").alias("aa_change"),
            F.col("ESP_AA_AF").alias("aa_end"),
            F.col("ESP_AA_AF").alias("aa_start"),
            "all_effects",
            F.col("Amino_acids").alias("amino_acids"),
            "case_id",
            F.col("CCDS").alias("ccds"),
            F.col("cDNA_position").alias("cdna_position"),
            F.col("CDS_position").alias("cds_position"),
            F.col("Center").alias("center"),
            F.col("CLIN_SIG").alias("clin_sig"),
            F.col("Codons").alias("codons"),
            F.col("Consequence").alias("consequence_type"),
            F.col("COSMIC").alias("cosmic_id"),
            F.col("dbSNP_RS").alias("dbsnp_rs"),
            F.col("dbSNP_Val_Status").alias("dbsnp_val_status"),
            F.col("DOMAINS").alias("domains"),
            F.col("End_Position").cast(types.IntegerType()).alias("end_position"),
            F.col("ENSP").alias("ensp"),
            F.col("Existing_variation").alias("existing_variation"),
            F.col("Chromosome").alias("gene_chromosome"),
            F.col("Gene").alias("gene_id"),
            F.col("HGVSc").alias("hgvsc"),
            F.col("HGVSp").alias("hgvsp"),
            F.col("HGVSp_Short").alias("hgvsp_short"),
            F.col("CANONICAL").cast(types.BooleanType()).alias("is_canonical"),
            F.col("Match_Norm_Seq_Allele1").alias("match_norm_seq_allele1"),
            F.col("Match_Norm_Seq_Allele2").alias("match_norm_seq_allele2"),
            F.col("Matched_Norm_Sample_Barcode").alias("matched_norm_sample_barcode"),
            F.col("Matched_Norm_Sample_UUID").alias("matched_norm_sample_uuid"),
            F.col("Mutation_Status").alias("mutation_status"),
            F.col("Mutation_Status").alias("mutation_type"),
            F.col("n_depth").cast(types.IntegerType()),
            F.col("NCBI_Build").alias("ncbi_build"),
            "normal_bam_uuid",
            F.col("PolyPhen").alias("polyphen"),
            F.col("Protein_position").alias("protein_position"),
            F.col("PUBMED").alias("pubmed"),
            F.col("ESP_AA_AF").alias("ref_seq_accession"),
            F.col("Reference_Allele").alias("reference_allele"),
            F.col("SIFT").alias("sift"),
            F.col("Start_Position").cast(types.IntegerType()).alias("start_position"),
            F.col("SWISSPROT").alias("swissprot"),
            F.col("t_alt_count").cast(types.IntegerType()),
            F.col("t_depth").cast(types.IntegerType()),
            F.col("t_ref_count").cast(types.IntegerType()),
            F.col("Transcript_ID").alias("transcript_id"),
            F.col("TREMBL").alias("trembl"),
            F.col("Allele").alias("tumor_allele"),
            "tumor_bam_uuid",
            F.col("Tumor_Sample_Barcode").alias("tumor_sample_barcode"),
            F.col("Tumor_Sample_UUID").alias("tumor_sample_uuid"),
            F.col("Tumor_Seq_Allele1").alias("tumor_seq_allele1"),
            F.col("Tumor_Seq_Allele2").alias("tumor_seq_allele2"),
            F.col("Tumor_Validation_Allele1").alias("tumor_validation_allele1"),
            F.col("Tumor_Validation_Allele2").alias("tumor_validation_allele2"),
            F.col("UNIPARC").alias("uniparc"),
            F.col("Validation_Method").alias("validation_method"),
            F.col("callers").alias("variant_caller"),
            F.col("Variant_Type").alias("variant_type"),
            F.col("IMPACT").alias("vep_impact"),
        )

        return df
