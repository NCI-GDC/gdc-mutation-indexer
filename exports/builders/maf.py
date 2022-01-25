import logging
from typing import Any, Dict, Iterable

import yaml
from pkg_resources import resource_filename
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

import config
from exports import pyspark_extensions
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
        self.schema = self.get_schema()
        self.annotation_builders = annotation_builders

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

        cols_to_drop = frozenset(gene_model_df.columns)
        df = df.select(*[c for c in df.columns if c not in cols_to_drop])
        df = df.join(gene_model_df, df.gene_id == gene_model_df._gene_id, "inner")
        df = df.drop("_gene_id")
        df = self.add_null(df)
        df = self.add_canonical_transcript_lengths(df)
        df = self.add_normal_genotype(df)
        df = self.map_transform(df)
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

        return df

    def get_annotation_schemas(self):
        return [ann.schema for ann in self.annotation_builders]

    def map_transform(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Transforms maf_df according to maf.yml :type and :pattern
        """
        for column in df.columns:
            if column in self.schema:
                if "type" in self.schema[column]:
                    val_type = self.schema[column]["type"]
                    assert val_type in ["float", "int", "str", "boolean"]
                    df = df.withColumn(column, df[column].cast(val_type))

                elif "pattern" in self.schema[column]:
                    pattern = self.schema[column]["pattern"]

                    def apply_pattern(value):
                        return pattern.format(value)

                    df = df.withColumn(
                        column, F.udf(apply_pattern, types.StringType())(df[column])
                    )
                else:
                    pass
        return df

    def add_null(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Adds a null column to use as defaults for mappings.
        """
        return df.withColumn("empty", F.lit(None).cast(types.StringType()))

    def standardize_schema(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Renames and select required columns from the MAF documents
        """
        df_columns = frozenset(df.columns)

        # Map old columns to their new names as given in the schema.
        # Some columns are optional; supply None values for those as specified.
        def standardize(new_column, props):
            old_column = props["name"]
            if old_column in df_columns:
                return F.col(old_column).alias(new_column)
            else:
                raise KeyError("Required column {} missing from MAF".format(old_column))

        # Iterate over the output schema rather than the input dataframe.
        # As long as we don't modify the schema after loading it, this should
        # ensure that we output columns in a consistent order.
        return df.select(*[standardize(k, v) for k, v in self.schema.items()])

    def get_schema(self) -> Dict[str, Dict[str, str]]:
        """
        Load the intended MAF schema from the local YAML file
        """
        path = resource_filename("exports.schemas", "maf.yml")
        with open(path) as f:
            return yaml.safe_load(f)["maf_schema"]

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
                F.col("chromosome"),
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
                F.col("chromosome"),
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

    def combine(self, urls=None) -> sql.DataFrame:
        """
        Combines data frames from a list of urls
        """
        df = None  # type Optional[sql.DataFrame]
        urls = urls or self.urls

        if urls is None:
            self.logger.error("Urls not passed")
            raise Exception

        for url in urls:
            try:
                # TODO: separate data transforms from combining multiple df into one
                #   latter should go as a static method to base class for MAF and Gistic Builders
                new_df = self.file_to_df(url)

                new_df = pyspark_extensions.default_columns(
                    new_df,
                    (
                        pyspark_extensions.DefaultColumn(name="normal_bam_uuid"),
                        pyspark_extensions.DefaultColumn(name="tumor_bam_uuid"),
                        pyspark_extensions.DefaultColumn(
                            name="callers", value="FM Simple Somatic Mutation"
                        ),
                    ),
                )

                # ensure a consistent schema so that the union works correctly
                # this will strip out any columns that aren't in the schema,
                # but we should not need those columns
                new_df = self.standardize_schema(new_df)

                if self.config.debug:
                    self.logger.info("Read {} rows from {}".format(new_df.count(), url))
                if df is None:
                    df = new_df
                else:
                    df = df.union(new_df)
            except Exception as e:
                self.logger.error(e)

        assert df is not None

        if self.config.debug:
            self.config.nb_mutations = df.count()
            self.logger.info(
                "Combined {} files for a total of {} rows".format(
                    len(urls), self.config.nb_mutations
                )
            )
        self.df = df
        return df

    def patch_url(self, url: str) -> str:
        """
        changes domain/bucket to bucket format
        s3:// -> s3a://
        """
        url = url.replace("cleversafe.service.consul/somatic_maf", "test")
        url = url.replace("s3://", "s3a://")

        return url
