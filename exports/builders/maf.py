import logging
from pkg_resources import resource_filename

import yaml
from pyspark.sql.functions import col, lit, lower, struct, udf
from pyspark.sql.types import StringType, IntegerType, ArrayType

from config import LOG_FORMAT
from exports.builders.utils import (
    uuid5_col,
    ssm_label_col,
    extract_sift_polyphen,
)

from exports.builders.base_input_builder import BaseInputBuilder
from exports.builders.gene_model import GeneModelBuilder
from exports.builders.clinical_annotations.civic import CivicBuilder

logging.basicConfig(format=LOG_FORMAT)


class MAFBuilder(BaseInputBuilder):
    """
    Class responsible for assembling maf files into a single dataframe with
    uniform features
    """

    def __init__(self, config, sqlContext):
        super(MAFBuilder, self).__init__(config, sqlContext, 'maf')
        self.schema = self.get_schema()
        self.annotation_builders = [CivicBuilder(config, sqlContext)]

    def build_from_cache(self, df):
        return df

    def build_from_scratch(self):
        """
        Builds a master MAF dataframe by combining individual MAFs and
        augmenting them with additional features
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
        df = extract_sift_polyphen(df)
        # Build gene model and join with MAF dataframe
        gm_df = GeneModelBuilder(self.config, self.sqlContext).build()

        cols_to_drop = [c for c in gm_df.columns]
        df = df.select(*[c for c in df.columns if c not in cols_to_drop])
        df = df.join(gm_df, df.gene_id == gm_df._gene_id, 'inner')
        df = df.drop('_gene_id')
        df = self.add_null(df)
        df = self.add_canonical_transcript_lengths(df)
        df = self.add_normal_genotype(df)
        df = self.map_transform(df)
        df = df.withColumn('variant_process', lit('masked'))
        df = self.format_chr(df)
        df = self.format_cosmic_id(df)
        for builder in self.annotation_builders:
            df = builder.merge_with_maf(df)

        self.logger.info('Repartitioning MAF dataframe')
        df = df.repartition(self.config.df_repartition, 'ssm_id')

        if self.config.cache_dataframes['mafs']:
            self.logger.info('Caching repartitioned MAF dataframe')
            df.cache().count()

        return df

    def get_annotation_schemas(self):
        return [ann.schema for ann in self.annotation_builders]

    def map_transform(self, df):
        """
        Transforms maf_df according to maf.yml :type and :pattern
        """
        for column in df.columns:
            if column in self.schema:
                if 'type' in self.schema[column]:
                    val_type = self.schema[column]['type']
                    assert val_type in ['float', 'int', 'str', 'boolean']
                    df = df.withColumn(column, df[column].cast(val_type))

                elif 'pattern' in self.schema[column]:
                    pattern = self.schema[column]['pattern']

                    def apply_pattern(value):
                        return pattern.format(value)
                    df = df.withColumn(column,
                                       udf(apply_pattern,
                                           StringType())(df[column]))
                else:
                    pass
        return df

    def add_null(self, df):
        """
        Adds a null column to use as defaults for mappings.
        """
        return df.withColumn('empty', lit(None).cast(StringType()))

    def standardize_schema(self, df, default_to_none=None):
        """
        Renames and select required columns from the MAF documents
        """
        if default_to_none is None:
            default_to_none = []

        df_columns = set(df.columns)

        # Map old columns to their new names as given in the schema.
        # Some columns are optional; supply None values for those as specified.
        def standardize(new_column, props):
            old_column = props['name']
            if old_column in df_columns:
                return col(old_column).alias(new_column)
            elif old_column in default_to_none:
                return lit(None).cast(StringType()).alias(new_column)
            else:
                raise KeyError(
                    'Required column {} missing from MAF'.format(old_column))

        # Iterate over the output schema rather than the input dataframe.
        # As long as we don't modify the schema after loading it, this should
        # ensure that we output columns in a consistent order.
        return df.select(*[standardize(k, v) for k, v in self.schema.items()])

    def get_schema(self):
        """
        Load the intended MAF schema from the local YAML file
        """
        path = resource_filename('exports.schemas', 'maf.yml')
        with open(path) as f:
            return yaml.safe_load(f)['maf_schema']

    def format_cosmic_id(self, df):
        """
        Turns StringType() cosmic_id field to ArrayType(StringType()) field
        """

        def to_array(cosmic_string):
            if cosmic_string is not None:
                if ';' in cosmic_string:
                    cosmic_string = cosmic_string.split(';')
                else:
                    cosmic_string = [cosmic_string]
            return cosmic_string

        to_array = udf(to_array, ArrayType(StringType()))
        df = df.withColumn('cosmic_id', to_array(df['cosmic_id']))
        return df

    def add_available_variation_data(self, df):
        """
        Populates available_variation_data with ['ssm']
        for all cases with mutations
        WARNING: Requires that cases that have been tested in the calling
        pipelines be present in the MAF. If a case was tested but was not
        called, it should have an empty row with only the case_id
        """
        avd_udf = udf(lambda x, y:
                      [] if (x is None and y is not None) else ['ssm'],
                      ArrayType(StringType()))
        return df.withColumn('available_variation_data',
                             avd_udf(col('tumor_sample_barcode'),
                                     col('case_id')))

    def add_mutation_type(self, df):

        def mutation_type(mut_type):
            types = {'Somatic': 'Simple Somatic Mutation'}
            if mut_type in types:
                return types[mut_type]
            else:
                return None

        mut_type_udf = udf(mutation_type, StringType())
        df = df.withColumn('mutation_type', mut_type_udf('mutation_type'))
        return df

    def format_chr(self, df):
        """
        Removes 'chr' from chromosome columns
        chr1 -> 1
        """
        return df.withColumn('gene_chromosome',
                             udf(lambda x: x.replace('chr', ''),
                                 StringType())(col('gene_chromosome')))

    def add_mutation_subtype(self, df):

        def subtype(variant_type):
            subtypes = {
                'SNP': 'Single base substitution',
                'DEL': 'Small deletion',
                'INS': 'Small insertion',
                'DNP': 'Di-nucleotide polymorphism',
                'TNP': 'Tri-nucleotide polymorphism',
                'ONP': 'Oligo-nucleotide polymorphism',
            }
            if variant_type in subtypes:
                return subtypes[variant_type]
            else:
                return None

        sub_type_udf = udf(subtype, StringType())
        df = df.withColumn('mutation_subtype', sub_type_udf('variant_type'))

        return df

    def add_normal_genotype(self, df):
        """
        Adds normal_genotype column to the MAF dataframe
        """
        maf_df = df.withColumn('normal_genotype',
                               struct(uuid5_col(col('match_norm_seq_allele1'),
                                                col('match_norm_seq_allele2'))
                                      .alias('allele_id')))
        return maf_df

    def add_ssm_id(self, df):
        """
        Adds ssm_id column to the MAF dataframe
        """
        maf_df = df.withColumn('ssm_id', uuid5_col(lit('ssm'),
                                                   col('ncbi_build'),
                                                   col('chromosome'),
                                                   col('start_position'),
                                                   col('end_position'),
                                                   col('mutation_subtype'),
                                                   col('reference_allele'),
                                                   col('tumor_allele')))
        return maf_df

    def add_occurrence_id(self, df):
        """
        Adds the occurrence_id, a uuid hash of:
        'ssm_occurrence' + ssm_id + case_id
        """
        df = df.withColumn('occurrence_id',
                           uuid5_col(lit('ssm_occurrence'),
                                     col('ssm_id'),
                                     col('case_id')))
        return df

    def add_genomic_dna_change(self, df):
        """
        Adds the genomic_dna_change column
        """
        maf_df = df.withColumn('genomic_dna_change',
                               ssm_label_col(col('chromosome'),
                                             col('variant_type'),
                                             col('start_position'),
                                             col('end_position'),
                                             col('reference_allele'),
                                             col('tumor_allele')))
        return maf_df

    def extract_cds_position(self, df):
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
            s = (s and s.split('/')[0].split('-')[0].strip()) or -1
            if s in [-1, '?']:
                return -1
            return int(s.split('/')[0].split('-')[0])

        def length(s):
            if not (s and s.split('/')[1].strip()):
                return -1
            return int(s.split('/')[1])

        def end(s):
            if -1 in [start(s), length(s)]:
                return -1
            return start(s) + length(s)

        df = df.withColumn('cds_start', udf(start,
                                            IntegerType())(col('cds_position')))
        df = df.withColumn('cds_end', udf(end,
                                          IntegerType())(col('cds_position')))
        df = df.withColumn('cds_length', udf(length,
                                             IntegerType())(col('cds_position')))
        return df

    def combine(self, urls=None):
        """
        Combines data frames from a list of urls
        """
        if urls is None and self.urls is not None:
            urls = self.urls
        elif urls is None and self.urls is None:
            self.logger.error('Urls not passed')
            raise Exception
        df = None

        for url in urls:
            try:
                # TODO: separate data transforms from combining multiple df into one
                #   latter should go as a static method to base class for MAF and Gistic Builders
                new_df = self.file_to_df(url)

                # ensure a consistent schema so that the union works correctly
                # this will strip out any columns that aren't in the schema,
                # but we should not need those columns
                new_df = self.standardize_schema(
                    new_df, default_to_none=['normal_bam_uuid',
                                             'tumor_bam_uuid'])

                if self.config.debug:
                    self.logger.info('Read {} rows from {}'.format(new_df.count(),
                                                                   url))
                if df is None:
                    df = new_df
                else:
                    df = df.union(new_df)
            except Exception as e:
                self.logger.error(e)

        assert df is not None

        if self.config.debug:
            self.config.nb_mutations = df.count()
            self.logger.info('Combined {} files for a total of {} rows'
                             .format(len(urls), self.config.nb_mutations))
        self.df = df
        return df

    def patch_url(self, url):
        """
        changes domain/bucket to bucket format
        s3:// -> s3a://
        """
        url = url.replace('cleversafe.service.consul/somatic_maf', 'test')
        url = url.replace('s3://', 's3a://')
        return url
