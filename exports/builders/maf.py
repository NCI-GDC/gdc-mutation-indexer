import yaml
import logging

from pyspark.sql.types import StringType, IntegerType, ArrayType
from pyspark.sql.functions import lit, col, regexp_extract, udf, struct
from elasticsearch import Elasticsearch
from pyspark.sql.utils import AnalysisException

from exports.builders.utils import (
    uuid5_col,
    ssm_label_col,
    extract_sift_polyphen,
)

from exports.es_utils import (
    iterate_es_results,
)

from exports.builders.base_input_builder import BaseInputBuilder
from exports.builders.gene_model import GeneModelBuilder

from pkg_resources import resource_filename

logging.basicConfig()


class MAFBuilder(BaseInputBuilder):
    """
    Class responsible for assembling maf files into a single dataframe with
    uniform features
    """

    def __init__(self, config, sqlContext):
        super(MAFBuilder, self).__init__(config, sqlContext, 'maf')
        self.acls = self.get_acls()

    def build(self):
        """
        Builds a master MAF dataframe by combining individual MAFs and
        augmenting them with additional features
        """
        if self.config.maf_use_existing:
            df = self.get_existing()
            return df

        df = self.combine()
        # Warn:this will strip anything out of the maf that isnt in the schema
        df = self.standardize_schema(df,
                                     default_to_none=['normal_bam_uuid',
                                                      'tumor_bam_uuid'])

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
        # Create observation_id
        df = self.add_observation_id(df)
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

        # Write data
        if self.config.maf_keep:
            self.df_to_s3(df, self.config.maf_path,
                          overwrite=self.config.maf_overwrite)

        self.logger.info('Repartitioning MAF dataframe')
        df = df.repartition(self.config.repartition, 'ssm_id')

        if self.config.cache_dataframes['mafs']:
            self.logger.info('Caching repartitioned MAF dataframe')
            df.cache().count()

        return df

    def map_transform(self, df):
        """
        Transforms maf_df according to maf.yml :type and :pattern
        """
        for column in df.columns:
            if column in self.schema:
                if 'type' in self.schema[column]:
                    val_type = self.schema[column]['type']
                    assert val_type in ['float', 'int', 'str', 'bool']
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
        path = resource_filename('exports.schemas', 'maf.yml')
        with open(path) as f:
            maf_schema = yaml.load(f)['maf_schema']

        # Save maf_schema for later use
        self.schema = maf_schema

        # Select and rename maf fields according to schema
        try:
            maf_df = df.select(*(col(v['name']).alias(k)
                                 for k, v in maf_schema.items()))
        except AnalysisException:

            if default_to_none is None:
                default_to_none = []

            maf_df = df.select(*(col(v['name']).alias(k)
                                 for k, v in maf_schema.items()
                                 if k not in default_to_none))

            # Add null valued columns
            for col_name in default_to_none:
                maf_df = maf_df.withColumn(col_name, lit(None).cast(StringType()))

        return maf_df

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

    def get_acls(self):
        """
        1. Take list of maf file names
        2. Assume the last part of the url is the file_name
        3. Look up corresponding files in es
        4. Parse out those files' acls
        """

        es = Elasticsearch(self.config.es_host,
                           port=self.config.es_port,
                           http_auth=(self.config.es_user,
                                      self.config.es_pass))

        file_names = self.config.get_maf_file_names()

        query = {
                "query": {
                    "bool": {
                        "must": {
                            "terms": {
                                "file_name": file_names
                                }
                            }
                        }
                    },
                "_source": ["file_name", "acl"]
        }

        # Build up dictionary of file_name to acl
        filenames_to_acls = {}
        for doc in iterate_es_results(es, self.config.graph_index, 'file', query=query):
            source = doc['_source']
            filename = source['file_name']
            acl = source['acl']

            filenames_to_acls[filename] = acl

        return filenames_to_acls

    def add_acl(self, df, url):
        """
        Populates mutation data with acls
        Have to do a little massaging of the file name to match
        Mapped on the maf name level
        """
        acls = self.acls

        def acl_inner():
            try:
                # trim out leading folders
                file_name = url.split('/')[-1]

                # mafs may be zipped or unzipped
                # we expect the file_name in the File to be 'xxx.gz'
                if not file_name.endswith('.gz'):
                    file_name += '.gz'

                return acls[file_name]

            except KeyError:

                raise Exception("ACL not found for maf with url {}, "
                                "file_name {}".format(url, file_name))

        acl_udf = udf(acl_inner, ArrayType(StringType()))
        return df.withColumn('acl', acl_udf())

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
                             avd_udf(col('Tumor_Sample_Barcode'),
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
                'INS': 'Small insertion'
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
        Adds the observation_id, a uuid hash of:
        'ssm_occurrence' + ssm_id + case_id
        """
        df = df.withColumn('occurrence_id',
                           uuid5_col(lit('ssm_occurrence'),
                                     col('ssm_id'),
                                     col('case_id')))
        return df

    def add_observation_id(self, df):
        """
        Adds the observation_id, a uuid hash of:
        occurrence_id+tumor_sample_uuid+matched_norm_sample_uuid+variant_caller+variant_process
        """
        df = df.withColumn('observation_id',
                           uuid5_col(lit('ssm_observation'),
                                     col('occurrence_id'),
                                     col('tumor_sample_uuid'),
                                     col('matched_norm_sample_uuid'),
                                     col('variant_caller'),
                                     lit('masked')))
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

    def extract_barcode(self, df):
        """
        Extracts the case barcode from the sample barcode
        TODO: Remove this as it only works for TCGA. Should look up case uuid
              from the sample uuid
        """
        maf_df = df.withColumn('_case_submitter_id',
                               regexp_extract(col('tumor_sample_barcode'),
                                              '([A-Z]{4}-[A-Z0-9]{2}-[A-Z0-9]{4})',
                                              1))
        return maf_df

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
            caller = self.get_caller(url)
            try:
                # TODO: separate data transforms from combining multiple df into one
                # latter should go as a static method to base class for MAF and Gistic Builders
                new_df = self.s3_to_df(url)
                new_df = new_df.withColumn('variant_caller', lit(caller))
                # add acl based on individual maf
                new_df = self.add_acl(new_df, url)

                self.logger.info('Read {} rows from {}'.format(new_df.count(), url))
                if df is None:
                    df = new_df
                else:
                    df = df.unionAll(new_df)
            except Exception as e:
                self.logger.error(e)

        assert df is not None

        self.config.nb_mutations = df.count()
        self.logger.info('Combined {} files for a total of {} rows'
                         .format(len(urls), self.config.nb_mutations))
        self.df = df
        return df

    def get_caller(self, url):
        """
        Identify variant caller by portion of url name.
        """

        possible_callers = ['mutect', 'muse', 'varscan', 'somaticsniper', 'FM']

        try:
            caller = [c for c in possible_callers if c in url][0]
            if caller == 'mutect':
                caller += '2'
            if caller == 'FM':
                caller += ' Simple Somatic Mutation'

        except IndexError:
            raise Exception("Cannot identify caller for url {}".format(url))

        return caller

    def patch_url(self, url):
        """
        changes domain/bucket to bucket format
        s3:// -> s3a://
        """
        url = url.replace('cleversafe.service.consul/somatic_maf', 'test')
        url = url.replace('s3://', 's3a://')
        return url

