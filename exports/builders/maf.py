import os
import yaml
import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import lit, col, regexp_extract, udf

from exports.builders.utils import uuid5_col, ssm_label_col
from exports.builders.gene_model import GeneModelBuilder


class MAFBuilder(object):
    """
    Class responsible for assembling maf files into a single dataframe with
    uniform features
    """

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext

        if config.maf_urls is not None:
            self.urls = config.maf_urls
        else:
            self.urls = self.get_urls()

    def build(self):
        """
        Builds a master MAF dataframe by combining individual MAFs and
        augmenting them with additional features
        """
        if self.config.maf_use_existing:
            try:
                df = self.get_existing()
                return df
            except IOError:
                self.logger.info('Couldn\'t find existing maf file at given path')

        df = self.combine()
        # Warn:this will strip anything out of the maf that isnt in the schema
        df = self.standardize_schema(df)
        # ssm_id from hashing unique columns in the maf
        df = self.add_ssm_id(df)
        # Add label identifying the mutation
        df = self.add_genomic_dna_change(df)
        # Add mutation_type
        df = self.add_mutation_type(df)
        # Add mutation_subtype
        df = self.add_mutation_subtype(df)
        # Get cds columns from cds_position
        df = self.extract_cds_position(df)
        # Build gene model and join with MAF dataframe
        gm_df = GeneModelBuilder(self.config, self.sqlContext).build()
        cols_to_drop = [c for c in gm_df.columns]
        df = df.select(*[c for c in df.columns if c not in cols_to_drop])
        df = df.join(gm_df, df.gene_id == gm_df._gene_id, 'inner')
        df = df.drop('_gene_id')
        df = self.add_null(df)
        df = self.add_canonical_lengths(df)
        df = self.add_normal_genotype(df)
        df = self.map_transform(df)
        df = df.withColumn('variant_process', lit('masked'))
        df = self.format_chr(df)

        # Write data
        if self.config.maf_keep:
            self.write(df)

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
                                       udf(apply_pattern, StringType())(df[column]))
                else:
                    pass
        return df

    def add_null(self, df):
        """
        Adds a null column to use as defaults for mappings.
        """
        return df.withColumn('empty', lit(None).cast(StringType()))

    def standardize_schema(self, df):
        """
        Renames and select required columns from the MAF documents
        """
        #path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../schemas/maf.yml'))
        path = os.path.abspath('exports/schemas/maf.yml')
        with open(path) as f:
            maf_schema = yaml.load(f)['maf_schema']

        # Save maf_schema for later use
        self.schema = maf_schema

        # Select and rename maf fields according to schema
        maf_df = df.select(*(col(v['name']).alias(k)
                             for k, v in maf_schema.items()))

        return maf_df

    def add_canonical_lengths(self, df):
        """
        Adds canonical_transcript_length{'','cds','genomic'} fields to a dataframe
        """

        def integer_udf(function):
            """ Spark IntegerType udf decorator """
            return udf(function, IntegerType())

        @integer_udf
        def len_udf(transcripts):
            for t in transcripts:
                if t['is_canonical']:
                    if 'length' in t:
                        return t['length']
                    else:
                        return None

        @integer_udf
        def len_cds_udf(transcripts):
            for t in transcripts:
                if t['is_canonical']:
                    if 'length_cds' in t:
                        return t['length_cds']
                    else:
                        return None

        @integer_udf
        def len_gen_udf(transcripts):
            for t in transcripts:
                if t['is_canonical']:
                    return int(t['end']) - int(t['start']) + 1

        df = df.withColumn('canonical_transcript_length',
                           len_udf(df.transcripts))
        df = df.withColumn('canonical_transcript_length_cds',
                           len_cds_udf(df.transcripts))
        df = df.withColumn('canonical_transcript_length_genomic',
                           len_gen_udf(df.transcripts))
        return df

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
                               uuid5_col(col('normal_allele1'),
                                         col('normal_allele2')))
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
                                                   col('variant_type'),
                                                   col('reference_allele'),
                                                   col('tumor_allele')))
        return maf_df

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
        Extracts cds_start and cds_length from the cds_position column
        cds_position: 1273/2112 -> cds_start: 1273, cds_length: 2112
        cds_position: 1273-1274/2112 -> cds_start: 1273, cds_length: 2112
        """
        def start(s):
            if not (s and s.split('/')[0].split('-')[0].strip()):
                return -1
            return int(s.split('/')[0].split('-')[0])

        def length(s):
            if not (s and s.split('/')[1].strip()):
                return -1
            return int(s.split('/')[1])

        df = df.withColumn('cds_start', udf(start,
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
            self.logger.error('Urls not passed and get_urls() not yet called')
            raise Exception
        df = None
        callers = ['mutect', 'muse', 'varscan', 'somaticsniper']
        for url in urls:
            caller = [ c for c in callers if c in url ][0]
            if caller == 'mutect':
                caller += '2'
            try:
                new_df = self.read_maf(url)
                new_df = new_df.withColumn('variant_caller', lit(caller))
                self.logger.info('Read {} rows from {}'.format(new_df.count(), url))
                if df is None:
                    df = new_df
                else:
                    df = df.unionAll(new_df)
            except BaseException as e:
                self.logger.error(e)

        self.config.nb_mutations = df.count()
        self.logger.info('Combined {} files for a total of {} rows'
                            .format(len(urls), self.config.nb_mutations))
        self.df = df
        return df

    def get_urls(self):
        """
        Retrieve file ids from the api then gets the s3 urls from signpost
        """
        filt = {
            "op": "and",
            "content": [{
                    "op": "in",
                    "content": {
                        "field": "files.data_format",
                        "value": ["MAF"]
                    }
                }, {
                    "op": "in",
                    "content": {
                        "field": "files.access",
                        "value": ["open"]
                    }
                }
            ]
        }

        filt = {
            "filters": json.dumps(filt),
            "size": "1000",
            "fields": "file_id"
        }

        r = requests.get('{}/files?pretty=true'.format(self.config.api_host),
                         params=filt, verify=False)
        file_ids = [f['file_id'] for f in r.json()['data']['hits']]

        urls = []
        for fid in file_ids:
            r = requests.get('{}/v0/did/{}'.format(self.config.signpost_host, fid))
            url = r.json()['urls'][0]
            urls.append(url)

        self.logger.info('Found urls for {} files'.format(len(urls)))

        return urls

    def patch_url(self, url):
        """
        changes domain/bucket to bucket format
        s3:// -> s3a://
        """
        url = url.replace('cleversafe.service.consul/somatic_maf', 'test')
        url = url.replace('s3://', 's3a://')
        return url

    def read_maf(self, url):
        """
        Read and return a single MAF from the given s3 url
        """
        return self.sqlContext.read.format('com.databricks.spark.csv')\
                   .options(header='true')\
                   .options(comment="#")\
                   .options(delimiter='\t')\
                   .options(codec="org.apache.hadoop.io.compress.GzipCodec")\
                   .load(url)

    def get_existing(self):
        """
        Loads a built combined maf
        """
        df = self.sqlContext.read.format('com.databricks.spark.csv')\
                            .options(header='true', inferschema='true')\
                            .load(self.config.maf_path)\
                            .drop_duplicates()

        self.config.nb_mutations = df.count()
        return df

    def write(self, df):
        """
        Writes the combined maf file
        """
        writer = df.write.format('com.databricks.spark.csv')
        if self.config.maf_overwrite:
            writer = writer.mode('overwrite')
        writer = writer.options(header='true').save(self.config.maf_path)
