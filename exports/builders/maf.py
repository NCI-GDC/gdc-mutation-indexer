import os
import yaml
import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.types import StringType
from pyspark.sql.functions import lit, col, regexp_extract

from exports.builders.utils import ssm_uuid_udf
from exports.builders.gene_model import GeneModelBuilder


class MAFBuilder(object):
    '''
    Class responsible for assembling maf files into a single dataframe with
    uniform features
    '''

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext

        if config.maf_urls is not None:
            self.urls = config.maf_urls
        else:
            self.urls = self.get_urls()

    def build(self):
        '''
        Builds a master MAF dataframe by combining individual MAFs and
        augmenting them with additional features
        '''
        if self.config.maf_use_existing:
            try:
                df = self.get_existing()
                return df
            except IOError:
                self.logger.info('Couldn\'t find existing maf file at given path')

        df = self.combine()
        df = df.fillna('')
        df = self.add_null(df)
        df = self.standardize_schema(df)
        df = self.add_ssm_id(df)
        df = self.extract_barcode(df)

        # Build gene model and join with MAF dataframe
        gm_df = GeneModelBuilder(self.config, self.sqlContext).build()
    
        cols_to_drop = [c for c in gm_df.columns]
        df = df.select(*[c for c in df.columns if c not in cols_to_drop])
        
        df = df.join(gm_df, df.gene_id == gm_df._gene_id, 'inner')

        df = df.drop('_gene_id')

        # Write data
        if self.config.maf_keep:
            self.write(df)
        return df


    def add_null(self, df):
        '''
        Adds a null column to use as defaults for mappings.
        '''
        return df.withColumn('empty', lit('').cast(StringType()))

    def standardize_schema(self, df):
        '''
        Renames and select required columns from the MAF documents
        '''
        #path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../schemas/maf.yml'))
        path = os.path.abspath('exports/schemas/maf.yml')
        with open(path) as f:
            maf_schema = yaml.load(f)['maf_schema']
        maf_df = df.select(*( col(v).alias(k) for k, v in maf_schema.items() ))
        return maf_df

    def add_ssm_id(self, df):
        '''
        Adds ssm_id column to the MAF dataframe
        '''
        ssm_func = ssm_uuid_udf(self.config.ssm_namespace)
        maf_df = df.withColumn('ssm_id', ssm_func(col('chromosome'),
                                                  col('variant_type'),
                                                  col('start_position'),
                                                  col('end_position'),
                                                  col('reference_allele'),
                                                  col('tumor_allele')))
        return maf_df

    def extract_barcode(self, df):
        '''
        Extracts the case barcode from the sample barcode
        TODO: Remove this as it only works for TCGA. Should look up case uuid
              from the sample uuid
        '''
        maf_df = df.withColumn('_case_submitter_id',
                                   regexp_extract(col('tumor_sample_barcode'),
                                         '([A-Z]{4}-[A-Z0-9]{2}-[A-Z0-9]{4})',1))
        return maf_df

    def combine(self, urls=None):
        '''
        Combines data frames from a list of urls
        '''
        if urls is None and self.urls is not None:
            urls = self.urls
        elif urls is None and self.urls is None:
            self.logger.error('Urls not passed and get_urls() not yet called')
            raise Exception
        df = None
        callers = ['mutect','muse','varscan','somaticsniper']
        for url in urls:
            caller = [ c for c in callers if c in url ][0]
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
        
        self.logger.info('Combined {} files for a total of {} rows'
                         .format(len(urls), df.count()))
        self.df = df
        return df

    def get_urls(self):
        '''
        Retrieve file ids from the api then gets the s3 urls from signpost
        '''
        filt = {
            "op":"and",
            "content":[{
                    "op":"in",
                    "content":{
                        "field":"files.data_format",
                        "value":["MAF"]
                    }
                },{
                    "op":"in",
                    "content":{
                        "field":"files.access",
                        "value":["open"]
                    }
                }
            ]
        }

        filt = {
            "filters":json.dumps(filt),
            "size":"1000",
            "fields":"file_id"
        }
        
        r = requests.get('{}/files?pretty=true'.format(self.config.api_host),
                            params=filt, verify=False)
        file_ids = [ f['file_id'] for f in r.json()['data']['hits'] ]

        urls = []
        for fid in file_ids:
            r = requests.get('{}/v0/did/{}'.format(self.config.signpost_host, fid))
            url = r.json()['urls'][0]
            urls.append(url)

        self.logger.info('Found urls for {} files'.format(len(urls)))

        return urls

    def patch_url(self, url):
        '''
        changes domain/bucket to bucket format
        s3:// -> s3a://
        '''
        url = url.replace('cleversafe.service.consul/somatic_maf', 'test')
        url = url.replace('s3://', 's3a://')
        return url

    def read_maf(self, url):
        '''
        Read and return a single MAF from the given s3 url
        '''
        return self.sqlContext.read.format('com.databricks.spark.csv')\
                   .options(header='true')\
                   .options(comment="#")\
                   .options(delimiter='\t')\
                   .options(codec="org.apache.hadoop.io.compress.GzipCodec")\
                   .load(url)

    def get_existing(self):
        '''
        Loads a built combined maf
        '''
        df = self.sqlContext.read.format('com.databricks.spark.csv')\
                        .options(header='true', inferschema='true')\
                        .load(self.config.maf_path)\
                        .drop_duplicates()
        return df

    def write(self, df):
        '''
        Writes the combined maf file
        '''
        writer = df.write.format('com.databricks.spark.csv')
        if self.config.maf_overwrite:
            writer = writer.mode('overwrite')
        writer = writer.options(header='true').save(self.config.maf_path)
