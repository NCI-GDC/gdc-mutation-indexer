import requests
import json
import logging
logging.basicConfig()

from pyspark.sql.functions import lit


class Aggregator(object):

    def __init__(self, config, sqlContext):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sqlContext = sqlContext

    def combine(self, urls):
        '''
        Combines data frames from a list of urls
        '''
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
