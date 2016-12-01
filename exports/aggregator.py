import requests
import json
import logging


class Aggregator(object):

    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.urls = self.get_urls()

    def combine(self):
        '''
        Combines data frames from a list of urls
        '''
        df = None
        for url in self.urls:
            try:
                new_df = read_maf(url)
                self.logger.info('Read {} rows from {}'.format(df.count(), url))
                if df is None:
                    df = new_df
                else:
                    df = df.unionAll(new_df)
            except BaseException as e:
                self.logger.error(e)
        
        self.logger.info('Combined {} files for a total of {} rows'
                         .format(len(urls), df.count()))

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
        return sqlContext.read.format('com.databricks.spark.csv')\
                   .options(header='true')\
                   .options(comment="#")\
                   .options(delimiter='\t')\
                   .options(codec="org.apache.hadoop.io.compress.GzipCodec")\
                   .load(url)
