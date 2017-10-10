import json
import gzip
import os


class TestDataStats:

    @classmethod
    def load_test_data(cls, data_dir):
        """
        Loads test data
        """

        # load cases
        cases_file = [f for f in os.listdir(data_dir) if f.find('cases') != -1][0]
        cases = cls.load_es_graph_dump(os.path.join(data_dir, cases_file))

        # load genes
        genes_file = [f for f in os.listdir(data_dir) if f.find('genes') != -1
                      and f.find('cytobands') == -1][0]
        genes = cls.load_es_graph_dump(os.path.join(data_dir, genes_file))

        return {'case': cases, 'gene': genes}

    @staticmethod
    def load_es_graph_dump(filename):
        if filename.endswith('.gz'):
            f = gzip.open(filename, 'rb')
        else:
            f = open(filename, 'rb')

        try:
            docs = json.load(f)
        except:
            f.seek(0)
            # If instead the file is a case doc per line
            docs = []
            for line in f.readlines():
                docs.append(json.loads(line))
        return docs

    @classmethod
    def get_stats(cls, maf_df, test_data, index_name):
        """
        Returns true stats for :index_name
        """
        function_name = '{}_stats'.format(index_name)
        return getattr(cls, function_name)(maf_df, test_data)

    @staticmethod
    def case_centric_stats(maf_df, data):
        """
        case{}
             |___ gene[]
                     |___ ssm[]
                           |___ consequence[]
                           |             |_____ transcript{}
                           |                          |_____ annotation{}
                           |___ observation[]
        """
        count = maf_df.select('case_id').distinct().count()
        return {'count': count}

    @staticmethod
    def gene_centric_stats(maf_df, data):
        """
        gene{}
             |___ case[]
                     |___ ssm[]
                           |___ consequence[]
                           |             |_____ transcript{}
                           |                          |_____ annotation{}
                           |___ observation[]
        """
        count = maf_df.select('gene_id').distinct().count()
        return {'count': count}

    @staticmethod
    def ssm_centric_stats(maf_df, data):
        """
        ssm{}
          |____ consequence[]
          |           |_____ transcript{}
          |                        |_____ gene{}
          |                        |_____ annotation{}
          |____ occurrence[]
                      |_____ case{}
                               |____ observation[]
        """
        ssm_count = maf_df.select('ssm_id').distinct().count()
        return {'count': ssm_count}

    @staticmethod
    def ssm_occurrence_centric_stats(maf_df, data):
        """
        ssm_occurrence{}
              |____ ssm{}
              |        |____ consequence[]
              |                     |_____ transcript{}
              |                                   |_____ gene{}
              |                                   |_____ annotation{}
              |____ case{}
                       |____ observation[]
        """
        count = maf_df.select('occurrence_id').distinct().count()
        return {'count': count}
