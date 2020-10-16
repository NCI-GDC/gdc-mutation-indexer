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
        cases_file = cls.filter_files(data_dir, ['cases'])[0]
        cases = cls.load_es_graph_dump(os.path.join(data_dir, cases_file))

        # load genes
        genes_file = cls.filter_files(data_dir, ['genes', 'json'])[0]
        genes = cls.load_es_graph_dump(os.path.join(data_dir, genes_file))

        return {'case': cases, 'gene': genes}

    @staticmethod
    def filter_files(directory, keywords):
        """
        Returns files from :directory that have all of the :keywords in name
        """
        return [
            f for f in os.listdir(directory)
            if all([k in f for k in keywords])
        ]

    @staticmethod
    def load_tsv_table(filename):
        rows = []
        with open(filename, 'r') as f:
            for n, line in enumerate(f.readlines()):
                row = line.replace('\n', '').split('\t')
                if n == 0:
                    header = row
                    continue
                rows.append(row)
        return {'header': header, 'data': rows}

    @staticmethod
    def load_es_graph_dump(filename):
        if filename.endswith('.gz'):
            f = gzip.open(filename, 'r')
        else:
            f = open(filename, 'r')

        try:
            docs = json.load(f)
        except:
            f.seek(0)
            # If instead the file is a case doc per line
            docs = []
            for line in f.readlines():
                try:
                    line = line.decode()
                except (UnicodeDecodeError, AttributeError):
                    pass
                docs.append(json.loads(line))

        f.close()
        return docs

    @classmethod
    def get_stats(cls, maf_df, gistic_df, test_data, doc_type):
        """
        Returns true stats for :doc_type
        """
        if doc_type in ["gene_expression"]:
            return None

        # Get index-specific stats:
        function_name = '{}_stats'.format(doc_type)
        stats = getattr(cls, function_name)(maf_df, gistic_df, test_data)

        # Add maf and gistic info:
        maf_data = maf_df.select('case_id', 'gene_id').collect()
        gistic_data = gistic_df.select('case_id', 'gene_id').collect()

        stats['ssm_cases'] = {r.case_id for r in maf_data}
        stats['cnv_cases'] = {r.case_id for r in gistic_data}

        stats['ssm_genes'] = {r.gene_id for r in maf_data}
        stats['cnv_genes'] = {r.gene_id for r in gistic_data}

        return stats

    @staticmethod
    def case_centric_stats(maf_df, gistic_df, data):
        """
        case{}
             |___ gene[]
                     |___ ssm[]
                     |     |___ consequence[]
                     |     |             |_____ transcript{}
                     |     |                          |_____ annotation{}
                     |     |___ observation[]
                     |
                     |___ cnv[]
                           |___ consequence[]
                           |            |_____ gene{}
                           |
                           |___ observation[]
        """
        # Number of cases in maf_df and gistic_df
        count = (
            maf_df.select('case_id').union(gistic_df.select('case_id'))
        ).distinct().count()
        return {'count': count}

    @staticmethod
    def gene_centric_stats(maf_df, gistic_df, data):
        """
        gene{}
             |___ case[]
                     |___ ssm[]
                     |     |___ consequence[]
                     |     |             |_____ transcript{}
                     |     |                          |_____ annotation{}
                     |     |___ observation[]
                     |
                     |___ cnv[]
                           |___ consequence[]
                           |            |_____ gene{}
                           |
                           |___ observation[]
        """
        count = (
            maf_df.select('gene_id').union(
                gistic_df.select('gene_id')
            ).distinct().count()
        )
        return {'count': count}

    @staticmethod
    def ssm_centric_stats(maf_df, gistic_df, data):
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
    def ssm_occurrence_centric_stats(maf_df, gistic_df, data):
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

    @staticmethod
    def cnv_centric_stats(maf_df, gistic_df, data):
        """
        cnv{}
            |____ consequence[]
            |             |_____ gene{}
            |____ occurrence[]
                        |_____ case{}
                                    |____ observation[]
        """
        count = gistic_df.select('cnv_id').distinct().count()
        return {'count': count}

    @staticmethod
    def cnv_occurrence_centric_stats(maf_df, gistic_df, data):
        """
        cnv{}
            |____ consequence[]
            |             |_____ gene{}
            |____ occurrence[]
                        |_____ case{}
                                    |____ observation[]
        """
        count = gistic_df.select('cnv_id', 'case_id').distinct().count()
        return {'count': count}

