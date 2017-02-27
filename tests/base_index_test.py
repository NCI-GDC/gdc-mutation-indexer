import json
import os
from elasticsearch import Elasticsearch
from contextlib import contextmanager

from exports.builders import MAFBuilder


class BaseIndexTest:
    """
    Abstract wrapper around ['gene_centric', 'case_centric',
                             'ssm_centric', 'ssm_occurrence_centric'] tests
    """

    def __init__(self, builder, test_config):
        self.builder = builder
        self.conf = test_config
        self.index = builder.index_name
        self.id_field = '{}_id'.format(self.index.replace('_centric', ''))
        self.output_dir = os.path.join(self.conf.output_dir, self.index)
        self.debug = self.conf.print_data_errors

    @contextmanager
    def index_generator(self, sql_context):
        es = self.generate_index(sql_context)

        yield es

        if not self.conf.keep_indices:
            es.indices.delete(index=self.conf.indices[self.index], ignore=399)

    def generate_index(self, sql_context):
        """
        Generates index corresponding to self.builder
        Returns Elasticsearch instance
        """
        for logtype in ['summary', 'errors', 'treediff']:
            try:
                os.remove(os.path.join(self.conf.log_dir,
                                       '{}_{}.log'.format(self.index, logtype)))
            except:
                pass

        es = Elasticsearch(self.conf.es_host, port=self.conf.es_port)

        maf_df = MAFBuilder(self.conf, sql_context).build()

        self.builder(self.conf, sql_context).build(maf_df).load()

        return es

    def get_docs_to_compare(self, es_index, filename):
        """
        Returns true document loaded from :filename
        and a corresponding built document from elasticsearch
        """
        # Compare each true output document with document in ES:
        with open(os.path.join(self.output_dir, filename), 'r') as f:
            true_doc = json.loads(f.read())

        es_doc = es_index.get(index=self.conf.indices[self.index], id=filename)['_source']

        assert self.id_field in es_doc.keys()

        return es_doc, true_doc

    def get_docs_to_compare_new(self, es_index_generator, filename):
        """
        Returns true document loaded from :filename
        and a corresponding built document from elasticsearch
        """
        # Compare each true output document with document in ES:
        with open(os.path.join(self.output_dir, filename), 'r') as f:
            true_doc = json.loads(f.read())

        with es_index_generator as es_index:
            es_doc = es_index.get(index=self.conf.indices[self.index], id=filename)['_source']

        assert self.id_field in es_doc.keys()

        return es_doc, true_doc


    def report_deepdiff(self, diff):
        """
        Reposts DeepDiff result.
        Writes to {self.conf.log_dir}/{index}_treediff.log
        """
        def print_level(deepdiff_level):
            lines = []
            if isinstance(deepdiff_level, set):
                for l in deepdiff_level:
                    t1, t2 = map(shorten, [l.t1, l.t2])
                    lines.append(['{}:\n\tes -> {}\n\tjj -> {}'.format(l.path(), t1, t2),
                                  '-'*30])
            else:
                raise Exception("Unknown deepdiff level type")

            for ln in lines:
                self.say(ln[0])
                self.say(ln[0])

            return lines

        def shorten(obj):
            if isinstance(obj, dict):
                return obj.keys()
            elif any(map(lambda x: isinstance(obj, x), [list, set])):
                return [shorten(x) for x in obj]
            else:
                return obj

        # Write report to file
        with open(os.path.join(self.conf.log_dir, '{}_treediff.log'.format(self.index)), 'a') as f:
            f.write('\n' + '[FILE]' + '+'*60+ '\n')

            for k, v in diff.items():
                f.write('\n[{}]'.format(k.upper()) + '='*60)
                for line in print_level(v):
                    f.write('\n' + '\n'.join(line))
                f.write('\n[\{}]'.format(k.upper()) + '='*60 + '\n')

            f.write('\n' + '[\FILE]' + '+'*60 + '\n')


    def report_correctness(self, es_doc, true_doc, label):
        """
        Calculate correctness and write {$log_dir}/{$index}_{summary,errors}.log files
        WARNING: Uses flattened json documents
        """
        cnt = {'correct': 0, 'missing_fields': 0, 'wrong_values': 0,
               'total': len(true_doc.keys())}
        err = {'missing_fields': [], 'wrong_values_for': [], 'wrong_values': []}
        for k, v in true_doc.items():
            if k not in es_doc:
                self.say("[Missing field]: {}".format(k))
                cnt['missing_fields'] += 1
                err['missing_fields'].append(k)
            elif es_doc[k] != v:
                self.say("[Value mismatch]: {} |Not Equals| {} [{}]".format(v, es_doc[k], k))
                cnt['wrong_values'] += 1
                err['wrong_values'].append([v, es_doc[k]])
                err['wrong_values_for'].append(k)
            else:
                cnt['correct'] += 1

        self.say("\n[STATS]: {}".format(cnt))
        self.say("[CORRECTNESS]: {}%\n".format(float(cnt['correct'])/cnt['total']))

        with open(os.path.join(self.conf.log_dir,
                               '{}_summary.log'.format(self.index)), 'a') as f:
            f.write('{},{},{}\n'.format(label, float(cnt['correct'])/cnt['total'], cnt))

        with open(os.path.join(self.conf.log_dir,
                               '{}_errors.log'.format(self.index)), 'a') as f:
            f.write('Missing fields:\n')
            f.write('\n'.join(err['missing_fields']))
            f.write('\nValue mismatch:\n')
            for z in zip(err['wrong_values_for'], err['wrong_values']):
                f.write('\n' + '{} :> {}'.format(z[0], z[1]))

    def say(self, string):
        """
        If not self.debug does nothing
        Else: prints decorated string
        """
        if string[0] == '\n':
            print ''
            self.say(string[1:])
        else:
            if self.debug:
                print '~{}~{}'.format(self.index, string)

    @staticmethod
    def flatten_json(json_dict):
        """
        Flattens json preserving path information in key names
        WARNING: lists order is NOT ignored
        """
        result = {}

        def flatten(x, name=''):
            if isinstance(x, dict):
                for a in x:
                    flatten(x[a], name + a + '_')
            elif isinstance(x, list):
                for i, a in enumerate(x):
                    flatten(a, name + str(i) + '_')
            else:
                result[name[:-1]] = x

        flatten(json_dict)
        return result
