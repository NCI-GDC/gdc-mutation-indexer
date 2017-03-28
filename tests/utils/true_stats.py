from pyspark.sql.functions import col, size, explode
import json
import os


class TrueStats:

    @staticmethod
    def load_data(output_dir, index_name):
        """
        Loads Junjun Data corresponding to :index_name
        """
        data_dir = os.path.join(output_dir, index_name)
        data = []
        for filename in os.listdir(data_dir):
            with open(os.path.join(data_dir, filename), 'r') as f:
                data.append(json.loads(f.read()))
        return data

    @classmethod
    def get_stats(cls, output_dir, index_name):
        """
        Returns stats for :index_name
        """
        data = cls.load_data(output_dir, index_name)
        function_name = '{}_stats'.format(index_name)
        return getattr(cls, function_name)(data)

    @classmethod
    def case_centric_stats(cls, data_list):
        """
        case{}
             |___ gene[]
                     |___ ssm[]
                           |___ consequence[]
                           |             |_____ transcript{}
                           |                          |_____ annotation{}
                           |___ observation[]
        """
        #       test_case_gene
        # Genes per Case
        gpc = {v['case_id']: len(v['gene']) for v in data_list}

        #       test_gene_ssm
        # SSMs per Gene (NOTE: depends on Case)
        spg = {}
        for D in data_list:
            case_id = D['case_id']
            spg[case_id] = {}
            for gene in D['gene']:
                gene_id = gene['gene_id']
                if gene_id not in spg[case_id]:
                    spg[case_id][gene_id] = len(gene['ssm'])

        #       test_ssm_subtree_case
        # Consequences per SSM and Observations per SSM
        # (depends on Case and Gene)
        cps = {}
        ops = {}
        for data in data_list:
            case_id = data['case_id']
            cps.setdefault(case_id, {})
            ops.setdefault(case_id, {})
            for gene in data['gene']:
                gene_id = gene['gene_id']
                cps[case_id].setdefault(gene_id, {})
                ops[case_id].setdefault(gene_id, {})
                for ssm in gene['ssm']:
                    ssm_id = ssm['ssm_id']
                    cps[case_id][gene_id][ssm_id] = len(ssm['consequence'])
                    ops[case_id][gene_id][ssm_id] = len(ssm['observation'])

        return {'count': len(data_list),
                'genes_per_case': gpc, 'ssms_per_gene': spg,
                'cons_per_ssm': cps, 'obs_per_ssm': ops}

    @classmethod
    def gene_centric_stats(cls, data_list):
        """
        gene{}
             |___ case[]
                     |___ ssm[]
                           |___ consequence[]
                           |             |_____ transcript{}
                           |                          |_____ annotation{}
                           |___ observation[]
        """
        #       test_gene_case
        # Cases per Gene
        cpg = {v['gene_id']: len(v['case']) for v in data_list}

        #       test_case_ssm
        # SSMs per case (NOTE: depends on Gene)
        spc = {}
        for D in data_list:
            gene_id = D['gene_id']
            spc[gene_id] = {}
            for case in D['case']:
                case_id = case['case_id']
                if case_id not in spc[gene_id]:
                    spc[gene_id][case_id] = len(case['ssm'])

        #       test_ssm_subtree_gene
        # Consequences per SSM and Observations per SSM
        # (depends on Gene and Case)
        cps = {}
        ops = {}
        for data in data_list:
            gene_id = data['gene_id']
            cps.setdefault(gene_id, {})
            ops.setdefault(gene_id, {})
            for case in data['case']:
                case_id = case['case_id']
                cps[gene_id].setdefault(case_id, {})
                ops[gene_id].setdefault(case_id, {})
                for ssm in case['ssm']:
                    ssm_id = ssm['ssm_id']
                    cps[gene_id][case_id][ssm_id] = len(ssm['consequence'])
                    ops[gene_id][case_id][ssm_id] = len(ssm['observation'])

        return {'count': len(data_list),
                'cases_per_gene': cpg, 'ssms_per_case': spc,
                'cons_per_ssm': cps, 'obs_per_ssm': ops}

    @staticmethod
    def ssm_centric_stats(data_list):
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
        #       test_ssm_occurrence
        # Consequences per SSM
        cps = {d['ssm_id']: len(d['consequence']) for d in data_list}

        # Occurrences per SSM
        ops = {d['ssm_id']: len(d['occurrence']) for d in data_list}

        #       test_occurrence_subtree
        # NOTE occurrence has no occurrence_id so this test cannot be implemeted
        # Observations per SSM
        # opo = {}
        # for d in [{k['occurrence_id']: len(k['ssm']) for k in D['case']}
        #           for D in data_list]:
        #     opo.update(d)
        return {'count': len(data_list),
                'cons_per_ssm': cps, 'occur_per_ssm': ops}

    @staticmethod
    def ssm_occurrence_centric_stats(data_list):
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

        # Observations per Case
        opc = {d['case']['case_id']: len(d['case']['observation'])
               for d in data_list}

        # Consequences per SSM
        cps = {d['ssm']['ssm_id']: len(d['ssm']['consequence'])
               for d in data_list}

        return {'count': len(data_list),
                'obs_per_case': opc, 'cons_per_ssm': cps}

    @staticmethod
    def print_diff(es_dict, true_dict):
        """
         Prints difference in stats nicely
         Used to debug join unit tests
        """

        es_keys = sorted(es_dict.keys())
        true_keys = sorted(true_dict.keys())

        if not es_keys == true_keys:
            print '1st level keys mismatch:\n{} != {}'.format(es_keys,
                                                              true_keys)
            return

        for key in es_dict.keys():
            es_keys = sorted(es_dict[key].keys())
            true_keys = sorted(true_dict[key].keys())
            if not es_keys == true_keys:
                print '2nd level keys mismatch:\n{} != {}'.format(es_keys,
                                                                  true_keys)
                return

            for kkey in es_dict[key].keys():
                es_keys = sorted(es_dict[key][kkey].keys())
                true_keys = sorted(true_dict[key][kkey].keys())
                if not es_keys == true_keys:
                    print '\n', key
                    print '\t', kkey
                    print '\t\t', [x for x in es_keys if
                                   x not in true_keys], 'in es, not in true'
                    print '\t\t', [x for x in true_keys if
                                   x not in es_keys], 'in true, not in es'


def get_ssm_subtree_stats(ssm_df, index_name):
    """
    Used to collect number of observations and consequences per ssm,
    given ssm_subtree from either case_centric or gene_centric index

    :param ssm_df: spark dataframe for ssm_subtree
    :param index_name:  in ['case_centric', 'gene_centric']
    :return: observations_per_ssm, consequences_per_ssm
    """
    assert index_name in ['case_centric', 'gene_centric']

    ssm_stats = map(json.loads,
                    (ssm_df.select('case_id', 'gene_id',
                                   explode('ssm').alias('ssm'))
                        .select('case_id', 'gene_id', 'ssm.ssm_id',
                                size('ssm.consequence'),
                                size('ssm.observation'))
                        .toJSON(use_unicode=False).collect()))
    es_ops = {}
    es_cps = {}
    for stat in ssm_stats:
        id_one, id_two = stat['case_id'], stat['gene_id']
        if index_name == 'gene_centric':
            id_one, id_two = id_two, id_one

        es_ops.setdefault(id_one, {})
        es_ops[id_one].setdefault(id_two, {})

        es_cps.setdefault(id_one, {})
        es_cps[id_one].setdefault(id_two, {})

        ssm_id = stat['ssm_id']
        if ssm_id in es_cps[id_one][id_two] or\
           ssm_id in es_ops[id_one][id_two]:
            raise Exception("Duplicate SSM for (case_id, gene_id)")

        es_cps[id_one][id_two][ssm_id] = stat['size(ssm.consequence)']
        es_ops[id_one][id_two][ssm_id] = stat['size(ssm.observation)']
    return es_ops, es_cps


