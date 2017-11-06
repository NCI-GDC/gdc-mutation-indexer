import json


class BaseStats(object):
    """
    Calculates summary status for a given index
    """
    def __init__(self, json_file):
        # Class attributes
        # Number of projects seen in the index
        self.Nprojects = 0
        # Number of cases seen in the index
        self.Ncases = 0
        # Number of genes seen in the index
        self.Ngenes = 0
        # Number of unique mutaitons seen in the index
        # identified by unique combinations of chromosome, start_pos,
        # mutation_subtype, ref_allele and tumor_allele
        self.NUniqMut = 0
        # Number of consequences
        self.Nconseq = 0

        self.projects = []
        self.cases = []

        # Case count by project
        self.cases_per_project = {}
        # Gene count by case
        self.genes_per_case = {}
        # Gene count
        self.gene_count = {}
        # Mutation count by case
        self.mutations_per_case = {}
        # Mutation count by gene
        self.mutations_per_gene = {}
        # Number of unique mutaitons seen in the index
        # identified by unique combinations of chromosome, start_pos,
        # mutation_subtype, ref_allele and tumor_allele
        self.uniqMutations = {}
        # Consequence count by unique case and mutation pair
        self.consequences = {}

        self.data = []

        if type(json_file) is list:
            self.data = json_file
        else:
            with open(json_file, 'r') as f:
                self.data = json.load(f)

    @staticmethod
    def get_mutation(ssm):
        chromosome = ssm['chromosome']
        startpos = ssm['start_position']
        mutType = ssm['mutation_subtype']
        refallele = ssm['reference_allele']
        tumorall2 = ssm['tumor_allele']

        mutation = '_'.join(str(c) for c in [chromosome,
                                             startpos,
                                             refallele,
                                             tumorall2,
                                             mutType])
        return mutation


class SSMCentricStats(BaseStats):

    def __init__(self, json_file):
        super(SSMCentricStats, self).__init__(json_file)

        for h in self.data:

            if '_source' in h:
                h = h['_source']

            mutation = self.get_mutation(h)
            cases_in_ssm = []

            for ocurrence in h['occurrence']:
                project = ocurrence['case']['project']['project_id']
                case = ocurrence['case']['submitter_id']

                if project not in self.projects:
                    self.projects.append(project)
                    self.cases_per_project[project] = 0

                if case not in self.cases:
                    self.cases.append(case)
                    self.genes_per_case[case] = 0
                    self.mutations_per_case[case] = 0
                    self.cases_per_project[project] += 1

                cases_in_ssm.append(case)
                self.mutations_per_case[case] += 1

            for conseq in h['consequence']:
                for case in cases_in_ssm:
                    # Get consequences mutations
                    mutation_case = case + '_' + mutation
                    if mutation_case not in self.consequences:
                        self.consequences[mutation_case] = 1
                    else:
                        self.consequences[mutation_case] += 1

                    if conseq['transcript']['is_canonical']:
                        gene = conseq['transcript']['gene']['gene_id']
                        gene_case = project + '_' + case + '_' + gene

                        if gene_case not in self.mutations_per_gene:
                            self.mutations_per_gene[gene_case] = 0
                            self.genes_per_case[case]         += 1

                        self.gene_count.setdefault(gene, 0)
                        self.gene_count[gene] += 1
                        self.mutations_per_gene[gene_case] += 1

                        if mutation not in self.uniqMutations:
                            self.uniqMutations[mutation] = 1
                        else:
                            self.uniqMutations[mutation] += 1

        self.Nprojects = len(self.projects)
        self.Ncases = len(self.cases)
        self.Ngenes = len(self.gene_count)
        self.NUniqMut = len(self.uniqMutations)
        self.Nconseq = len(self.consequences)


class SSMOccurrenceCentricStats(BaseStats):

    def __init__(self, json_file):
        super(SSMOcurrenceCentricStats, self).__init__(json_file)

        for h in self.data:
            if '_source' in h:
                h = h['_source']

            project = h['case']['project']['project_id']
            case = h['case']['submitter_id']

            if project not in self.projects:
                self.projects.append(project)
                self.cases_per_project[project] = 0

            if case not in self.cases:
                self.cases.append(case)
                self.genes_per_case[case] = 0
                self.mutations_per_case[case] = 0
                self.cases_per_project[project] += 1

            self.mutations_per_case[case] += 1

            ssm = h['ssm']
            mutation = self.get_mutation(ssm)

            for conseq in ssm['consequence']:
                # Get consequences mutations
                mutation_case = case + '_' + mutation
                if mutation_case not in self.consequences:
                    self.consequences[mutation_case] = 1
                else:
                    self.consequences[mutation_case] += 1

                gene = conseq['transcript']['gene']['gene_id']
                gene_case = project + '_' + case + '_' + gene

                self.gene_count.setdefault(gene, 0)
                self.gene_count[gene] += 1

            if gene_case not in self.mutations_per_gene:
                self.mutations_per_gene[gene_case] = 0
                self.genes_per_case[case] += 1
            self.mutations_per_gene[gene_case] += 1

            mutation = self.get_mutation(ssm)

            if mutation not in self.uniqMutations:
                self.uniqMutations[mutation] = 1
            else:
                self.uniqMutations[mutation] += 1

        self.Nprojects = len(self.projects)
        self.Ncases = len(self.cases)
        self.Ngenes = len(self.gene_count)
        self.NUniqMut = len(self.uniqMutations)
        self.Nconseq = len(self.consequences)


class CaseCentricStats(BaseStats):

    def __init__(self, json_file):
        super(CaseCentricStats, self).__init__(json_file)
        self.NEmptyCases = 0

        for h in self.data:

            if 'hits' in h:
                h = h['hits']['hits'][0]

            if '_source' in h:
                h = h['_source']

            project = h['project']['project_id']
            case = h['submitter_id']

            if project not in self.projects:
                self.projects.append(project)
                self.cases_per_project[project] = 0

            if case not in self.cases:
                self.cases.append(case)
                self.cases_per_project[project] += 1
                self.genes_per_case[case] = 0
                self.mutations_per_case[case] = 0

            if h['gene'] is None:
                # There are "empty cases" in the data - cases with no mutations
                # thus they don't have a gene also
                self.NEmptyCases += 1
                continue

            for g in h['gene']:
                gene = g['gene_id']
                gene_case = project + '_' + case + '_' + gene

                self.gene_count.setdefault(gene, 0)
                self.gene_count[gene] += 1

                if gene_case not in self.mutations_per_gene:
                    self.mutations_per_gene[gene_case] = 0
                    self.genes_per_case[case] += 1

                for ssm in g['ssm']:
                    self.mutations_per_case[case] += 1
                    self.mutations_per_gene[gene_case] += 1

                    mutation = self.get_mutation(ssm)

                    if mutation not in self.uniqMutations:
                        self.uniqMutations[mutation] = 1
                    else:
                        self.uniqMutations[mutation] += 1

                    mutation_case = case + '_' + mutation
                    for conseq in ssm['consequence']:
                        if mutation_case not in self.consequences:
                            self.consequences[mutation_case] = 1
                        else:
                            self.consequences[mutation_case] += 1

        self.Nprojects = len(self.projects)
        self.Ncases = len(self.cases)
        self.Ngenes = len(self.gene_count)
        self.NUniqMut = len(self.uniqMutations)
        self.Nconseq = len(self.consequences)


class GeneCentricStats(BaseStats):

    def __init__(self, json_file):
        super(GeneCentricStats, self).__init__(json_file)

        for h in self.data:

            if 'hits' in h:
                h = h['hits']['hits'][0]

            if '_source' in h:
                h = h['_source']

            gene = h['gene_id']

            self.gene_count.setdefault(gene, 0)
            self.gene_count[gene] += 1

            for c in h['case']:
                project = c['project']['project_id']
                case = c['submitter_id']

                if project not in self.projects:
                    self.projects.append(project)
                    self.cases_per_project[project] = 0

                if case not in self.cases:
                    self.cases.append(case)
                    self.cases_per_project[project] += 1
                    self.genes_per_case[case] = 0
                    self.mutations_per_case[case] = 0

                gene_case = project + '_' + case + '_' + gene
                if gene_case not in self.mutations_per_gene:
                    self.mutations_per_gene[gene_case] = 0
                    self.genes_per_case[case] += 1

                for ssm in c['ssm']:
                    self.mutations_per_case[case] += 1
                    self.mutations_per_gene[gene_case] += 1

                    mutation = self.get_mutation(ssm)

                    if mutation not in self.uniqMutations:
                        self.uniqMutations[mutation] = 1
                    else:
                        self.uniqMutations[mutation] += 1

                    mutation_case = case + '_' + mutation
                    for conseq in ssm['consequence']:
                        if mutation_case not in self.consequences:
                            self.consequences[mutation_case] = 1
                        else:
                            self.consequences[mutation_case] += 1

        self.Nprojects = len(self.projects)
        self.Ncases = len(self.cases)
        self.Ngenes = len(self.gene_count)
        self.NUniqMut = len(self.uniqMutations)
        self.Nconseq = len(self.consequences)


def get_matches(maf_metrics, index_metrics, total):

    matches = 0
    for m in maf_metrics:
        if isinstance(index_metrics, list):
            if m in index_metrics:
                matches += 1
        else:
            if m in index_metrics and maf_metrics[m] == index_metrics[m]:
                matches += 1
    matches = float(matches) * 100 / total

    return matches


def test(maf_data, output_data):

    percentage_test = dict()

    percentage_test['Projects'] = get_matches(maf_data.projects, output_data.projects, maf_data.Nprojects)
    percentage_test['Cases'] = get_matches(maf_data.cases, output_data.cases, maf_data.Ncases)
    percentage_test['Cases per project'] = get_matches(maf_data.cases_per_project, output_data.cases_per_project, maf_data.Nprojects)
    percentage_test['Mutations per case'] = get_matches(maf_data.mutations_per_case, output_data.mutations_per_case, maf_data.Ncases)
    percentage_test['Genes per case'] = get_matches(maf_data.genes_per_case, output_data.genes_per_case, maf_data.Ncases)
    percentage_test['Mutations per gene'] = get_matches(maf_data.mutations_per_gene, output_data.mutations_per_gene, maf_data.Ngenes)
    percentage_test['Unique mutations'] = get_matches(maf_data.uniqMutations, output_data.uniqMutations, maf_data.NUniqMut)
    percentage_test['Consequences per ssm'] = get_matches(maf_data.consequences, output_data.consequences, maf_data.Nconseq)

    return percentage_test
