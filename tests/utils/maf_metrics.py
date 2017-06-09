class MAFStats(object):
    """
    Test count attributes for a MAF file
    """

    def __init__(self, maf_files):

        count_attributes = ['Nprojects', 'Ncases', 'Ngenes', 'Nmutations',
                            'NUniqMut', 'Nconseq']
        list_attributes = ['projects', 'cases', 'genes']
        dict_attributes = ['cases_per_project', 'genes_per_case', 'cases_per_gene',
                           'mutations_per_case', 'mutations_per_gene',
                           'uniqMutations', 'consequences', 'observations']

        for attr in count_attributes:
            setattr(self, attr, 0)

        for attr in list_attributes:
            setattr(self, attr, [])

        for attr in dict_attributes:
            setattr(self, attr, {})

        # Read project MAFs
        for maf in maf_files:
            self.process_maf_file(maf)

        self.Nprojects = len(self.projects)
        self.Ncases = len(self.cases)
        self.Ngenes = len(self.mutations_per_gene)
        self.NUniqMut = len(self.uniqMutations)
        self.Nconseq = len(self.consequences)

    def process_maf_file(self, filename):
        """
        Extracts counts from a MAF file and stores them as :self attributes
        :param filename:
        :return:
        """
        filename = filename.replace('file://', '')
        project = '-'.join(filename.split('/')[-1].split('.')[0:2])

        if project not in self.projects:
            self.projects.append(project)
            self.cases_per_project[project] = 0

        with open(filename, 'r') as f:
            for line in f:
                line = line.strip('\n')

                # Skip comment lines
                if line[0] == '#':
                    continue

                columns = line.split('\t')

                # Get columns positions from headers
                if columns[0] == 'Hugo_Symbol':
                    self.columns = {c: columns.index(c) for c in columns}
                    continue

                # Read relevant columns
                chromosome = columns[self.columns['Chromosome']].replace('M', 'MT')
                startpos = columns[self.columns['Start_Position']]
                refallele = columns[self.columns['Reference_Allele']]
                tumorall2 = columns[self.columns['Tumor_Seq_Allele2']]
                gene = columns[self.columns['Gene']]
                tumorUUID = columns[self.columns['Tumor_Sample_Barcode']]
                normalUUID = columns[self.columns['Matched_Norm_Sample_Barcode']]
                mutType = columns[self.columns['Variant_Type']]
                consequence = columns[self.columns['Consequence']]
                effects = columns[self.columns['all_effects']]

                # Get counts based on cases
                case = '-'.join(tumorUUID.split('-')[0:3])
                if case not in self.cases:
                    self.cases.append(case)
                    self.cases_per_project[project] += 1
                    self.genes_per_case[case] = 0
                    self.mutations_per_case[case] = 0

                # Get counts based on gene (case/gene)
                if gene not in self.genes:
                    self.genes.append(gene)

                gene_case = project + '_' + case + '_' + gene
                if gene_case not in self.mutations_per_gene:
                    self.genes_per_case[case] += 1
                    self.mutations_per_gene[gene_case] = 0

                    if gene not in self.cases_per_gene:
                        self.cases_per_gene[gene] = 0

                    self.cases_per_gene[gene] += 1

                # Define key for mutation
                mutation = '_'.join([chromosome,
                                     startpos,
                                     refallele,
                                     tumorall2,
                                     mutType])

                if mutation not in self.uniqMutations:
                    self.uniqMutations[mutation] = 0

                # Get consequences and observations
                mutation_case = case + '_' + mutation
                if mutation_case not in self.observations:
                    self.mutations_per_case[case] += 1
                    self.mutations_per_gene[gene_case] += 1
                    self.uniqMutations[mutation] += 1
                    self.observations[mutation_case] = 1

                    all_effects = effects.split(';')
                    if all_effects[-1] == '':
                        all_effects = all_effects[0:-1]
                    for effect in all_effects:
                        if mutation_case not in self.consequences:
                            self.consequences[mutation_case] = 1
                        else:
                            self.consequences[mutation_case] += 1

                else:
                    self.observations[mutation_case] += 1

                # Count each variant
                self.Nmutations += 1

    @staticmethod
    def histogram(data, title, xlabel, ylabel, bins=50, tails=None, filename=None):
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            return None

        values = [v for v in data. values()]
        values = sorted(values)
        try:
            plt.hist(values, bins)
            plt.title(title)
            plt.xlabel(xlabel)
            plt.ylabel(ylabel)

            if tails is not None:
                percentage = len(values)*tails/100
                endtail = values[percentage]
                plt.xlim([0, endtail])

            plt.show()
            if filename is not None:
                plt.savefig(filename)
        except:
            pass

