import argparse
try:
    # Dont want to require matplotlib if we don't have to
    import matplotlib.pyplot as plt
except ImportError:
    pass

class maf():

    def __init__(self, maf_files):   

        # Class attributes
        self.Nprojects  = 0
        self.Ncases     = 0
        self.Ngenes     = 0
        self.Nmutations = 0
        self.NUniqMut   = 0
        self.Nconseq    = 0

        self.projects = []
        self.cases = []
        self.genes = []
        
        self.cases_per_project  = {}
        self.genes_per_case     = {}
        self.mutations_per_case = {}        
        self.mutations_per_gene = {}
        self.uniqMutations = {}  
        
        self.consequences = {}

        # Read project MAFs
        for fileName in maf_files:

          project = '-'.join(fileName.split('/')[-1].split('.')[0:2])
          
          if not project in self.projects:
              self.projects.append(project)
              self.cases_per_project[project] = 0

          with open(fileName,'rt') as f:
             for line in f:

                 line = line.strip('\n')

                 # Not consider comment lines
                 if line[0] == '#':
                    continue

                 columns = line.split('\t')

                 # Get columns positions from headers
                 if columns[0] == 'Hugo_Symbol':
                      self.headers  = columns
                      geneidx       = self.headers.index('Gene')
                      chridx        = self.headers.index('Chromosome')
                      startidx      = self.headers.index('Start_Position')
                      endidx        = self.headers.index('End_Position')
                      refidx        = self.headers.index('Reference_Allele')  
                      tumorAll1idx  = self.headers.index('Tumor_Seq_Allele1')
                      tumorAll2idx  = self.headers.index('Tumor_Seq_Allele2') 
                      normalAll1idx = self.headers.index('Match_Norm_Seq_Allele1')      
                      normalAll2idx = self.headers.index('Match_Norm_Seq_Allele2')
                      typeidx       = self.headers.index('Variant_Type')
                      tumoridx      = self.headers.index('Tumor_Sample_Barcode')
                      normalidx     = self.headers.index('Matched_Norm_Sample_Barcode')
                      
                      # Transcript columns
                      conseqidx     = self.headers.index('Consequence')   
                      aaidx         = self.headers.index('Amino_acids')      
                      prposidx      = self.headers.index('Protein_position')
                      effectidx     = self.headers.index('all_effects')
                      continue
                 
                 # Read relevant columns
                 chromosome  = columns[chridx] #.replace('chr','')
                 startpos    = columns[startidx]
                 refallele   = columns[refidx]
                 tumorall2   = columns[tumorAll2idx]
                 gene        = columns[geneidx]
                 tumorUUID   = columns[tumoridx]
                 normalUUID  = columns[normalidx]   
                 mutType     = columns[typeidx]                           
                 consequence = columns[conseqidx] 
                 effects     = columns[effectidx] 

                 # Get counts based on cases
                 case = '-'.join(tumorUUID.split('-')[0:3])
                 if not case in self.cases:
                    self.cases.append(case)
                    self.cases_per_project[project] += 1

                    self.genes_per_case[case]     = 0
                    self.mutations_per_case[case] = 0                  
                 self.mutations_per_case[case] += 1

                 # Get counts based on gene (case/gene)
                 if gene not in self.genes:
                    self.genes.append(gene)
                 gene_case = project + '_' + case + '_' + gene
                 if not gene_case in self.mutations_per_gene:
                    self.genes_per_case[case] += 1  
                    self.mutations_per_gene[gene_case] = 0                    
                                                  
                 self.mutations_per_gene[gene_case] += 1   

                 # Define key for mutation
                 mutation = '_'.join([chromosome, 
                                      startpos,  
                                      refallele, 
                                      tumorall2, 
                                      mutType])   
                 if not mutation in self.uniqMutations:
                    self.uniqMutations[mutation] = 1
                 else:
                    self.uniqMutations[mutation] += 1                   

                 # Get consequences mutations
                 mutation_case = case + '_' + mutation
                 all_effects = effects.split(';')[0:-1]                
                 for effect in all_effects:
                    if not mutation_case in self.consequences:
                      self.consequences[mutation_case] = 1   
                    else:
                      self.consequences[mutation_case] += 1                    

                 # Count each variant
                 self.Nmutations += 1

        self.Nprojects  = len(self.projects)
        self.Ncases     = len(self.cases)
        self.Ngenes     = len(self.genes)
        self.NUniqMut   = len(self.uniqMutations)       
        self.Nconseq    = len(self.consequences)

def histogram(data, title, xlabel, ylabel, bins=50, tails=None, filename=None):

    values = [v for v in data. values()]
    values = sorted(values)
    try:
        plt.hist(values, bins)
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        
        if tails != None:
           percentage = len(values)*tails/100
           endtail    = values[percentage]
           plt.xlim([0, endtail])

        plt.show()

        if filename != None:
          plt.savefig(filename)
    except:
        pass

