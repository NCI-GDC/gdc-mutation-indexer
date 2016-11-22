def get_array_paths(doc, path=[]):
    '''
    Given a json document, return the dot-path of all array type attributes.
    This is important as array values cannot be identified from the mapping
    of an index, although they must be supplied to the elasticsearch-hadoop
    adapter when saving to elasticsearch
    '''
    if type(doc) not in [list, dict]:
        return []

    paths = []
    for k, v in doc.items():
        if type(v) is list:
            paths.append('.'.join(path+[k]))
            for elem in v:
                paths.extend(get_array_paths(elem, path + [k]))
        elif type(v) is dict:
            paths.extend(get_array_paths(v, path + [k]))
            
    return paths

## Rename and select desired columns in the mafs

maf_gene_map = {
    'biotype': 'BIOTYPE',
    'canonical_transcript_id': 'Transcript_ID',
    'gene_chromosome': 'Chromosome',
    'gene_end': 'End_Position',
    'gene_id': 'Gene',
    'gene_start': 'Start_Position',
    'name': 'Hugo_Symbol',
    'symbol': 'SYMBOL',
}

maf_ssm_map = {
    'gene_symbol':'Hugo_Symbol',
    'ncbi_build':'NCBI_Build',
    'mutation_subtype':'Mutation_Status',
    'start_position':'Start_Position',
    'end_position':'End_Position',
    'variant_type':'Variant_Type',
    'tumor_allele':'Tumor_Seq_Allele1',
    'reference_allele':'Reference_Allele',
    'genomic_dna_change':'Allele',
    'mutation_type':'Mutation_Status',
    'chromosome':'Chromosome',
    #'Tumor_Sample_Barcode':'tumor_sample_barcode'
}

maf_transcript_map = {
    'is_canonical':'CANONICAL',
    'consequence_type':'Score',
    'gene_symbol':'Hugo_Symbol',
    'ref_seq_accession':'AA_MAF',
    'aa_start':'AA_MAF',
    'aa_end':'AA_MAF',
    'aa_change':'AA_MAF'
}

maf_annotation_map = {
    'impact':'IMPACT',
    'amino_acids':'Amino_acids',
    'existing_variation':'Existing_variation',
    'sift':'SIFT',
    'pubmed':'PUBMED',
    'ccds':'CCDS',
    'cdna_position':'cDNA_position',
    'hgvsp':'HGVSp',
    'ensp':'ENSP',
    'dbsnp_rs':'dbSNP_RS',
    'trembl':'TREMBL',
    'uniparc':'UNIPARC',
    'codons':'Codons',
    'polyphen':'PolyPhen',
    'hgvsc':'HGVSc',
    'swissprot':'SWISSPROT',
    'domains':'DOMAINS',
    'protein_position':'Protein_position',
    'cds_start':'Entrez_Gene_Id',
    'cds_position':'PUBMED',
    'cds_length':'Entrez_Gene_Id',
    'cds_end':'Entrez_Gene_Id',
    'hgvsp_short':'HGVSp_Short'
}

maf_observation_map = {
    'src_vcf_id':'src_vcf_id',
    'mutation_status':'Mutation_Status',
    'center':'Center',
    'gene_symbol': 'Hugo_Symbol'
}
# Nested objects
normal_genotype_map = {
    'normal_allele1':'Match_Norm_Seq_Allele1',
    'normal_allele2':'Match_Norm_Seq_Allele2',
}
tumor_genotype_map = {
    'tumor_seq_allele1':'Tumor_Seq_Allele1',
    'tumor_seq_allele2':'Tumor_Seq_Allele2',
}
tumor_validation_map = {
    'tumor_validation_allele1':'Tumor_Validation_Allele1',
    'tumor_validation_allele2':'Tumor_Validation_Allele2',
}
read_depth_map = {
    'n_depth':'n_depth',
    't_alt_count':'t_alt_count',
    't_depth':'t_depth',
    't_ref_count':'t_ref_count',
}
input_bam_map = {
    'normal_bam_uuid':'normal_bam_uuid',
    'tumor_bam_uuid':'tumor_bam_uuid',
}
sample_map = {
    'matched_norm_sample_barcode':'Matched_Norm_Sample_Barcode',
    'matched_norm_sample_uuid':'Matched_Norm_Sample_UUID',
    'tumor_sample_barcode':'Tumor_Sample_Barcode',
    'tumor_sample_uuid':'Tumor_Sample_UUID'
}

maf_cols = {}
maf_cols.update(maf_annotation_map)
maf_cols.update(maf_gene_map)
maf_cols.update(maf_observation_map)
maf_cols.update(maf_ssm_map)
maf_cols.update(maf_transcript_map)
maf_cols.update(normal_genotype_map)
maf_cols.update(tumor_genotype_map)
maf_cols.update(tumor_validation_map)
maf_cols.update(read_depth_map)
maf_cols.update(input_bam_map)
maf_cols.update(sample_map)
