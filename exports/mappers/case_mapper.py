from base_mapper import Mapper


class CaseMapper(Mapper):
    '''
    case{}
       |___ gene[]
               |___ ssm[]
                     |___ consequence[]
                     |             |_____ transcript{}
                     |                          |_____ annotation{}
                     |___ observation[]
    '''

    def build_mapping(self):
        mapping = Mapper.build_mapping(self)
        mapping.update({"_id": { "path": "case_id" }})
        case_map = self.load_properties('case.yml', nested=False)
        mapping.update(case_map)

        # Add gene 
        gene_map = self.load_properties('gene.yml', nested=True)

        self.change_props_to_keyword([
            'gene_id',
            'symbol',
            'canonical_transcript_id',
            'cytoband',
            'synonyms',
            'description', # this should probably be text
            'external_db_ids.properties.entrez_gene',
            'external_db_ids.properties.hgnc',
            'external_db_ids.properties.omim_gene',
            'external_db_ids.properties.uniprotkb_swissprot',
            'name'
        ], gene_map)

        mapping['properties']['gene'] = gene_map
        # Add ssm
        ssm_map = self.load_properties('ssm.yml', nested=True)

        self.change_props_to_keyword([
            'genomic_dna_change'
        ], ssm_map)

        gene_map['properties']['ssm'] = ssm_map
        # Consequence only holds transcript
        # Add transcript
        tran_map = self.load_properties('transcript.yml')

        self.change_props_to_keyword([
            'aa_change',
        ], tran_map)

        ssm_map['properties']['consequence'] = {'properties':{'transcript': tran_map}}
        ssm_map['properties']['consequence']['type'] = 'nested'
        # Add annotation
        annot_map = self.load_properties('annotation.yml', nested=False)
        tran_map['properties']['annotation'] = annot_map
        # Add observation
        obs_map = self.load_properties('observation.yml', nested=True)
        ssm_map['properties']['observation'] = obs_map

        mapping = self.clean(mapping)
        return mapping
