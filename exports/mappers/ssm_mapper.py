from base_mapper import Mapper


class SSMMapper(Mapper):
    '''
    ssm{}
      |____ consequence[]
      |           |_____ transcript{}
      |                        |_____ gene{}
      |                        |_____ annotation{}
      |____ occurrence[]
                  |_____ case{}
                           |____ observation[]
    '''

    def build_mapping(self):
        mapping = Mapper.build_mapping(self)
        mapping.update({"_id": { "path": "ssm_id" }})
        mapping.update(self.load_properties('ssm.yml'))
        
        # Consequence only holds transcript
        # Add transcript
        tran_map = self.load_properties('transcript.yml')
        mapping['properties']['consequence'] = {'properties':
                                                    {'transcript': tran_map,
                                                     'consequence_id': {
                                                        'type':'keyword'
                                                    }}
                                               }
        mapping['properties']['consequence']['type'] = 'nested'
        # Add gene 
        gene_map = self.load_properties('gene.yml', nested=False)

        self.change_props_to_keyword([
            'gene_id',
            'symbol',
            'canonical_transcript_id',
            'cytoband',
            'synonyms',
            'external_db_ids.properties.entrez_gene',
            'external_db_ids.properties.hgnc',
            'external_db_ids.properties.omim_gene',
            'external_db_ids.properties.uniprotkb_swissprot',
            'name'
        ], gene_map)

        tran_map['properties']['gene'] = gene_map
        # Add annotation
        annot_map = self.load_properties('annotation.yml', nested=False)
        tran_map['properties']['annotation'] = annot_map

        # Occurance only holds case 
        case_map = self.load_properties('case.yml', nested=False)

        self.change_props_to_keyword([
            'case_id',
            'submitter_id',
            'project.properties.disease_type',
            'project.properties.name',
            'project.properties.primary_site',
            'project.properties.project_id',
        ], case_map)

        mapping['properties']['occurrence'] = {
                                                'properties': {
                                                    'case': case_map,
                                                    'ssm_occurrence_id': {
                                                        'type': 'keyword' 
                                                    },
                                                    'occurrence_id': {
                                                        'type': 'keyword' 
                                                    }
                                                }
                                              }
        mapping['properties']['occurrence']['type'] = 'nested'
        # Add observation
        obs_map = self.load_properties('observation.yml', nested=True)
        case_map['properties']['observation'] = obs_map

        self.add_copy_to([
            'consequence.properties.transcript.properties.aa_change',
            'consequence.properties.transcript.properties.gene.properties.symbol',
            'genomic_dna_change',
            'ssm_id'
        ], mapping, 'ssm_autocomplete')

        mapping = self.clean(mapping)
        return mapping
