from base_mapper import Mapper


class SSMOccurrenceMapper(Mapper):
    '''
    ssm_occurrence{}
               |____ ssm{}
               |        |____ consequence[]
               |                     |_____ transcript{}
               |                                   |_____ gene{}
               |                                   |_____ annotation{}
               |____ case{}
                        |____ observation[]
    '''

    def build_mapping(self):
        mapping = Mapper.build_mapping(self)
        mapping.update({"_id": { "path": "ssm_occurance_id" }})
        mapping.update({'properties': {
                            'ssm_occurrence_id': {
                                'type': 'string',
                                'index': 'not_analyzed'
                            }
                        }})

        # Add ssm
        ssm_map = self.load_properties('ssm.yml', nested=False)
        mapping['properties']['ssm'] = ssm_map
        # Consequence only holds transcript
        # Add transcript
        tran_map = self.load_properties('transcript.yml', nested=False)
        ssm_map['properties']['consequence'] = {'properties':{'transcript': tran_map}}
        ssm_map['properties']['consequence']['type'] = 'nested'
        # Add gene 
        gene_map = self.load_properties('gene.yml')

        # change gene_id to keyword
        del gene_map['properties']['gene_id']['fields']
        gene_map['properties']['gene_id']['type'] = 'keyword'

        # change symbol to keyword
        del gene_map['properties']['symbol']['fields']
        gene_map['properties']['symbol']['type'] = 'keyword'

        tran_map['properties']['gene'] = gene_map
        # Add annotation
        annot_map = self.load_properties('annotation.yml')
        tran_map['properties']['annotation'] = annot_map

        # Occurance only holds case 
        case_map = self.load_properties('case.yml', nested=False)

        # change project_id to keyword
        del case_map['properties']['project']['properties']['project_id']['fields']
        case_map['properties']['project']['properties']['project_id']['type'] = 'keyword'

        # change primary_site to keyword
        del case_map['properties']['project']['properties']['primary_site']['fields']
        case_map['properties']['project']['properties']['primary_site']['type'] = 'keyword'

        # change disease_type to keyword
        del case_map['properties']['project']['properties']['disease_type']['fields']
        case_map['properties']['project']['properties']['disease_type']['type'] = 'keyword'

        mapping['properties']['case'] = case_map
        # Add observation
        obs_map = self.load_properties('observation.yml', nested=True)
        case_map['properties']['observation'] = obs_map

        mapping = self.clean(mapping)
        return mapping
