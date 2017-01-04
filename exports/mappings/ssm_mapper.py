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
        mapping['properties']['consequence'] = {'properties':{'transcript': tran_map}}
        mapping['properties']['consequence']['type'] = 'nested'
        # Add gene 
        gene_map = self.load_properties('gene.yml', nested=False)

        # change gene_id to keyword
        del gene_map['properties']['gene_id']['fields']
        gene_map['properties']['gene_id']['type'] = 'keyword'

        # change symbol to keyword
        del gene_map['properties']['symbol']['fields']
        gene_map['properties']['symbol']['type'] = 'keyword'

        tran_map['properties']['gene'] = gene_map
        # Add annotation
        annot_map = self.load_properties('annotation.yml', nested=False)
        tran_map['properties']['annotation'] = annot_map

        # Occurance only holds case 
        case_map = self.load_properties('case.yml', nested=False)
        mapping['properties']['occurrence'] = {'properties':{'case': case_map}}
        mapping['properties']['occurrence']['type'] = 'nested'
        # Add observation
        obs_map = self.load_properties('observation.yml', nested=True)
        case_map['properties']['observation'] = obs_map

        mapping = self.clean(mapping)
        return mapping
