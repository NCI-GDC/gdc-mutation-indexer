from base_mapper import Mapper


class GeneMapper(Mapper):
    '''
    gene{}
         |___ case[]
                 |___ ssm[]
                       |___ consequence[]
                       |             |_____ transcript{}
                       |                          |_____ annotation{}
                       |___ observation[]
    '''

    def build_mapping(self):
        mapping = Mapper.build_mapping(self)
        mapping.update({"_id": { "path": "gene_id" }})
        # Gene is at the root here
        mapping.update(self.load_properties('gene.yml'))

        # Add case mapping from graph
        case_map = self.load_properties('case.yml', nested=True)
        mapping['properties']['case'] = case_map
        mapping['properties']['case']['type'] = 'nested'
        # Add ssm mapping
        ssm_map  = self.load_properties('ssm.yml', nested=True)
        case_map['properties']['ssm'] = ssm_map
        # Consequence only holds transcript
        # Add transcript
        tran_map = self.load_properties('transcript.yml', nested=False)
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
