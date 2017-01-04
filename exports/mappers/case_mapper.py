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

        # change gene_id to keyword
        del gene_map['properties']['gene_id']['fields']
        gene_map['properties']['gene_id']['type'] = 'keyword'

        # change symbol to keyword
        del gene_map['properties']['symbol']['fields']
        gene_map['properties']['symbol']['type'] = 'keyword'

        mapping['properties']['gene'] = gene_map
        # Add ssm
        ssm_map = self.load_properties('ssm.yml', nested=True)
        gene_map['properties']['ssm'] = ssm_map
        # Consequence only holds transcript
        # Add transcript
        tran_map = self.load_properties('transcript.yml')
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
