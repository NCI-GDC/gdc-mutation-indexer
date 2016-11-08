from base_mapper import Mapper
from esbuild.graph.active.mappings import ActiveESMapper


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
        graph_mapper = ActiveESMapper()
        mapping.update(graph_mapper.get_case_es_mapping())
        
        # Add gene 
        gene_map = self.load_properties('gene.yaml', nested=True)
        mapping['properties']['gene'] = gene_map
        # Add ssm
        ssm_map = self.load_properties('ssm.yaml', nested=True)
        gene_map['properties']['ssm'] = ssm_map
        # Consequence only holds transcript
        # Add transcript
        tran_map = self.load_properties('transcript.yaml')
        ssm_map['properties']['consequence'] = {'properties':{'transcript': tran_map}}
        ssm_map['properties']['consequence']['type'] = 'nested'
        # Add annotation
        annot_map = self.load_properties('annotation.yaml', nested=True)
        tran_map['properties']['annotation'] = annot_map
        # Add observation
        obs_map = self.load_properties('observation.yaml', nested=True)
        ssm_map['properties']['observation'] = obs_map

        return mapping
