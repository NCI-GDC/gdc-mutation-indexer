from base_mapper import Mapper
from esbuild.graph.active.mappings import ActiveESMapper


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
        mapping.update(self.load_properties('exports/mappings/gene.yaml'))

        graph_mapper = ActiveESMapper()
        # Add case mapping from graph
        case_map = graph_mapper.get_case_es_mapping()
        mapping['properties']['case'] = case_map
        # Add ssm mapping
        ssm_map  = self.load_properties('exports/mappings/ssm.yaml')
        case_map['properties']['ssm'] = ssm_map
        # Consequence only holds transcript
        # Add transcript
        tran_map = self.load_properties('exports/mappings/transcript.yaml')
        ssm_map['properties']['consequence'] = {'properties':{'transcript': tran_map}}
        # Add annotation
        annot_map = self.load_properties('exports/mappings/annotation.yaml')
        tran_map['properties']['annotation'] = annot_map
        # Add observation
        obs_map = self.load_properties('exports/mappings/observation.yaml')
        ssm_map['properties']['observation'] = obs_map
        
        return mapping
