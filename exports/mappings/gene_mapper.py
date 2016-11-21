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
        mapping.update(self.load_properties('gene.yaml'))

        # Add case mapping from graph
        graph_mapper = ActiveESMapper()
        case_map = graph_mapper.get_case_es_mapping()
        mapping['properties']['case'] = case_map
        mapping['properties']['case']['type'] = 'nested'
        # Add ssm mapping
        ssm_map  = self.load_properties('ssm.yaml', nested=True)
        case_map['properties']['ssm'] = ssm_map
        # Consequence only holds transcript
        # Add transcript
        tran_map = self.load_properties('transcript.yaml', nested=True)
        ssm_map['properties']['consequence'] = {'properties':{'transcript': tran_map}}
        ssm_map['properties']['consequence']['type'] = 'nested'
        # Add annotation
        annot_map = self.load_properties('annotation.yaml', nested=True)
        tran_map['properties']['annotation'] = annot_map
        # Add observation
        obs_map = self.load_properties('observation.yaml', nested=True)
        ssm_map['properties']['observation'] = obs_map
       
        mapping = self.clean(mapping)

        return mapping
