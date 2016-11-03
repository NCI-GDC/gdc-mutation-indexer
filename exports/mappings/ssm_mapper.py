from base_mapper import Mapper
from esbuild.graph.active.mappings import ActiveESMapper


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
        mapping.update(self.load_properties('exports/mappings/ssm.yaml'))

        
        # Consequence only holds transcript
        # Add transcript
        tran_map = self.load_properties('exports/mappings/transcript.yaml')
        mapping['properties']['consequence'] = {'properties':{'transcript': tran_map}}
        mapping['properties']['consequence']['type'] = 'nested'
        # Add gene 
        gene_map = self.load_properties('exports/mappings/gene.yaml', nested=True)
        tran_map['properties']['gene'] = gene_map
        # Add annotation
        annot_map = self.load_properties('exports/mappings/annotation.yaml', nested=True)
        tran_map['properties']['annotation'] = annot_map

        # Occurance only holds case 
        # Add case mapping from graph
        graph_mapper = ActiveESMapper()
        case_map = graph_mapper.get_case_es_mapping()
        mapping['properties']['occurrence'] = {'properties':{'case': case_map}}
        mapping['properties']['occurrence']['type'] = 'nested'
        # Add observation
        obs_map = self.load_properties('exports/mappings/observation.yaml', nested=True)
        case_map['properties']['observation'] = obs_map

        return mapping
