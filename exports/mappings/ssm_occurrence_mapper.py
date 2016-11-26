from base_mapper import Mapper
from esbuild.graph.active.mappings import ActiveESMapper


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
        ssm_map = self.load_properties('ssm.yml', nested=True)
        mapping['properties']['ssm'] = ssm_map
        # Consequence only holds transcript
        # Add transcript
        tran_map = self.load_properties('transcript.yml', nested=True)
        ssm_map['properties']['consequence'] = {'properties':{'transcript': tran_map}}
        ssm_map['properties']['consequence']['type'] = 'nested'
        # Add gene 
        gene_map = self.load_properties('gene.yml')
        tran_map['properties']['gene'] = gene_map
        # Add annotation
        annot_map = self.load_properties('annotation.yml')
        tran_map['properties']['annotation'] = annot_map

        # Occurance only holds case 
        # Add case mapping from graph
        graph_mapper = ActiveESMapper()
        case_map = graph_mapper.get_case_es_mapping()
        mapping['properties']['case'] = case_map
        # Add observation
        obs_map = self.load_properties('observation.yml', nested=True)
        case_map['properties']['observation'] = obs_map

        mapping = self.clean(mapping)
        return mapping
