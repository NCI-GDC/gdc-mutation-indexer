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

        self.change_props_to_keyword([
            'aliquot_ids',
            'analyte_ids',
            'case_id',
            'portion_ids',
            'sample_ids',
            'slide_ids',
            'submitter_aliquot_ids',
            'submitter_analyte_ids',
            'submitter_id',
            'submitter_portion_ids',
            'submitter_sample_ids',
            'submitter_slide_ids',
            'project.properties.disease_type',
            'project.properties.name',
            'project.properties.primary_site',
            'project.properties.project_id',
        ], case_map)

        mapping['properties']['case'] = case_map
        mapping['properties']['case']['type'] = 'nested'
        # Add ssm mapping
        ssm_map  = self.load_properties('ssm.yml', nested=True)

        case_map['properties']['ssm'] = ssm_map
        # Consequence only holds transcript
        # Add transcript
        tran_map = self.load_properties('transcript.yml', nested=False)

        self.change_props_to_keyword(['gene_symbol', 'aa_change'], tran_map)

        ssm_map['properties']['consequence'] = {'properties': {'transcript': tran_map}}
        ssm_map['properties']['consequence']['type'] = 'nested'
        # Add annotation
        annot_map = self.load_properties('annotation.yml', nested=False)
        tran_map['properties']['annotation'] = annot_map
        # Add observation
        obs_map = self.load_properties('observation.yml', nested=True)
        ssm_map['properties']['observation'] = obs_map
        mapping = self.clean(mapping)

        return mapping
