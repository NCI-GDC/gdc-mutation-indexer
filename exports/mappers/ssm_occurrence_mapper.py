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
                                'type': 'keyword'
                            }
                        }})

        # Add ssm
        ssm_map = self.load_properties('ssm.yml', nested=False)

        self.change_props_to_keyword([
            'genomic_dna_change',
        ], ssm_map)

        mapping['properties']['ssm'] = ssm_map
        # Consequence only holds transcript
        # Add transcript
        tran_map = self.load_properties('transcript.yml', nested=False)

        self.change_props_to_keyword([
            'aa_change',
        ], tran_map)

        ssm_map['properties']['consequence'] = {'properties':
                                                    {'transcript': tran_map,
                                                     'consequence_id': {
                                                        'type':'keyword'
                                                    }}
                                               }
        ssm_map['properties']['consequence']['type'] = 'nested'
        # Add gene 
        gene_map = self.load_properties('gene.yml')

        self.change_props_to_keyword([
            'gene_id',
            'symbol',
            'canonical_transcript_id',
            'cytoband',
            'synonyms',
            'external_db_ids.properties.entrez_gene',
            'external_db_ids.properties.hgnc',
            'external_db_ids.properties.omim_gene',
            'external_db_ids.properties.uniprotkb_swissprot',
            'name'
        ], gene_map)

        del gene_map['properties']['biotype']
        del gene_map['properties']['description']
        del gene_map['properties']['name']
        del gene_map['properties']['symbol']
        del gene_map['properties']['transcripts']

        tran_map['properties']['gene'] = gene_map
        # Add annotation
        annot_map = self.load_properties('annotation.yml')
        tran_map['properties']['annotation'] = annot_map

        # Occurance only holds case 
        case_map = self.load_properties('case.yml', nested=False)

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
            'project.properties.project_id',
            'project.properties.primary_site',
            'project.properties.disease_type',
            'project.properties.name'
        ], case_map)

        mapping['properties']['case'] = case_map
        mapping['properties']['ssm_occurrence_id'] = {'type': 'keyword'}
        mapping['properties']['occurrence_id'] = {'type': 'keyword'}
        # Add observation
        obs_map = self.load_properties('observation.yml', nested=True)
        case_map['properties']['observation'] = obs_map

        mapping = self.clean(mapping)
        return mapping
