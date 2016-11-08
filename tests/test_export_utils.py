import unittest
import pytest
from conftest import ES_INDEX

from exports.utils import get_array_paths


@pytest.mark.usefixtures('test_index')
class TestExportUtils(unittest.TestCase):

    def test_array_paths_simple(self):
        
        doc = { 'elem_1': [ { 'name': 'lorem'}, {'names':['ipsum','dolor']} ],
                'elem_2': { 'collection': [1, 2, {'four':'five'} ] }}

        paths = { 'elem_1', 'elem_1.names', 'elem_2.collection' }
        self.assertSetEqual(set(get_array_paths(doc)), paths)


    def test_array_paths_case(self):
        '''
        Test that path's of the array attributes of a json doc are identified
        '''
        # These are all the array paths that should exist in case docs
        paths = {'summary.data_categories', 'summary.experimental_strategies',
                  'sample_ids', 'submitter_sample_ids', 'samples',
                  'samples.portions', 'samples.portions.slides',
                  'samples.portions.analytes',
                  'samples.portions.analytes.aliquots', 'portion_ids',
                  'submitter_portion_ids', 'slide_ids', 'submitter_slide_ids',
                  'exposures', 'diagnoses', 'diagnoses.treatments',
                  'aliquot_ids', 'submitter_aliquot_ids', 'analyte_ids',
                  'submitter_analyte_ids', 'files', 'files.acl',
                  'files.index_files', 'files.cases', 'files.cases.exposures',
                  'files.cases.diagnoses', 'files.cases.diagnoses.treatments',
                  'files.cases.samples', 'files.cases.samples.portions',
                  'files.cases.samples.portions.analytes',
                  'files.cases.samples.portions.analytes.aliquots',
                  'files.cases.samples.portions.slides',
                  'files.downstream_analyses',
                  'files.downstream_analyses.output_files',
                  'files.analysis.input_files',
                  'files.analysis.metadata.read_groups',
                  'files.analysis.metadata.read_groups.read_group_qcs',
                 }
        case_doc = self.es.get(index=self.graph_index, doc_type='case',
                                id='a53e9117-0a49-40d8-af40-7867370b671a')

        case_doc = case_doc['_source']
        self.assertSetEqual(set(get_array_paths(case_doc)), paths)
