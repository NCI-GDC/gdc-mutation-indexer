import pytest
import json

from pyspark.sql.functions import size, explode, lit
from exports.builders import (
    CaseBuilder,
    ObservationBuilder,
    ConsequenceBuilder,
    GeneCentricBuilder,
    CaseCentricBuilder,
    SSMCentricBuilder,
    SSMOccurrenceCentricBuilder
)
from tests_config import TestConfig
from utils.true_stats import TrueStats, get_ssm_subtree_stats
from exports.builders.utils import extract_aas_position

conf = TestConfig()


@pytest.mark.usefixtures('sqlContext', 'maf_df')
class TestBuildersSimple:
    @pytest.mark.parametrize("builder", [CaseCentricBuilder,
                                         GeneCentricBuilder,
                                         SSMCentricBuilder,
                                         SSMOccurrenceCentricBuilder])
    def test_simple_build_indices(self, maf_df, sqlContext, builder):
        builder(conf, sqlContext).build(maf_df)

    @pytest.mark.parametrize("index_name", conf.indices)
    @pytest.mark.parametrize("builder", [ConsequenceBuilder,
                                         ObservationBuilder])
    def test_simple_build_other(self, maf_df, sqlContext, builder, index_name):
        builder(conf, sqlContext).build(maf_df, index_name)


@pytest.mark.usefixtures('sqlContext', 'maf_df', 'test_index')
class TestCaseCentricJoins:
    """ Test intermediate result from the case centric builder """

    @pytest.fixture(scope='class')
    def builder(self, sqlContext):
        yield CaseCentricBuilder(conf, sqlContext)

    @pytest.fixture(scope='class')
    def build_df(self, builder, maf_df):
        yield builder.build(maf_df).case_centric

    @pytest.fixture(scope='class')
    def true_stats(self):
        yield TrueStats.get_stats(conf.output_dir, 'case_centric')

    @pytest.mark.ssm_subtree_case
    @pytest.mark.skipif(conf.indices_are_pruned, reason='n/a if pruned')
    def test_ssm_subtree(self, maf_df, builder, true_stats):
        ssm_df = builder.build_ssm(maf_df)

        # Correct number of ssm's
        assert ssm_df.count() == maf_df.select('ssm_id').distinct().count()

        es_ops, es_cps = get_ssm_subtree_stats(ssm_df, 'case_centric')

        # Correct number of Observations per SSM
        assert es_ops == true_stats['obs_per_ssm']

        # Correct number of Consequences per SSM
        assert es_cps == true_stats['cons_per_ssm']

    @pytest.mark.skipif(conf.indices_are_pruned, reason='n/a if pruned')
    def test_gene_ssm(self, maf_df, builder, true_stats):
        # # one gene per ssm for our test mafs
        gene_df = builder.build_gene_ssm(maf_df)

        # Correct number of SSMs per Gene
        es_spg = map(json.loads,
                     gene_df.select('case_id', 'gene.gene_id', size('gene.ssm'))
                            .toJSON(use_unicode=False).collect())
        spg_dict = {}
        for d in es_spg:
            case_id = d['case_id']
            gene_id = d['gene_id']
            if case_id not in spg_dict:
                spg_dict[case_id] = {}
            spg_dict[case_id][gene_id] = d['size(gene.ssm)']

        assert spg_dict == true_stats['ssms_per_gene']

    @pytest.mark.skipif(conf.indices_are_pruned, reason='n/a if pruned')
    def test_case_gene(self, build_df, true_stats):
        case_df = build_df
        # Correct number of Case documents
        assert case_df.count() == true_stats['count']

        # Correct number of Genes per Case
        es_gpc = (case_df.select(size('gene'), 'case_id')
                  .toJSON(use_unicode=False).collect())
        es_gpc = {d['case_id']: d['size(gene)']
                  for d in map(json.loads, es_gpc)}
        assert es_gpc == true_stats['genes_per_case']

    def test_variation_data(self, test_index, build_df):
        """ Test that cases without any ssm, but were tested are flagged """
        case_df = build_df

        # Insert a case to graph with no data
        test_index.index(conf.graph_index, doc_type='case',
                         id='empty_case', body={'case_id': 'empty_case'})
        # Force ES to refresh before trying to build index
        test_index.indices.refresh(index=conf.graph_index)

        assert 'available_variation_data' in case_df.columns
        # Sum of booleans, True = 1, False = 0, should only have one test case

        assert sum(
            [r['available_variation_data'] == ['ssm']
             for r in case_df.select('available_variation_data').collect()]
        ) == case_df.count() - 1

        assert sum([r['available_variation_data'] == [] for r in
                   case_df.select('available_variation_data').collect()]) == 1

        assert (case_df.cache()
                       .filter(case_df.case_id == 'empty_case')
                       .select('available_variation_data')
                       .collect()[0]['available_variation_data'] == [])

        # Get rid of the test document and force an ES refresh
        test_index.delete(conf.graph_index, doc_type='case', id='empty_case')
        test_index.indices.refresh(index=conf.graph_index)


@pytest.mark.usefixtures('sqlContext', 'maf_df')
class TestGeneCentricJoins:
    """
    Test intermediate result from the case centric builder
    """

    @pytest.fixture(scope='class')
    def builder(self, sqlContext):
        yield GeneCentricBuilder(conf, sqlContext)

    @pytest.fixture(scope='class')
    def true_stats(self):
        yield TrueStats.get_stats(conf.output_dir, 'gene_centric')

    @pytest.mark.ssm_subtree_gene
    @pytest.mark.skipif(conf.indices_are_pruned, reason='n/a if pruned')
    def test_ssm_subtree(self, maf_df, builder, true_stats):
        ssm_df = builder.build_ssm(maf_df)

        # Correct number of ssm's
        assert ssm_df.count() == maf_df.select('ssm_id').distinct().count()

        es_ops, es_cps = get_ssm_subtree_stats(ssm_df, 'gene_centric')

        # Correct number of Observations per SSM
        assert es_ops == true_stats['obs_per_ssm']

        # Correct number of Consequences per SSM
        assert es_cps == true_stats['cons_per_ssm']

    @pytest.mark.skipif(conf.indices_are_pruned, reason='n/a if pruned')
    def test_case_ssm(self, maf_df, builder, true_stats):
        case_df = builder.build_case_ssm(maf_df)

        # Correct number of SSMs per Case
        es_spc = map(json.loads,
                     case_df.select('gene_id', 'case.case_id', size('case.ssm'))
                            .toJSON(use_unicode=False).collect())
        spc_dict = {}
        for d in es_spc:
            gene_id = d['gene_id']
            case_id = d['case_id']
            if gene_id not in spc_dict:
                spc_dict[gene_id] = {}
            spc_dict[gene_id][case_id] = d['size(case.ssm)']

        assert spc_dict == true_stats['ssms_per_case']

    @pytest.mark.skipif(conf.indices_are_pruned, reason='n/a if pruned')
    def test_gene_case(self, maf_df, builder, true_stats):
        gene_df = builder.build(maf_df).gene_centric

        es_cpg = (gene_df.select(size('case'), 'gene_id')
                  .toJSON(use_unicode=False).collect())
        es_cpg = {d['gene_id']: d['size(case)']
                  for d in map(json.loads, es_cpg)}

        # Correct number of Gene documents
        assert gene_df.count() == true_stats['count']

        # Correct number of Cases per Gene
        assert es_cpg == true_stats['cases_per_gene']


@pytest.mark.usefixtures('sqlContext', 'maf_df')
class TestSSMCentricJoins:

    @pytest.fixture(scope='class')
    def builder(self, sqlContext):
        yield SSMCentricBuilder(conf, sqlContext)

    @pytest.fixture(scope='class')
    def true_stats(self):
        yield TrueStats.get_stats(conf.output_dir, 'ssm_centric')

    @pytest.mark.skip(reason="Can not be implemented before occurrences have "
                             "'occurrence_id' field")
    def test_occurrence_subtree(self, maf_df, builder, true_stats):
        pass

    def test_ssm_centric(self, maf_df, builder, true_stats):
        ssm_df = builder.build(maf_df).ssm_centric

        assert ssm_df.count() == true_stats['count']

        # Consequences per SSM
        es_cps = (ssm_df.select(size('consequence'), 'ssm_id')
                  .toJSON(use_unicode=False).collect())
        es_cps = {d['ssm_id']: d['size(consequence)']
                  for d in map(json.loads, es_cps)}

        assert es_cps == true_stats['cons_per_ssm']

        # Occurrences per SSM
        es_ops = (ssm_df.select(size('occurrence'), 'ssm_id')
                  .toJSON(use_unicode=False).collect())
        es_ops = {d['ssm_id']: d['size(occurrence)']
                  for d in map(json.loads, es_ops)}

        assert es_ops == true_stats['occur_per_ssm']

    def test_ssm_columns(self, maf_df, builder):
        ssm_df = builder.build(maf_df).ssm_centric
        assert 'occurrence_id' in (ssm_df.select(explode('occurrence')
                                                 .alias('occurrence'))
                                         .select('occurrence.*').columns)


@pytest.mark.usefixtures('sqlContext', 'maf_df')
class TestSSMOccurrenceCentricJoins:

    @pytest.fixture(scope='class')
    def builder(self, sqlContext):
        yield SSMOccurrenceCentricBuilder(conf, sqlContext)

    @pytest.fixture(scope='class')
    def true_stats(self):
        yield TrueStats.get_stats(conf.output_dir, 'ssm_occurrence_centric')

    @pytest.mark.ssm_subtree_ssm_occurrence
    def test_ssm_subtree(self, maf_df, builder, true_stats):
        ssm_df = builder.build_ssm(maf_df)

        # Consequences per SSM:
        es_cps = (ssm_df.select(size('ssm.consequence'), 'ssm_id')
                  .toJSON(use_unicode=False).collect())
        es_cps = {d['ssm_id']: d['size(ssm.consequence)']
                  for d in map(json.loads, es_cps)}

        assert es_cps == true_stats['cons_per_ssm']

    def test_ssm_occurrence_columns(self, maf_df, builder):
        ssm_occurrence_df = builder.build(maf_df).ssm_occurrence_centric
        assert 'ssm_occurrence_id' in ssm_occurrence_df.columns

    def test_case_subtree(self, maf_df, builder, true_stats):
        # one gene per ssm for our test mafs
        case_df = builder.build_case(maf_df)

        # Observations per case:
        es_opc = (case_df.select(size('case.observation'), 'case_id')
                  .toJSON(use_unicode=False).collect())
        es_opc = {d['case_id']: d['size(case.observation)']
                  for d in map(json.loads, es_opc)}

        assert es_opc == true_stats['obs_per_case']

    def test_ssm_occurrence(self, maf_df, builder, true_stats):
        ssm_occurrence_df = builder.build(maf_df).ssm_occurrence_centric
        assert ssm_occurrence_df.count() == true_stats['count']


@pytest.mark.usefixtures('sqlContext', 'maf_df')
class TestObservationBuilder:
    """ Test intermediate result from the observation builder """

    @pytest.fixture(scope='class')
    def builder(self, sqlContext):
        yield ObservationBuilder(conf, sqlContext)

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_join_columns(self, builder, maf_df, index_name):
        """ Check for correct columns """
        obs_df = builder.build(maf_df, index_name)
        assert set(obs_df.columns) == {'case_id', 'ssm_id',
                                       'observation', 'occurrence_id'}

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_observation_id(self, builder, maf_df, index_name):
        """ Test that the observation_id was created """
        obs_df = builder.build(maf_df, index_name)
        assert 'observation_id' in (obs_df.select(explode('observation')
                                                  .alias('observation'))
                                          .select('observation.*')
                                          .columns)

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_observation_count(self, builder, maf_df, index_name):
        """ Check for the right number of observations by submitter_id """
        n_observations = maf_df.select('case_id', 'ssm_id').distinct().count()
        assert builder.build(maf_df, index_name).count() == n_observations

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_observation_values(self, builder, maf_df, index_name):
        obs_df = builder.build(maf_df, index_name)

        true_obs = map(json.loads, (maf_df.select('case_id', 'ssm_id')
                                          .distinct().toJSON().collect()))

        obs = map(json.loads, (obs_df.select('case_id', 'ssm_id')
                                     .toJSON().collect()))

        assert sorted(obs) == sorted(true_obs)


@pytest.mark.usefixtures('sqlContext', 'maf_df')
class TestConsequenceBuilder:
    """ Test intermediate result from the transcript builder """

    @pytest.fixture(scope='class')
    def builder(self, sqlContext):
        yield ConsequenceBuilder(conf, sqlContext)

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_consequence_count(self, builder, maf_df, index_name):
        cons_df = builder.build(maf_df, index_name)

        n_consequences = maf_df.select('ssm_id').distinct().count()
        assert cons_df.count() == n_consequences

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_consequence_no_gene(self, builder, maf_df, index_name):
        cons_df = builder.build(maf_df, index_name)
        transcripts = (cons_df.select(explode('consequence.transcript')
                                      .alias('transcript'))
                              .select('transcript.*'))

        # Check that gene not in transctipts
        assert 'gene' not in transcripts.columns

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_consequence_with_gene(self, builder, maf_df, index_name):
        cons_df = builder.build(maf_df, index_name, join_gene=True)
        transcripts = (cons_df.select(explode('consequence.transcript')
                                      .alias('transcript'))
                              .select('transcript.*'))

        assert 'symbol' in (cons_df.select(explode('consequence.transcript.gene')
                                            .alias('gene'))
                                            .select('gene.*')
                                            .columns)

        if index_name != 'case_centric':
            cytobands = transcripts.select('gene.cytoband').collect()
            cytobands = [t['cytoband'] for t in cytobands]
            assert all([type(c) is list for c in cytobands])

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_consequence_with_gene_aa_change(self, builder, maf_df, index_name):
        cons_df = builder.build(maf_df, index_name, add_gene_aa_change=True)

        assert 'gene_aa_change' in cons_df.columns

        data = cons_df.select('gene_aa_change',
                              'consequence.transcript.aa_change',
                              'consequence.transcript.gene.symbol').collect()
        for row in data:
            expected_list = [x for x in zip(row.symbol, row.aa_change)
                             if None not in x]
            expected_list = map(lambda x: '{} {}'.format(*x), expected_list)
            expected_list = sorted(list(set(expected_list)))

            assert sorted(row.gene_aa_change) == expected_list


    @pytest.mark.parametrize('index_name', conf.indices)
    def test_only_related_transcripts(self, builder, maf_df, index_name):
        """
        Test that consequence only contains transcripts from one gene
        """
        cons_df = builder.build(maf_df, index_name, join_gene=True)
        consequences = cons_df.collect()
        for consequence in consequences:
            # Each consequence is a list of transcripts
            transcripts = consequence.asDict(recursive=True)['consequence']
            genes = set()
            for transcript in transcripts:
                genes.add(transcript['transcript']['gene']['gene_id'])
            assert len(genes) == 1

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_all_effects_cols(self, builder, maf_df, index_name):
        fields = ['consequence_type', 'aa_change',
                  'transcript_id', 'ref_seq_accession']
        ssm_trans = builder._build_all_effects_cols(maf_df)

        for f in fields:
            assert f in ssm_trans.columns

    @pytest.mark.parametrize('index_name', conf.indices)
    def test_consequence_id(self, builder, maf_df, index_name):
        """ Test that consequence_id is created correctly """
        cons_df = builder.build(maf_df, index_name, join_gene=False)

        assert 'consequence_id' in cons_df.first().asDict()['consequence'][0]


@pytest.mark.usefixtures('sqlContext', 'maf_df', 'test_index_class')
class TestCaseBuilder:
    """ Test the CaseBuilder functionality for extracting the graph index """

    @pytest.fixture(scope='class')
    def case_df(self, sqlContext):
        yield CaseBuilder(conf, sqlContext).build()

    def test_case_build(self, sqlContext, test_index_class, case_df):
        es = test_index_class
        df = case_df
        assert (df.count() == es.search(conf.graph_index,
                                        conf.graph_document,
                                        size=0)['hits']['total'])

    def test_case_columns(self, sqlContext, case_df):
        """ Test that the right properties were loaded from case docs """
        assert 'case_id' in case_df.columns
        assert 'files' not in case_df.columns
        # Make sure the sample_ids, slide_ids are not present
        assert '_ids' not in ','.join(case_df.columns)
