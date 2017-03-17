import pytest
import json

from pyspark.sql.functions import size, explode
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

conf = TestConfig()


@pytest.mark.usefixtures('sqlContext', 'maf_df')
class TestBuildersSimple:
    @pytest.mark.parametrize("builder", [CaseCentricBuilder,
                                         GeneCentricBuilder,
                                         SSMCentricBuilder,
                                         SSMOccurrenceCentricBuilder])
    def test_simple_build(self, maf_df, sqlContext, builder):
        builder(conf, sqlContext).build(maf_df)


@pytest.mark.usefixtures('sqlContext', 'maf_df')
class TestCaseCentricJoins:
    ''' Test intermediate result from the case centric builder '''

    @pytest.fixture(scope='class')
    def builder(self, sqlContext):
        yield CaseCentricBuilder(conf, sqlContext)

    @pytest.fixture(scope='class')
    def true_stats(self):
        yield TrueStats.get_stats(conf.output_dir, 'case_centric')

    @pytest.mark.ssm_subtree_case
    def test_ssm_subtree(self, maf_df, builder, true_stats):
        ssm_df = builder.build_ssm(maf_df)

        # Correct number of ssm's
        assert ssm_df.count() == maf_df.select('ssm_id').distinct().count()

        es_ops, es_cps = get_ssm_subtree_stats(ssm_df, 'case_centric')

        # Correct number of Observations per SSM
        assert es_ops == true_stats['obs_per_ssm']

        # Correct number of Consequences per SSM
        assert es_cps == true_stats['cons_per_ssm']

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

    def test_case_gene(self, maf_df, builder, true_stats):
        case_df = builder.build(maf_df).case_centric
        # Correct number of Case documents
        assert case_df.count() == true_stats['count']

        # Correct number of Genes per Case
        es_gpc = (case_df.select(size('gene'), 'case_id')
                  .toJSON(use_unicode=False).collect())
        es_gpc = {d['case_id']: d['size(gene)']
                  for d in map(json.loads, es_gpc)}
        assert es_gpc == true_stats['genes_per_case']


@pytest.mark.usefixtures('sqlContext', 'maf_df')
class TestGeneCentricJoins:
    ''' Test intermediate result from the case centric builder '''

    @pytest.fixture(scope='class')
    def builder(self, sqlContext):
        yield GeneCentricBuilder(conf, sqlContext)

    @pytest.fixture(scope='class')
    def true_stats(self):
        yield TrueStats.get_stats(conf.output_dir, 'gene_centric')

    @pytest.mark.ssm_subtree_gene
    def test_ssm_subtree(self, maf_df, builder, true_stats):
        ssm_df = builder.build_ssm(maf_df)

        # Correct number of ssm's
        assert ssm_df.count() == maf_df.select('ssm_id').distinct().count()

        es_ops, es_cps = get_ssm_subtree_stats(ssm_df, 'gene_centric')

        # Correct number of Observations per SSM
        assert es_ops == true_stats['obs_per_ssm']

        # Correct number of Consequences per SSM
        assert es_cps == true_stats['cons_per_ssm']

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
        assert 'occurrence_id' in ssm_occurrence_df.columns

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
    ''' Test intermediate result from the observation builder '''

    @pytest.fixture(scope='class')
    def builder(self, sqlContext):
        yield ObservationBuilder(conf, sqlContext)

    @pytest.fixture(scope='class')
    def build_df(self, maf_df, builder):
        yield builder.build(maf_df)

    def test_join_columns(self, build_df):
        ''' Check for correct columns '''
        obs_df = build_df
        assert set(obs_df.columns) == {'case_id', 'ssm_id', 'observation'}

    def test_observation_count(self, build_df, maf_df):
        ''' Check for the right number of observations by submitter_id '''
        n_observations = maf_df.select('case_id', 'ssm_id').distinct().count()
        assert build_df.count() == n_observations

    def test_observation_values(self, build_df, maf_df):
        obs_df = build_df

        true_obs = map(json.loads, (maf_df.select('case_id', 'ssm_id')
                                          .distinct().toJSON().collect()))

        obs = map(json.loads, (obs_df.select('case_id', 'ssm_id')
                                     .toJSON().collect()))

        assert sorted(obs) == sorted(true_obs)


@pytest.mark.usefixtures('sqlContext', 'maf_df')
class TestConsequenceBuilder:
    ''' Test intermediate result from the transcript builder '''

    @pytest.fixture(scope='class')
    def builder(self, sqlContext):
        yield ConsequenceBuilder(conf, sqlContext)

    @pytest.fixture(scope='class')
    def build_df(self, maf_df, builder):
        yield builder.build(maf_df, join_gene=False)

    def test_consequence_count(self, maf_df, build_df):
        cons_df = build_df

        n_consequences = maf_df.select('ssm_id').distinct().count()
        assert cons_df.count() == n_consequences

    def test_consequence_no_gene(self, build_df):
        cons_df = build_df
        transcripts = (cons_df.select(explode('consequence.transcript')
                                      .alias('transcript'))
                              .select('transcript.*'))

        # Check that gene not in transctipts
        assert 'gene' not in transcripts.columns

    def test_consequence_with_gene(self, maf_df, builder):
        cons_df = builder.build(maf_df, join_gene=True)
        transcripts = (cons_df.select(explode('consequence.transcript')
                                      .alias('transcript'))
                              .select('transcript.*'))

        cytobands = transcripts.select('gene.cytoband').collect()
        cytobands = [t['cytoband'] for t in cytobands]
        assert all([type(c) is list for c in cytobands])

    def test_all_effects_cols(self, maf_df, builder):
        fields = ['do_not_use', 'consequence_type', 'aa_change',
                  'transcript_id', 'ref_seq_accession']
        ssm_trans = builder._build_all_effects_cols(maf_df)

        for f in fields:
            assert f in ssm_trans.columns


    def test_consequence_id(self, maf_df, builder):
        """ Test that consequence_id is created correctly """
        cons_df = builder.build(maf_df, join_gene=False)

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
