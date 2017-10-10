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


conf = TestConfig()


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
    def case_df(self, sqlContext, maf_df):
        yield CaseBuilder(conf, sqlContext).build(maf_df)

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
