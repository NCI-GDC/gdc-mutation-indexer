import pytest
import json

from pyspark.sql.functions import col, size
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

    def test_case_centric_ssm_subtree(self, maf_df, builder):
        ssm = builder.build_ssm(maf_df)
        assert ssm.count() == 18
        assert ssm.filter(size(col('observation')) == 1).count() == 18
        assert (ssm.filter(size(col('consequence')) == 17)
                   .filter(ssm.gene_id == 'ENSG00000029363').count() == 1)
        assert (ssm.filter(size(col('consequence')) == 16)
                   .filter(ssm.gene_id == 'ENSG00000079841').count() == 1)

    def test_gene_ssm(self, maf_df, builder):
        # one gene per ssm for our test mafs
        gene_df = builder.build_gene_ssm(maf_df)
        assert (gene_df.filter(size('gene.ssm') == 1).count() == 18)

    def test_case_gene(self, maf_df, builder):
        case_df = builder.build(maf_df).case_centric
        assert case_df.filter(size('gene') == 1).count() == 6
        long_gene_submitters = (case_df.filter(size('gene') == 9)
                                       .select('submitter_id')
                                       .collect())
        assert len(long_gene_submitters) == 1
        assert long_gene_submitters[0].submitter_id == 'TCGA-A4-A6HP'


@pytest.mark.usefixtures('sqlContext', 'maf_df')
class TestGeneCentricJoins:
    ''' Test intermediate result from the case centric builder '''

    @pytest.fixture(scope='class')
    def builder(self, sqlContext):
        yield GeneCentricBuilder(conf, sqlContext)

    def test_ssm(self, maf_df, builder):
        ssm = builder.build_ssm(maf_df)
        assert ssm.count() == 18
        assert ssm.filter(size(col('observation')) == 1).count() == 18
        assert (
            ssm.filter(size(col('consequence')) == 17)
            .filter(ssm.gene_id == 'ENSG00000029363').count()) == 1
        assert (
            ssm.filter(size(col('consequence')) == 16)
            .filter(ssm.gene_id == 'ENSG00000079841').count()) == 1

    def test_case_with_gene_id(self, maf_df, builder, sqlContext):
        case_df = CaseBuilder(conf, sqlContext).build()
        case_gene_id = builder.build_case_with_gene_id(maf_df)
        assert set(['gene_id'] + case_df.columns) == set(case_gene_id.columns)
        assert case_gene_id.count() == 18

    def test_case_ssm(self, maf_df, builder):
        # one ssm per case
        case_ssm = builder.build_case_ssm(maf_df)
        assert case_ssm.filter(size('case.ssm') == 1).count() == 18

    def test_gene_case(self, maf_df, builder):
        # one case per gene
        gene_centric = builder.build(maf_df).gene_centric
        assert gene_centric.filter(size('case') == 1).count() == 18


@pytest.mark.usefixtures('sqlContext', 'maf_df')
class TestSSMOccurrenceCentricJoins:

    @pytest.fixture(scope='class')
    def builder(self, sqlContext):
        yield SSMOccurrenceCentricBuilder(conf, sqlContext)

    def test_ssm(self, maf_df, builder):
        ssm = builder.build_ssm(maf_df)
        assert ssm.count() == 18
        assert (ssm.filter(size(col('consequence')) == 17)
                   .filter(ssm.ssm_id == 'ed0bbbd9-4f0d-5697-bd6c-0cbd72fc6bd0')
                   .count()) == 1
        assert (ssm.filter(size(col('consequence')) == 16)
                   .filter(ssm.ssm_id == '0d33430c-c896-53c6-9b5f-c1ac68dcc132')
                   .count()) == 1

    def test_case(self, maf_df, builder):
        # one gene per ssm for our test mafs
        case_df = builder.build_case(maf_df)
        assert case_df.count() == 18
        assert (case_df.filter(size('observation') == 1).count()) == 18

    def test_ssm_occurrence(self, maf_df, builder):
        ssm_occurrence_df = builder.build(maf_df).ssm_occurrence_centric
        assert ssm_occurrence_df.count() == 18
        assert (ssm_occurrence_df.filter(size(col('case.observation')) == 1)
                                 .count()) == 18
        assert (ssm_occurrence_df.filter(size(col('case.observation')) == 1)
                                 .filter(col('ssm_occurrence_id') ==
                                         '4d96cd1a-d679-52ba-bfc3-fe14cceb6d24')
                                 .count()) == 1


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
        ''' Check for columns that are used by other builders to join on '''
        obs_df = build_df
        assert '_case_submitter_id' in obs_df.columns

    def test_observation_size(self, build_df):
        ''' Check for the right number of observations by submitter_id '''
        obs_df = build_df
        assert obs_df.count() == 18

    def test_observation_values(self, build_df):
        obs_df = build_df
        # There are three observations for TCGA-KL-8328
        case_row = obs_df.where(obs_df._case_submitter_id == 'TCGA-KL-8328')
        assert case_row.count() == 3

        case_row = obs_df.where(obs_df._case_submitter_id == 'TCGA-B2-4102')
        assert case_row.count() == 1
        obs = json.loads(case_row.toJSON().collect()[0])
        assert obs['ssm_id'] == '1f18a8c6-d828-5a64-b26a-4e1c7a1734db'
        assert len(obs['observation']) == 1
        obs = obs['observation'][0]
        assert (obs[u'tumor_genotype'] == {
                                   "tumor_seq_allele1": "T",
                                   "tumor_seq_allele2": "G"
                                      })

        assert (obs['read_depth'] == {
                                   "t_depth": 162,
                                   "t_alt_count": 41,
                                   "t_ref_count": 121,
                                   "n_depth": 162
                              })

        assert (obs['variant_calling'] == {
                                    'variant_process': 'masked',
                                    'variant_caller': 'mutect2'
                              })


@pytest.mark.usefixtures('sqlContext', 'maf_df')
class TestConsequenceBuilder:
    ''' Test intermediate result from the transcript builder '''

    @pytest.fixture(scope='class')
    def builder(self, sqlContext):
        yield ConsequenceBuilder(conf, sqlContext)

    def test_transcript_without_gene(self, maf_df, builder):
        tran_df = builder.build(maf_df, join_gene=False)
        tran_df = tran_df.where(tran_df.ssm_id == '1a191926-2c54-539a-817d-6196d105bb38')
        assert tran_df.count() == 1
        cons = tran_df.collect()[0]['consequence']
        assert len(cons) == 7
        cons = [r['transcript'] for r in cons]
        assert all(['gene' not in t for t in cons])

    def test_transcript_values(self, maf_df, builder):
        tran_df = builder.build(maf_df, join_gene=True)
        tran_df = tran_df.where(tran_df.ssm_id == '1a191926-2c54-539a-817d-6196d105bb38')
        assert tran_df.count() == 1
        cons = tran_df.collect()[0]['consequence']
        assert len(cons) == 7
        cytobands = [r['transcript']['gene']['cytoband'] for r in cons]
        assert all([type(c) is list for c in cytobands ])

    def test_all_effects_cols(self, maf_df, builder):
        fields = [ 'do_not_use', 'consequence_type', 'aa_change',
                   'transcript_id', 'ref_seq_accession' ]
        ssm_trans = builder._build_all_effects_cols(maf_df)

        for f in fields:
            assert f in ssm_trans.columns

        # this gene has 20 transcripts overall
        assert ssm_trans.filter(
            ssm_trans.gene_id == 'ENSG00000079841').count() == 16


@pytest.mark.usefixtures('sqlContext', 'maf_df', 'test_index_class')
class TestCaseBuilder:

    def test_case_build(self, sqlContext, test_index_class):
        es = test_index_class
        df = CaseBuilder(conf, sqlContext).build()
        assert (df.count() == es.search(conf.graph_index,
                                        conf.graph_document,
                                        size=0)['hits']['total'])
