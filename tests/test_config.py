from collections import namedtuple

import pytest
from indexclient.client import IndexClient, Document

from config import BaseConfig
from tests_config import TestConfig


BOGUS_MAF_URL = 'file://tmp/no.test.data.found.maf'

FakeS3Key = namedtuple('FakeS3Key', 'key')
FakeS3Key.__docs__ = 'Fake Boto S3 Key with a minimal subset of fields.'

test_config = TestConfig()


class TestBaseConfig(object):
    """Tests for the BaseConfig class from the mutation indexer config module.

    Not to be confused with a config for tests (see tests_config.py for that).
    """

    @pytest.fixture
    def base_config(self, setup_graph_indices, monkeypatch):
        """Create a BaseConfig and configure it for the test environment.

        Monkeypatch the config as needed to avoid querying indexd or S3.
        """
        monkeypatch.setattr(IndexClient, 'get', self._stub_indexd_get_maf)
        monkeypatch.setattr(BaseConfig, 'list_bucket', self._stub_list_bucket)

        env_dict = TestConfig.get_env_dict()
        base_config = BaseConfig(env_dict=env_dict)

        # Mutation indexer normally expects the MAFs to be gzipped, but the
        # test MAFs are not gzipped, so override the file filter accordingly.
        base_config.formatted_maf_keywords = 'DR-10.0.somatic.maf'
        return base_config

    @classmethod
    def _stub_indexd_get_maf(cls, did):
        """Retrieve a stub indexd document corresponding to test MAF data.

        Return a document with a validated Cleversafe URL, which is the main
        thing used by BaseConfig. The URL will point to an actual test MAF if
        one exists for the given DID; otherwise, a bogus URL will be returned
        to make BaseConfig happy. Our test graph index refers to MAFs for which
        we don't have test data, so we can't raise if the MAF is missing.
        """
        matching_urls = [url for url in test_config.maf_urls if did in url]
        cleversafe_url = matching_urls[0] if matching_urls else BOGUS_MAF_URL

        doc_json = {
            'urls': [cleversafe_url],
            'urls_metadata': {
                cleversafe_url: {'type': 'cleversafe', 'state': 'validated'}
            },
        }
        return Document(client=None, did=did, json=doc_json)

    @classmethod
    def _stub_list_bucket(cls, bucket_name):
        """Retrieve a stub list of GISTIC URLs corresponding to test data.

        Assert the given bucket name matches the one from the test config, to
        simulate the real list_buckets querying a nonexistent bucket.
        Return fake Boto S3 keys with the URLs of the test GISTIC files.
        """
        assert bucket_name == test_config.s3_gistic_bucket
        return [FakeS3Key(url) for url in test_config.gistic_urls]

    @classmethod
    def _reformat_maf_url(cls, url):
        """Reformat a test MAF URL in the way we expect from BaseConfig.

        Typically, BaseConfig would convert the indexd s3:// URLs to s3a://
        URLs for Spark's consumption. Our test URLs use file:// rather than
        s3://, however, so expect s3a:// to have been prepended.
        """
        return 's3a://{}'.format(url)

    @classmethod
    def _reformat_gistic_url(cls, url):
        """Reformat a test GISTIC URL in the way we expect from BaseConfig.

        BaseConfig prepends the bucket name and scheme to the keys it finds,
        so do the same even though it results in weird-looking URLs.
        """
        return '{bucket}{url}'.format(
            bucket=test_config.s3_gistic_bucket, url=url
        )

    def test_get_maf_urls(self, base_config):
        """Confirm get_maf_urls finds all of the available test MAFs.

        Also verify the URLs are reformatted as expected.
        """
        urls = base_config.get_maf_urls()

        expected_urls = sorted(
            self._reformat_maf_url(url) for url in test_config.maf_urls
        )

        # Sort since the order of the returned URLs shouldn't matter.
        assert sorted(urls) == expected_urls

    @pytest.mark.parametrize('projects, file_patterns', [
        (['TCGA-KICH'], ['KICH']),
        (['TCGA-KIRC', 'TCGA-KIRP'], ['KIRC', 'KIRP']),
    ])
    def test_get_maf_urls__filters_by_project(
        self, projects, file_patterns, base_config
    ):
        """Confirm get_maf_urls only returns files in the given projects.

        Find the expected MAFs using filename patterns that don't look exactly
        like the project IDs, to better represent real-world filenames.
        """
        base_config.projects = projects
        urls = base_config.get_maf_urls()

        expected_urls = sorted(
            self._reformat_maf_url(url)
            for url in test_config.maf_urls
            if any(pattern in url for pattern in file_patterns)
        )

        assert sorted(urls) == expected_urls

    def test_get_gistic_urls(self, base_config):
        """Confirm get_gistic_urls finds all available test GISTIC files.

        Also verify the URLs are reformatted as expected.
        """
        urls = base_config.get_gistic_urls()

        expected_urls = sorted(
            self._reformat_gistic_url(url) for url in test_config.gistic_urls
        )

        assert sorted(urls) == expected_urls

    @pytest.mark.parametrize('projects, file_patterns', [
        (['TCGA-KICH'], ['KICH']),
        (['TCGA-SKCM', 'TCGA-KIRP'], ['SKCM', 'KIRP']),
    ])
    def test_get_gistic_urls__filters_by_project(
        self, projects, file_patterns, base_config
    ):
        """Confirm get_gistic_urls only returns files in the given projects."""
        base_config.projects = projects
        urls = base_config.get_gistic_urls()

        expected_urls = sorted(
            self._reformat_gistic_url(url)
            for url in test_config.gistic_urls
            if any(pattern in url for pattern in file_patterns)
        )

        assert sorted(urls) == expected_urls

    def test_get_index_names__open(self, base_config):
        """Test index name formatting for an open-access build.

        Omit the "study label" and confirm index names are formatted as expected.
        """
        base_config.build_label = 'open_index'
        base_config.study_label = ''

        indices = base_config.get_index_names()

        assert indices == {
            'case_centric': 'open_index__case_centric',
            'cnv_centric': 'open_index__cnv_centric',
            'cnv_occurrence_centric': 'open_index__cnv_occurrence_centric',
            'gene_centric': 'open_index__gene_centric',
            'ssm_centric': 'open_index__ssm_centric',
            'ssm_occurrence_centric': 'open_index__ssm_occurrence_centric',
        }

    def test_get_index_names__controlled(self, base_config):
        """Test index name formatting for a controlled-access build.

        Provide a "study label" and confirm index names are formatted as expected.
        """
        base_config.build_label = 'dr123'
        base_config.study_label = 'fm'

        indices = base_config.get_index_names()

        assert indices == {
            'case_centric': 'dr123__case_centric__fm__controlled',
            'cnv_centric': 'dr123__cnv_centric__fm__controlled',
            'cnv_occurrence_centric': 'dr123__cnv_occurrence_centric__fm__controlled',
            'gene_centric': 'dr123__gene_centric__fm__controlled',
            'ssm_centric': 'dr123__ssm_centric__fm__controlled',
            'ssm_occurrence_centric': 'dr123__ssm_occurrence_centric__fm__controlled',
        }

    def test_get_index_names__double_underscore(self, base_config):
        """Confirm double underscores are disallowed in build and study labels.

        Make sure double underscores are reserved for formatting the final index names,
        so we can't create name collisions by configuring weird labels.
        """
        base_config.build_label = 'open__index'
        base_config.study_label = ''
        with pytest.raises(ValueError):
            base_config.get_index_names()

        base_config.build_label = 'dr123'
        base_config.study_label = 'fm__controlled'
        with pytest.raises(ValueError):
            base_config.get_index_names()
