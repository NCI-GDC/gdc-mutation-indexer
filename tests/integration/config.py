import os
from typing import Any, Optional

import dataclasses
import importlib_resources as resources
import toml

import mutation_indexer
from mutation_indexer.core import configuration
from mutation_indexer.core.constants import master
from mutation_indexer.core.configuration import (
    spark as _spark,
    build as _build,
    builders as _builders,
    aws as _aws,
    indexd as _indexd,
    elasticsearch as _elasticsearch,
)


def _recursive_update(a: dict, b: dict) -> None:
    for key, value in b.items():
        if isinstance(value, dict) and isinstance(a.setdefault(key, {}), dict):
            _recursive_update(a[key], value)
        else:
            a[key] = value


def _update_section(default: dict, updates: Any) -> None:
    if updates:
        _recursive_update(default, dataclasses.asdict(updates))


def _load_config_dict(
    spark_arguments: Optional[_spark.Arguments],
    spark: Optional[_spark.Spark],
    build: Optional[_build.Build],
    builders: Optional[_builders.Builders],
    aws: Optional[_aws.S3],
    indexd: Optional[_indexd.IndexD],
    elasticsearch: Optional[_elasticsearch.Elasticsearch],
) -> dict:
    default_build = {
        "data_release": "test",
        "build_version": "v0",
        "index_types": ("case_centric", "gene_centric"),
        "projects": (),
        "config_dir": "./",
    }
    default = toml.loads(resources.read_text(mutation_indexer, "config.toml"))

    default["build"].update(default_build)

    _update_section(default["spark_arguments"], spark_arguments)
    _update_section(default["spark"], spark)
    _update_section(default["build"], build)
    _update_section(default["builders"], builders)
    _update_section(default["aws"], aws)
    _update_section(default["indexd"], indexd)
    _update_section(default["elasticsearch"], elasticsearch)


class Configuation(configuration.Configuration):
    def __init__(
        self,
        spark_arguments: Optional[_spark.Arguments] = None,
        spark: Optional[_spark.Spark] = None,
        build: Optional[_build.Build] = None,
        builders: Optional[_builders.Builders] = None,
        aws: Optional[_aws.S3] = None,
        indexd: Optional[_indexd.IndexD] = None,
        elasticsearch: Optional[_elasticsearch.Elasticsearch] = None,
    ) -> None:
        default = _load_config_dict(
            spark_arguments,
            spark,
            build,
            builders,
            aws,
            indexd,
            elasticsearch,
        )
        config: configuration.Configuration = configuration.CONFIG_SCHEMA.load(default)

        super().__init__(
            config.spark_arguments,
            config.spark,
            config.build,
            config.builders,
            config.aws,
            config.indexd,
            config.elasticsearch,
        )


class TestConfig(configuration.ConfigAdapter):
    __test__ = False

    # Directories used for test data
    root_dir = os.path.dirname(os.path.realpath(mutation_indexer.__file__))
    test_dir = os.path.dirname(os.path.realpath(__file__))
    schemas_dir = os.path.join(root_dir, "schemas")
    data_dir = os.path.join(test_dir, "data")
    log_dir = os.path.join(data_dir, "log")
    input_dir = os.path.join(data_dir, "input")
    maf_dir = os.path.join(input_dir, "maf", "merged_aliquot")
    gistic_dir = os.path.join(input_dir, "cnv")

    # Initialize test directory tree if incomplete
    for directory in [log_dir, input_dir, maf_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    # Whether or not to rebuild graph index after every test
    graph_force_build = False

    # Whether or not to print document mismatches to stdout when testing
    print_data_errors = False

    # Whether or not to skip field-by-field data tests
    skip_in_depth_tests = True

    # Switch tests based on pruned/not_pruned version of indices
    indices_are_pruned = True

    # This grouping is useful to understand which tests to run
    main_indices = ["case_centric", "gene_centric"]
    ssm_indices = ["ssm_centric", "ssm_occurrence_centric"]
    cnv_indices = ["cnv_centric", "cnv_occurrence_centric"]

    # Additional test files
    doc_files = {
        "case": os.path.join(input_dir, "cases.ndjson.gz"),
        "file": os.path.join(input_dir, "files.ndjson.gz"),
    }

    # Additional exports files
    citobands_file = os.path.join(input_dir, "genes.cytobands.tsv.gz")
    census_file = os.path.join(input_dir, "cancer_gene_census_set.tsv.gz")
    gene_model_file = os.path.join(input_dir, "genes.ndjson.gz")

    percentile_threshold = {
        "genes_per_case": 100,
        "occurrences_per_ssm": 100,
        "consequences_per_ssm": 100,
        "observations_per_ssm": 100,
        "occurrences_per_cnv": 100,
    }

    cache_dataframes = {
        "mafs": True,
        "cases": False,
        "case_centric": True,
        "gene_centric": True,
        "ssm_centric": True,
        "ssm_occurrence_centric": True,
        "cnv_centric": True,
        "cnv_occurrence_centric": True,
    }

    def __init__(self):
        config = Configuation()

        super().__init__(config=config, elasticsearch=None, indexd=None)

        # To make it more convenient to write tests that load in data, put the names
        # of the graph indices in a dictionary keyed by doc type.
        self.graph_indices = {
            "case": self.graph_case_index,
            "file": self.graph_file_index,
        }

    def validate_indices(self, indices):
        existing_indices = self.es.indices.get_alias().keys()
        name_collisions = [
            name for name in indices.values() if name in existing_indices
        ]
        if name_collisions:
            for collision in name_collisions:
                self.es.indices.delete(collision)
            self.es.indices.refresh()

    @classmethod
    def get_env_dict(cls):
        """
        Simulate environment variables with dictionary
        """
        env_dict = {
            "BUILD_TYPE": "develop",
            "ES_NODES": os.getenv("ES_NODES_TEST", "http://localhost"),
            "ES_HOST": os.getenv("ES_HOST_TEST", "http://localhost"),
            "ES_PORT": "9200",
            "SOURCE_ES_HOST": os.getenv("SOURCE_ES_HOST_TEST", "http://localhost"),
            "SOURCE_ES_PORT": "9200",
            "S3_HOST": "fake_s3",
            "S3_ACCESS_KEY": "fake_s3_access",
            "S3_SECRET_KEY": "fake_s3_secret",
            "DF_REPARTITION": "10",
            "INDEXD_USER": "fake_indexd_user",
            "INDEXD_PASS": "fake_indexd_pass",
        }

        return env_dict

    def get_maf_urls(self):
        return [
            "file://" + os.path.join(self.maf_dir, f)
            for f in os.listdir(self.maf_dir)
            if f.endswith("maf")
        ]

    def get_gistic_urls(self):
        return [
            "file://" + os.path.join(self.gistic_dir, f)
            for f in os.listdir(self.gistic_dir)
            if f.endswith(".tsv")
        ]
