from typing import Any, Callable, Iterable
from unittest import mock

import elasticsearch
import pytest
from pyspark import sql

import config
from exports import es_utils
from exports.constants import build
from tests.integration.utils import schema_validation, test_setup


@pytest.mark.usefixtures("setup_graph_indices", "files_with_linked_cases")
@pytest.mark.parametrize(
    ("input_file", "output_file"),
    (
        (
            "input/es_utils/test_get_dataframe_from_es_base.yaml",
            "output/es_utils/test_get_dataframe_from_es_base.yaml",
        ),
        (
            "input/es_utils/test_get_dataframe_from_es_complex.yaml",
            "output/es_utils/test_get_dataframe_from_es_complex.yaml",
        ),
    ),
    ids=("basic", "complex"),
)
def test_data_frame_util_read(
    sqlContext: sql.SQLContext,
    input_file: str,
    output_file: str,
    load_data_from_file: Callable[[str], Any],
    default_old_config: config.BaseConfig,
) -> None:
    # Arrange
    validator = schema_validation.PysparkSchemaValidator()

    inputs = load_data_from_file(input_file)
    index = inputs["index"].upper()
    doc_id = inputs["document_id"]
    kwargs = inputs["kwargs"]

    expected = load_data_from_file(output_file)
    expected_schema = schema_validation.Schema(expected["expected_schema"])
    expected_data = expected["expected_data"]

    dataframe_util = es_utils.DataFrameUtil(
        default_old_config, sqlContext, mock.MagicMock()
    )

    # Act
    result_df = dataframe_util.read(build.IndexType[index], **kwargs)

    # Assert
    validator.validate_schema(result_df.schema, expected_schema)

    result_data = {row[doc_id]: row.asDict(True) for row in result_df.collect()}

    assert result_data == expected_data


def test_data_frame_util_write(
    sqlContext: sql.SQLContext, es_client: elasticsearch.Elasticsearch
) -> None:
    def load_config(data: dict) -> dict:
        data["build"]["data_release"] = "test_data_frame_util_write"
        data["build"]["index_types"] = ("CASE_CENTRIC",)

        return data

    conf = test_setup.load_configuraiton(load_config)
    old_conf = config.ConfigAdapter(conf, es_client, None)
    case_index = conf.elasticsearch.write.indices[build.IndexType.CASE_CENTRIC]
    case_mapping = {
        "settings": {
            "index": {
                "refresh_interval": "1m",
                "number_of_shards": 12,
                "number_of_replicas": 0,
                "mapping.total_fields.limit": 2000,
            },
            "analysis": {
                "analyzer": {
                    "autocomplete_analyzed": {
                        "filter": ["lowercase", "edge_ngram"],
                        "tokenizer": "standard",
                    },
                    "autocomplete_prefix": {
                        "filter": ["lowercase", "edge_ngram"],
                        "tokenizer": "keyword",
                    },
                    "lowercase_keyword": {
                        "filter": ["lowercase"],
                        "tokenizer": "keyword",
                    },
                },
                "filter": {
                    "edge_ngram": {
                        "max_gram": "20",
                        "min_gram": "1",
                        "side": "front",
                        "type": "edge_ngram",
                    }
                },
                "normalizer": {
                    "clinical_normalizer": {
                        "type": "custom",
                        "char_filter": [],
                        "filter": ["lowercase"],
                    }
                },
            },
            "index.mapping.nested_fields.limit": 100,
            "index.mapping.nested_objects.limit": 100000000,
            "index.max_result_window": 100000000,
        },
        "mappings": {
            "_size": {"enabled": True},
            "_source": {"excludes": ["gene.*"]},
            "properties": {
                "available_variation_data": {
                    "type": "keyword",
                    "normalizer": "clinical_normalizer",
                },
                "case_autocomplete": {
                    "fields": {
                        "analyzed": {
                            "analyzer": "autocomplete_analyzed",
                            "search_analyzer": "lowercase_keyword",
                            "type": "text",
                        },
                        "lowercase": {
                            "analyzer": "lowercase_keyword",
                            "type": "text",
                        },
                        "prefix": {
                            "analyzer": "autocomplete_prefix",
                            "search_analyzer": "lowercase_keyword",
                            "type": "text",
                        },
                    },
                    "type": "keyword",
                    "normalizer": "clinical_normalizer",
                },
                "case_id": {
                    "copy_to": ["case_autocomplete"],
                    "type": "keyword",
                    "normalizer": "clinical_normalizer",
                },
                "gene": {
                    "properties": {
                        "gene_id": {"type": "keyword"},
                    }
                },
                "project": {
                    "properties": {
                        "project_id": {
                            "copy_to": ["case_autocomplete"],
                            "type": "keyword",
                        },
                    }
                },
                "samples": {
                    "properties": {
                        "sample_type": {
                            "type": "keyword",
                            "normalizer": "clinical_normalizer",
                        }
                    }
                },
            },
            "dynamic": "strict",
        },
    }
    model_mapper = mock.MagicMock()
    model_mapper.get_normalized_mappings.return_value = case_mapping
    model_mapper_factory = mock.MagicMock(return_value=model_mapper)
    util = es_utils.DataFrameUtil(old_conf, sqlContext, es_client, model_mapper_factory)
    case_data = (
        {
            "available_variation_data": ["ssm", "cnv"],
            "case_id": "case-0",
            "gene": [{"gene_id": "gene-0"}],
            "project": {"project_id": "GDC-TEST"},
            "samples": [{"sample_type": "Normal"}],
        },
    )
    case_df = sqlContext.createDataFrame(case_data)

    try:
        util.write(case_df, build.IndexType.CASE_CENTRIC, "case_id")

        assert es_client.indices.exists(index=case_index)
        assert (
            es_client.count(
                index=case_index,
                body={"query": {"term": {"case_id": "case-0"}}},
            ).get("count")
            == 1
        )
    finally:
        es_client.indices.delete(
            index=case_index,
            ignore_unavailable=True,
        )


@pytest.mark.usefixtures("setup_graph_indices", "files_with_linked_cases")
def test_get_rdd_from_es(
    spark_session: sql.SparkSession, default_old_config: config.BaseConfig
) -> None:
    # Arrange
    included_fields = (
        "file_id",
        "cases.case_id",
    )
    query = {
        "query": {
            "nested": {"path": "cases", "query": {"exists": {"field": "cases.case_id"}}}
        }
    }
    rdd_util = es_utils.RDDUtil(default_old_config, spark_session.sparkContext)

    # Act
    result = rdd_util.get_rdd(
        build.IndexType.FILE, include_fields=included_fields, query=query
    ).first()

    # Assert
    assert len(result) == 2
    assert isinstance(result[0], str)
    assert isinstance(result[1], dict)
    assert "cases" in result[1]
    assert isinstance(result[1]["cases"], Iterable)
    assert "file_id" in result[1]
    assert isinstance(result[1]["file_id"], str)
    assert all("case_id" in case for case in result[1]["cases"])
    assert all(isinstance(case["case_id"], str) for case in result[1]["cases"])
