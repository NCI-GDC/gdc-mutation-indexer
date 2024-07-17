import logging
import pathlib
from collections.abc import Callable, Iterable
from typing import Any
from unittest import mock

import elasticsearch
import pytest
import yaml
from pyspark import sql
from pyspark.sql import types

from mutation_indexer import configuration, es_utils
from mutation_indexer.constants import build
from tests.integration.utils import test_setup

logger = logging.getLogger(__name__)


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
    ids=("base", "complex"),
)
def test_data_frame_util_read(
    spark_session: sql.SparkSession,
    input_file: str,
    output_file: str,
    load_data_from_file: Callable[[str], Any],
    default_config: configuration.Configuration,
) -> None:
    # Arrange
    inputs = load_data_from_file(input_file)
    index = inputs["index"].upper()
    doc_id = inputs["document_id"]
    kwargs = inputs["kwargs"]

    expected = load_data_from_file(output_file)
    expected_schema = types.StructType.fromJson(expected["expected_schema"])
    expected_data = expected["expected_data"]

    dataframe_util = es_utils.DataFrameUtil(
        default_config.elasticsearch,
        spark_session,
        mock.MagicMock(),
        es_utils.MappingsLoader(),
        es_utils.SchemaLoader(),
    )

    # Act
    result_df = dataframe_util.read(build.IndexType[index], **kwargs)

    # Assert
    assert result_df.schema == expected_schema

    result_data = {row[doc_id]: row.asDict(True) for row in result_df.collect()}

    assert result_data == expected_data


def test_data_frame_util_write(
    input_dir: pathlib.Path,
    spark_session: sql.SparkSession,
    es_client: elasticsearch.Elasticsearch,
) -> None:
    def load_config(data: dict) -> dict:
        data["build"]["data_release"] = "test_data_frame_util_write"
        data["build"]["index_types"] = ("CASE_CENTRIC",)

        return data

    case_mapping_file = input_dir / "es_utils/test_data_frame_util_write.yaml"

    with open(case_mapping_file) as f:
        model_mapper = mock.MagicMock(**yaml.safe_load(f))

    conf = test_setup.load_configuration(load_config)
    case_index = conf.elasticsearch.write.indices[build.IndexType.CASE_CENTRIC]
    mappings_loader = mock.MagicMock()
    mappings_loader.load_mapper.return_value = model_mapper
    util = es_utils.DataFrameUtil(
        conf.elasticsearch,
        spark_session,
        es_client,
        mappings_loader,
        es_utils.SchemaLoader(),
    )
    case_data = (
        {
            "available_variation_data": ["ssm", "cnv"],
            "case_id": "case-0",
            "gene": [{"gene_id": "gene-0"}],
            "project": {"project_id": "GDC-TEST"},
            "samples": [{"sample_type": "Normal"}],
        },
    )
    case_df = spark_session.createDataFrame(case_data)  # type: ignore

    with test_setup.IndexManager(
        conf,
        es_client,
        logger,
        index_types=(build.IndexType.CASE_CENTRIC,),
        skip_creation=True,
    ):
        util.write(case_df, build.IndexType.CASE_CENTRIC, "case_id")

        assert es_client.indices.exists(index=case_index)
        assert (
            es_client.count(
                index=case_index,
                body={"query": {"term": {"case_id": "case-0"}}},
            ).get("count")
            == 1
        )


@pytest.mark.usefixtures("setup_graph_indices", "files_with_linked_cases")
def test_get_rdd_from_es(
    spark_session: sql.SparkSession, default_config: configuration.Configuration
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
    rdd_util = es_utils.RDDUtil(
        default_config.elasticsearch, spark_session.sparkContext
    )

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
