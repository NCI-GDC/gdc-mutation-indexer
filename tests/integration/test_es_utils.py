import pathlib
from typing import Iterable
from unittest import mock

import elasticsearch
import pytest
import yaml
from pyspark import sql

import config
from exports import es_utils
from exports.constants import build
from tests.integration.data import schemas
from tests.integration.utils import test_setup


@pytest.mark.usefixtures("setup_graph_indices")
def test_dataframe_util_read(
    sqlContext: sql.SQLContext,
    default_old_config: config.BaseConfig,
) -> None:
    # Arrange
    search_body = {
        "query": {
            "nested": {
                "path": "cases",
                "query": {
                    "term": {"cases.case_id": "c65d7c98-9678-401b-9f4d-0e1e83be3697"}
                },
            }
        }
    }
    include_fields = (
        "file_id",
        "acl",
        "updated_datetime",
        "cases.demographic.year_of_birth",
        "cases.diagnoses.treatments.state",
        "cases.project.primary_site",
    )
    include_as_arrays = ("acl", "cases.project.primary_site")
    dataframe_util = es_utils.DataFrameUtil(
        default_old_config, sqlContext, mock.MagicMock()
    )

    # Act
    result_df = dataframe_util.read(
        build.IndexType.FILE,
        query=search_body,
        include_fields=include_fields,
        include_as_arrays=include_as_arrays,
        read_metadata=True,
    )

    # Assert
    assert result_df.schema == schemas.load_schema(
        "es_utils/test_dataframe_util_read/final_schema.yaml"
    )
    assert all(
        any(c.case_id == "c65d7c98-9678-401b-9f4d-0e1e83be3697" for c in f.cases)
        for f in result_df.toLocalIterator()
    )


def test_dataframe_util_write(
    sqlContext: sql.SQLContext,
    es_client: elasticsearch.Elasticsearch,
    input_dir: pathlib.Path,
) -> None:
    def load_config(data: dict) -> dict:
        data["build"]["data_release"] = "test_data_frame_util_write"
        data["build"]["index_types"] = ("CASE_CENTRIC",)

        return data

    with open(
        input_dir.joinpath("es_utils/test_dataframe_util_write/case_mappings.yaml"), "r"
    ) as f:
        case_mapping = yaml.safe_load(f)

    conf = test_setup.load_configuraiton(load_config)
    old_conf = config.ConfigAdapter(conf, es_client, mock.MagicMock())
    case_index = conf.elasticsearch.write.indices[build.IndexType.CASE_CENTRIC]
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
    case_df = sqlContext.createDataFrame(case_data)  # type: ignore

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
