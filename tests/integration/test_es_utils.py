from typing import Iterable

import pytest
from normalizer import mapper
from pyspark import sql

from exports import es_utils
from exports.constants import build
from tests.integration import config
from tests.integration.utils import schema_validation

conf = config.TestConfig()


@pytest.fixture
def diagnoses_missing_field(source_es_client):
    graph_mapper = mapper.ModelMapper("gdc_from_graph", "case")
    centric_mapper = mapper.ModelMapper("case_centric")

    graph_diagnoses = graph_mapper.select_mapping("diagnoses")["properties"]
    centric_diagnoses = centric_mapper.select_mapping("diagnoses")["properties"]

    diff = set(graph_diagnoses.keys()) - set(centric_diagnoses.keys())

    default_blacklist = {
        f.split(".")[1] for f in conf.case_exclude_fields if f.startswith("diagnoses.")
    }

    diff = diff - default_blacklist

    target_field = None
    for field in diff:
        field_type = graph_diagnoses[field]["type"]
        if field_type == "keyword":
            target_field = field
            break

    assert target_field

    dummy_document = {"case_id": "foo", "diagnoses": {target_field: "dummy-value"}}

    index_name = conf.graph_case_index
    result = source_es_client.index(index=index_name, body=dummy_document)
    source_es_client.indices.refresh(index_name)

    assert result["result"] == "created"

    yield dummy_document

    source_es_client.delete(index=index_name, id=result["_id"])
    source_es_client.indices.refresh(index_name)


@pytest.mark.usefixtures("setup_graph_indices")
def test_missing_fields(diagnoses_missing_field):
    result = es_utils.get_non_null_fields(conf)

    field, _ = diagnoses_missing_field["diagnoses"].popitem()
    expected_field = "diagnoses.{}".format(field)

    assert result
    assert set(result) == {expected_field}


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
def test_get_dataframe_from_es(
    sqlContext, input_file, output_file, load_data_from_file
):
    # Arrange
    conf = config.TestConfig()
    validator = schema_validation.PysparkSchemaValidator()

    inputs = load_data_from_file(input_file)
    index = inputs["index"].upper()
    doc_id = inputs["document_id"]
    kwargs = inputs["kwargs"]

    expected = load_data_from_file(output_file)
    expected_schema = schema_validation.Schema(expected["expected_schema"])
    expected_data = expected["expected_data"]

    dataframe_util = es_utils.DataFrameUtil(conf, sqlContext)

    # Act
    result_df = dataframe_util.get_dataframe(build.IndexType[index], **kwargs)

    # Assert
    validator.validate_schema(result_df.schema, expected_schema)

    result_data = {row[doc_id]: row.asDict(True) for row in result_df.collect()}

    assert result_data == expected_data


@pytest.mark.usefixtures("setup_graph_indices", "files_with_linked_cases")
def test_get_rdd_from_es(spark_session: sql.SparkSession):
    # Arrange
    conf = config.TestConfig()
    included_fields = (
        "file_id",
        "cases.case_id",
    )
    query = {
        "query": {
            "nested": {"path": "cases", "query": {"exists": {"field": "cases.case_id"}}}
        }
    }
    rdd_util = es_utils.RDDUtil(conf, spark_session.sparkContext)

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
