import pytest
from exports.builders import utils


@pytest.mark.parametrize("filename", ["files-response-1.yaml"])
def test_aliquot_selection(fake_hits_and_expectations, filename):
    response, expected = fake_hits_and_expectations(filename)

    res = utils._select_primary_aliquot_gene_expressions(response["hits"])

    assert len(res) == len(expected)
    assert set(res) == set(expected)
