import pytest
from marshmallow import validate

from tests.integration.utils import test_setup


def test__build__validate_index_types() -> None:
    def pre_load(data: dict) -> dict:
        data["build"]["index_types"] = ["GENE_EXPRESSION", "GENE_CENTRIC"]

        return data

    with pytest.raises(validate.ValidationError):
        test_setup.load_configuraiton(pre_load)
