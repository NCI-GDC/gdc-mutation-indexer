import pytest
from marshmallow import exceptions

from tests.integration.utils import test_setup


def test__build__validate_index_types() -> None:
    with pytest.raises(exceptions.ValidationError):
        test_setup.load_configuration(
            {"build": {"index_types": ["GENE_EXPRESSION", "GENE_CENTRIC"]}}
        )
