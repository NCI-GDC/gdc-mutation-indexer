import marshmallow
import pytest

from tests.integration.utils import test_setup


def test__build__validate_index_types() -> None:
    with pytest.raises(marshmallow.ValidationError):
        test_setup.load_viz_config(
            {"build": {"index_types": ["GENE_EXPRESSION", "GENE_CENTRIC"]}}
        )
