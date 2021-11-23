import os

import yaml
import pytest


@pytest.fixture(scope="module")
def data_dir():
    current_path = os.path.dirname(os.path.abspath(__file__))

    return os.path.join(current_path, "data")


@pytest.fixture
def fake_hits_and_expectations(data_dir):

    def load_hits_from_file(filename):
        with open(os.path.join(data_dir, filename)) as f:
            contents = yaml.safe_load(f)

        hits = contents["hits"]
        expected = contents["expected"]

        return (
            {"hits": [{"_id": hit.pop("_id"), "_source": hit} for hit in hits]},
            expected,
        )

    return load_hits_from_file
