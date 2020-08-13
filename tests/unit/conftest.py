import pytest


@pytest.fixture
def fake_es_hits():
    response = {
        "hits": {
            "hits": [
                {
                    "file_id": "file-1",
                    "created_datetime": "2020-01-02",
                    "cases": [
                        {
                            "case_id": "case-1",
                            "samples": [{"sample_type": "Primary Tumor"}]
                        }
                    ]
                },
                {
                    "file_id": "file-2",
                    "created_datetime": "2020-01-01",
                    "cases": [
                        {
                            "case_id": "case-1",
                            "samples": [{"sample_type": "Primary Tumor"}]
                        }
                    ]
                }
            ]
        }
    }

    return response
