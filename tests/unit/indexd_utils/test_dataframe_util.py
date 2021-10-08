import itertools
import unittest
from os import path
from typing import Container, Iterable
from unittest import mock

import pytest

from exports import indexd_utils
from pyspark import sql

TEST_FILE_PATH = "tests/unit/data/input/indexd_utils/dataframe_util/test_dataframe_util_header"


class TestDataFrameUtil(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def import_fixtures(self, sqlContext: sql.SQLContext):
        self.sql_context = sqlContext

    def _mock_bulk_request(
        self, missing_dids: Container[str] = frozenset()
    ) -> mock.MagicMock:
        def get_dummies(dids: Iterable[str]) -> Iterable[mock.MagicMock]:
            dids = filter(lambda did: did not in missing_dids, dids)

            for did in dids:
                yield mock.MagicMock(
                    did=did,
                    urls_metadata={
                        "file://{}_{}.tsv".format(path.abspath(TEST_FILE_PATH), did): {
                            "type": "cleversafe",
                            "state": "blocked" if did in missing_dids else "validated",
                        },
                        "Q:\Temp\file.txt": {"type": "s3", "state": "validated"},
                    },
                )

        return mock.MagicMock(side_effect=get_dummies)

    def test__get_dataframe__include_dids(self):
        indexd = mock.MagicMock(bulk_request=self._mock_bulk_request())
        logger = mock.MagicMock()
        util = indexd_utils.DataFrameUtil(indexd, self.sql_context, logger)
        doc_ids = ("1", "2")

        result = util.get_dataframe(doc_ids, enforce_schema=False).collect()
        result_by_file = {
            did: tuple(rows)
            for did, rows in itertools.groupby(result, lambda row: row.did)
        }

        self.assertEqual(6, len(result))
        self.assertSetEqual(frozenset(("1", "2")), frozenset(result_by_file))
        self.assertEqual(3, len(result_by_file["1"]))
        self.assertEqual(3, len(result_by_file["2"]))
