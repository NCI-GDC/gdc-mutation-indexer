import sqlite3
from unittest import mock

import pytest
from pyspark import sql

from mutation_indexer.configuration import databases
from mutation_indexer.databases import sqlite

CREATE = "CREATE TABLE IF NOT EXISTS cases(case_id TEXT PRIMARY KEY, submitter_id TEXT)"
INSERT = "INSERT INTO cases (case_id, submitter_id) VALUES (?, ?)"


class TestSQLiteDatabase:
    def _arrange_config(self) -> databases.SQLiteDatabase:
        return mock.MagicMock()

    def test__enter__creates_db(self) -> None:
        config = self._arrange_config()

        with sqlite.SQLiteDatabase(config, mock.MagicMock()) as db:
            assert db.dbfile.exists() and db.dbfile.is_file()

    def test__exit__uploads_and_deletes_db_file(self) -> None:
        config = self._arrange_config()
        s3 = mock.MagicMock()

        with sqlite.SQLiteDatabase(config, s3) as db:
            dbfile = db.dbfile

            assert dbfile.exists() and dbfile.is_file()

        s3.upload_file.assert_called_once_with(
            str(dbfile.absolute()),
            Bucket=config.destination.bucket,
            Key=config.destination.key,
        )

        assert not dbfile.exists()

    def test__dbfile__cannot_be_accessed_outside_context(self) -> None:
        config = self._arrange_config()
        s3 = mock.MagicMock()
        db = sqlite.SQLiteDatabase(config, s3)

        with pytest.raises(
            RuntimeError, match=r"Cannot access DB outside of a context\."
        ):
            db.dbfile

        with db:
            assert db.dbfile.exists() and db.dbfile.is_file()

        with pytest.raises(
            RuntimeError, match=r"Cannot access DB outside of a context\."
        ):
            db.dbfile

    def test__write__data_writes_to_table(
        self, spark_session: sql.SparkSession
    ) -> None:
        config = self._arrange_config()
        df = spark_session.createDataFrame(
            (("case-0", "sub-case-0"),),
            schema=("case_id", "submitter_id"),
        )

        with sqlite.SQLiteDatabase(config, mock.MagicMock()) as db:
            db.write(df, INSERT, CREATE)

            with sqlite3.connect(db.dbfile) as connection:
                cursor = connection.execute(
                    "SELECT * FROM cases WHERE case_id = 'case-0'"
                )

                assert cursor.fetchone() == ("case-0", "sub-case-0")

    def test__write__nonexistant_table_raises(
        self, spark_session: sql.SparkSession
    ) -> None:
        config = self._arrange_config()
        df = spark_session.createDataFrame(
            (("gene-0", "symbol"),),
            schema=("case_id", "symbol"),
        )

        with sqlite.SQLiteDatabase(config, mock.MagicMock()) as db:
            with pytest.raises(sqlite3.OperationalError, match="no such table: cases"):
                db.write(df, INSERT)
