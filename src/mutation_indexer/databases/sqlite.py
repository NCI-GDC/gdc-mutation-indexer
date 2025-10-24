"""A module for enabling reading from and writing to SQLite databases."""

import contextlib
import logging
import pathlib
import sqlite3
import tempfile
from typing import Any, Self

import more_itertools
import mypy_boto3_s3 as s3
from pyspark import sql

from mutation_indexer.configuration import databases

logger = logging.getLogger(__name__)


class SQLiteDatabase:
    __slots__ = ("_config", "_s3_client", "_context", "_dbfile")

    def __init__(self, config: databases.SQLiteDatabase, s3_client: s3.Client) -> None:
        """A class for managing a SQLite database.

        Args:
            config: The configuration for the database.
            s3_client: The client used for uploading the final database to s3.
        """
        self._config = config
        self._s3_client = s3_client
        self._context = contextlib.ExitStack()
        self._dbfile: pathlib.Path | None = None

    @property
    def dbfile(self) -> pathlib.Path:
        """The path to the file storing the database.

        Raises:
            RuntimeError: When accessed outside of context.
        """
        if not self._dbfile:
            raise RuntimeError("Cannot access DB outside of a context.")

        return self._dbfile

    def __enter__(self) -> Self:
        """Enters the database's context.

        NOTE: When this context is entered, these things happen:
        1) A temporary file is created to store the database.
        """
        tmp_file = self._context.enter_context(tempfile.NamedTemporaryFile())
        self._dbfile = pathlib.Path(tmp_file.name)

        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        """Exits the database's context.

        NOTE: When this context is exited, these things happen:
        1) The database file is loaded to s3.
        2) The local copy of the database file is deleted.
        """
        destination = self._config.destination

        self._s3_client.upload_file(
            str(self.dbfile.absolute()),
            Bucket=destination.bucket,
            Key=destination.key,
        )
        logger.info(f"Uploaded SQLite DB to: s3://{destination.bucket}/{destination.key}")

        self._context.close()

        self._dbfile = None

    def write(self, df: sql.DataFrame, insert: str, create: str | None = None) -> None:
        """Writes the data in the data frame to the configured database.

        Args:
            df: The data frame to be written.
            insert: The SQL insert statement to use for inserting values from the
                df.
            create: The SQL statement to create the table in the database. If None,
                the table must already exist in the database.
        """
        with sqlite3.connect(self.dbfile) as connection:
            cursor = connection.cursor()

            if create:
                cursor.execute(create)

            for batch in more_itertools.ichunked(
                df.toLocalIterator(), self._config.batch_size
            ):
                cursor.executemany(insert, map(tuple, batch))
