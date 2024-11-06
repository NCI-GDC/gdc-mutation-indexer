import contextlib
import pathlib
import sqlite3
import tempfile
from typing import Any, Optional

import more_itertools
import mypy_boto3_s3 as s3
from pyspark import sql
from typing_extensions import Self

from mutation_indexer.configuration import databases


class SQLiteDatabase:
    __slots__ = ("_config", "_s3_client", "_context", "_dbfile")

    def __init__(self, config: databases.SQLiteDatabase, s3_client: s3.Client) -> None:
        self._config = config
        self._s3_client = s3_client
        self._context = contextlib.ExitStack()
        self._dbfile: Optional[pathlib.Path] = None

    @property
    def dbfile(self) -> pathlib.Path:
        """The path to the file storing the database.

        WARNING: Cannot be accessed outside of context.
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
        self._s3_client.upload_file(
            str(self.dbfile.absolute()),
            Bucket=self._config.destination.bucket,
            Key=self._config.destination.key,
        )

        self._context.close()

        self._dbfile = None

    def write(
        self, df: sql.DataFrame, insert: str, create: Optional[str] = None
    ) -> None:
        """Writes the data in the data frame to the configured database.

        Args:
            df: The data frame to be written.
            insert: The SQL insert statement to use for inserting values from the
                df.
            create: The SQL statement to create the table in the database. If none
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
