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
        if not self._dbfile:
            raise RuntimeError("Cannot access DB outside of a context.")

        return self._dbfile

    def __enter__(self) -> Self:
        tmp_file = self._context.enter_context(tempfile.NamedTemporaryFile())
        self._dbfile = pathlib.Path(tmp_file.name)

        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
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
        with sqlite3.connect(self.dbfile) as connection:
            cursor = connection.cursor()

            if create:
                cursor.execute(create)

            for batch in more_itertools.ichunked(
                df.toLocalIterator(), self._config.batch_size
            ):
                cursor.executemany(insert, map(tuple, batch))
