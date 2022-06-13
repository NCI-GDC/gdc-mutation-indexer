from pyspark.sql import types

from mutation_indexer.driver import schemas


def load_schema(schema_filename: str) -> types.StructType:
    return schemas.load_schema(__name__, schema_filename)
