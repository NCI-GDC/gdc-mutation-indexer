import marshmallow
import marshmallow_dataclass

from exports.configuration import (
    aws,
    build,
    builders,
    elasticsearch,
    environment,
    indexd,
    marshmallow_extensions,
    spark,
)

@marshmallow_dataclass.dataclass(
    frozen=True, base_schema=marshmallow_extensions.ExtendedSchema
)
class Configuration:
    aws: aws.AWS
    build: build.Build
    builders: builders.Builders
    elasticsearch: elasticsearch.Elasticsearch
    environment: environment.Environment
    indexd: indexd.IndexD
    spark: spark.Spark

CONFIG_SCHEMA: marshmallow.Schema = Configuration.Schema()
OBFUSCATED_CONFIG_SCHEMA: marshmallow.Schema = Configuration.Schema(context={"is_obfuscated": True})
