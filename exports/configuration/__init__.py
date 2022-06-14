import marshmallow
import marshmallow_dataclass

from exports.configuration import (
    aws,
    build,
    builders,
    elasticsearch,
    environment,
    indexd,
    spark,
)


@marshmallow_dataclass.dataclass(frozen=True)
class Configuration:
    aws: aws.AWS
    build: build.Build
    builders: builders.Builders
    elasticsearch: elasticsearch.Elasticsearch
    environment: environment.Environment
    indexd: indexd.IndexD
    spark: spark.Spark


CONFIG_SCHEMA: marshmallow.Schema = Configuration.Schema()
