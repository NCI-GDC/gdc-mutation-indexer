import marshmallow
import marshmallow_dataclass

from exports.configuration import aws, build, builders, elasticsearch, indexd, spark


@marshmallow_dataclass.dataclass(frozen=True)
class Configuration:
    spark_arguments: spark.Arguments
    spark: spark.Spark
    build: build.Build
    builders: builders.Builders
    aws: aws.AWS
    indexd: indexd.IndexD
    elasticsearch: elasticsearch.Elasticsearch


CONGIF_SCHEMA: marshmallow.Schema = Configuration.Schema()
