import dataclasses

from exports.configuration import aws, build, builders, elasticsearch, indexd, spark


@dataclasses.dataclass(frozen=True)
class Configuration:
    spark_arguments: spark.Arguments
    spark: spark.Spark
    build: build.Build
    builders: builders.Builders
    aws: aws.AWS
    indexd: indexd.IndexD
    elasticsearch: elasticsearch.Elasticsearch
