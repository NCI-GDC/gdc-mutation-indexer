import itertools
from typing import Any
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

_DEFAULT_DICT = {}


@marshmallow_dataclass.dataclass(frozen=True)
class Configuration:
    aws: aws.AWS
    build: build.Build
    builders: builders.Builders
    elasticsearch: elasticsearch.Elasticsearch
    environment: environment.Environment
    indexd: indexd.IndexD
    spark: spark.Spark

    @marshmallow.pre_load
    def _add_projects_to_builders(self, data: dict, **kwargs: Any) -> dict:
        builders = itertools.chain(
            data.get("builders", _DEFAULT_DICT).get("viz", _DEFAULT_DICT).values(),
            data.get("builders", _DEFAULT_DICT)
            .get("gene_expression", _DEFAULT_DICT)
            .values(),
        )
        projects = tuple(data.get("build", _DEFAULT_DICT).get("projects", ()))

        for builder in builders:
            builder["projects"] = projects

        return data


CONFIG_SCHEMA: marshmallow.Schema = Configuration.Schema()
OBFUSCATED_CONFIG_SCHEMA: marshmallow.Schema = Configuration.Schema(
    context={"is_obfuscated": True}
)
