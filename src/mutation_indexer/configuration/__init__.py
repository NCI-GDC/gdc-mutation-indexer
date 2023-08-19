"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""
import dataclasses
import itertools
import types
from typing import Any, Generic, Iterable, Optional, TypeVar

import marshmallow
import marshmallow_dataclass

from mutation_indexer.configuration import (
    aws,
    build,
    builders,
    elasticsearch,
    environment,
    indexd,
    spark,
)

_DEFAULT_DICT = {}
TBuilders = TypeVar("TBuilders")


def _get_index_template(build: dict) -> Optional[str]:
    if "data_release" not in build or "build_version" not in build:
        return None

    data_release = build["data_release"]
    build_version = build["build_version"]

    if build.get("study_label"):
        study_label = build["study_label"]

        return f"{data_release}_viz_closed_{build_version}__{{}}__{study_label}__controlled"

    return f"{data_release}_viz_open_{build_version}__{{}}"


@dataclasses.dataclass(frozen=True)
class Configuration(Generic[TBuilders]):
    aws: aws.AWS
    build: build.Build
    elasticsearch: elasticsearch.Elasticsearch
    environment: environment.Environment
    indexd: indexd.IndexD
    spark: spark.Spark
    builders: TBuilders

    @marshmallow.pre_load
    def _add_projects_to_builders(self, data: dict, **kwargs: Any) -> dict:
        """
        This method insures that all builders' projects properties are defaulted to
        that of the main build prior to the marshmallow load process.
        """
        builders: Iterable[dict] = itertools.chain(
            data.get("builders", _DEFAULT_DICT).get("viz", _DEFAULT_DICT).values(),
            data.get("builders", _DEFAULT_DICT)
            .get("gene_expression", _DEFAULT_DICT)
            .values(),
        )
        projects = tuple(data.get("build", _DEFAULT_DICT).get("projects", ()))

        for builder in builders:
            builder.setdefault("projects", projects)

        return data

    @marshmallow.pre_load
    def _add_es_write_indices(self, data: dict, **kwargs: Any) -> dict:
        """
        This method populates the indices property of the elasticsearch write
        configuration prior to the marshmallow load process.
        """
        build = data.get("build", _DEFAULT_DICT)
        index_types = build.get("index_types", ())
        template = _get_index_template(build)
        es_write = data.get("elasticsearch", _DEFAULT_DICT).get("write", {})

        if template:
            es_write["indices"] = types.MappingProxyType(
                {
                    index_type: template.format(index_type.lower())
                    for index_type in index_types
                }
            )

        return data
