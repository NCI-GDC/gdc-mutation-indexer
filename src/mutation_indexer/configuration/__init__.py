"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""
import itertools
import types
from typing import Any, ClassVar, Iterable, Optional

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
_DEFAULT_ACL = ("open",)


def _get_index_template(build_data: dict) -> Optional[str]:
    if "data_release" not in build_data or "build_version" not in build_data:
        return None

    data_release = build_data["data_release"]
    build_version = build_data["build_version"]

    if build_data.get("study_label"):
        study_label = build_data["study_label"]

        return (
            f"{data_release}_viz_closed_{build_version}__{{}}__{study_label}"
            "__controlled"
        )

    return f"{data_release}_viz_open_{build_version}__{{}}"


@marshmallow_dataclass.dataclass(frozen=True)
class Configuration:
    Schema: ClassVar[type[marshmallow.Schema]]  # pylint: disable=C0103

    aws: aws.AWS
    build: build.Build
    builders: builders.Builders
    elasticsearch: elasticsearch.Elasticsearch
    environment: environment.Environment
    indexd: indexd.IndexD
    spark: spark.Spark

    @marshmallow.pre_load
    def _add_projects_to_builders(self, data: dict, **_: Any) -> dict:
        """
        This method insures that all builders' projects properties are defaulted to
        that of the main build prior to the marshmallow load process.
        """
        builders_data: Iterable[dict] = itertools.chain(
            data.get("builders", _DEFAULT_DICT).get("viz", _DEFAULT_DICT).values(),
            data.get("builders", _DEFAULT_DICT)
            .get("gene_expression", _DEFAULT_DICT)
            .values(),
        )
        projects = tuple(data.get("build", _DEFAULT_DICT).get("projects", ()))
        acl = tuple(data.get("build", _DEFAULT_DICT).get("acl", _DEFAULT_ACL))

        for builder in builders_data:
            builder.setdefault("projects", projects)
            builder.setdefault("acl", acl)

        return data

    @marshmallow.pre_load
    def _add_es_write_indices(self, data: dict, **_: Any) -> dict:
        """
        This method populates the indices property of the elasticsearch write
        configuration prior to the marshmallow load process.
        """
        build_data = data.get("build", _DEFAULT_DICT)
        index_types = build_data.get("index_types", ())
        template = _get_index_template(build_data)
        es_write = data.get("elasticsearch", _DEFAULT_DICT).get("write", {})

        if template:
            es_write["indices"] = types.MappingProxyType(
                {
                    index_type: template.format(index_type.lower())
                    for index_type in index_types
                }
            )

        return data


CONFIG_SCHEMA: marshmallow.Schema = Configuration.Schema(unknown="exclude")
OBFUSCATED_CONFIG_SCHEMA: marshmallow.Schema = Configuration.Schema(
    context={"is_obfuscated": True}
)
