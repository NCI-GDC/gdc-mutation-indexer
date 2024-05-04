"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""

import itertools
import types
from typing import Any, Iterable, Optional

import dataclasses
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


@dataclasses.dataclass(frozen=True)
class DataReleaseAndBuildVersion:
    data_release: Optional[str]
    build_version: Optional[str]

    def __bool__(self) -> bool:
        return self.build_version is not None and self.data_release is not None


def _get_data_release_and_build_version(build: dict) -> Optional[dict]:
    return DataReleaseAndBuildVersion(
        build.get("data_release"), build.get("build_version")
    )


def _get_index_template(build: dict) -> Optional[str]:
    build_config: DataReleaseAndBuildVersion = _get_data_release_and_build_version(
        build
    )
    if not build_config:
        return None

    if build.get("study_label"):
        study_label = build["study_label"]

        return f"{build_config.data_release}_viz_closed_{build_config.build_version}__{{}}__{study_label}__controlled"

    return f"{build_config.data_release}_viz_open_{build_config.build_version}__{{}}"


def _get_builders_from_data(data: dict) -> Iterable[dict]:
    builders: Iterable[dict] = itertools.chain(
        data.get("builders", _DEFAULT_DICT).get("viz", _DEFAULT_DICT).values(),
        data.get("builders", _DEFAULT_DICT)
        .get("gene_expression", _DEFAULT_DICT)
        .values(),
    )
    return builders


@marshmallow_dataclass.dataclass(frozen=True)
class Configuration:
    aws: aws.AWS
    build: build.Build
    builders: builders.Builders
    elasticsearch: elasticsearch.Elasticsearch
    environment: environment.Environment
    indexd: indexd.IndexD
    spark: spark.Spark

    # NOTE: Will add another "patcher" to patch the backup output path to the gen'ed configured output path.
    @marshmallow.pre_load
    def _add_projects_to_builders(self, data: dict, **kwargs: Any) -> dict:
        """
        This method insures that all builders' projects properties are defaulted to
        that of the main build prior to the marshmallow load process.
        """
        builders = _get_builders_from_data(data)
        projects = tuple(data.get("build", _DEFAULT_DICT).get("projects", ()))
        acl = tuple(data.get("build", _DEFAULT_DICT).get("acl", _DEFAULT_ACL))

        for builder in builders:
            builder.setdefault("projects", projects)
            builder.setdefault("acl", acl)

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

    @marshmallow.pre_load
    def _update_backup_path(self, data: dict, **kwargs: Any) -> dict:
        """
        This method populates build_version and data_release (if present) in the
        backup path configuration prior to the marshmallow load process.
        """
        version = _get_data_release_and_build_version(data.get("build", _DEFAULT_DICT))
        if version:
            builders = _get_builders_from_data(data)

            for builder in builders:
                backup = builder.get("backup", _DEFAULT_DICT)
                if "path" in backup:
                    path = backup["path"]
                    backup["path"] = path.format(**dataclasses.asdict(version))

        return data


CONFIG_SCHEMA: marshmallow.Schema = Configuration.Schema(unknown="exclude")
OBFUSCATED_CONFIG_SCHEMA: marshmallow.Schema = Configuration.Schema(
    context={"is_obfuscated": True}
)
