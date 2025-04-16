"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""

from __future__ import annotations

import contextlib
import dataclasses
import datetime
import functools
import itertools
import pathlib
import tempfile
import types
from collections.abc import Iterator, Mapping
from importlib import abc, resources
from typing import Any, Iterable

import marshmallow
import tomli
import tomli_w
from typing_extensions import Self

from mutation_indexer.configuration import (
    _extensions,
    aws,
    build,
    builders,
    elasticsearch,
    environment,
    indexd,
    spark,
)
from mutation_indexer.constants import app

_DEFAULT_DICT = {}
_DEFAULT_ACL = ("open",)


def _load_data(source: abc.Traversable | Mapping[str, Any]) -> Mapping[str, Any]:
    """Loads the data in the given source file or returns the source mapping.

    Args:
        source: Either a source file or a source mapping.

    Returns:
        The data in the given file or the mapping.
    """
    if isinstance(source, Mapping):
        return source

    with source.open("rb") as fp:
        return tomli.load(fp)


@dataclasses.dataclass(frozen=True)
class DataReleaseAndBuildVersion:
    """Should be specified in configuration.toml."""

    data_release: str | None
    build_version: str | None

    def __bool__(self) -> bool:
        return self.build_version is not None and self.data_release is not None


def _get_data_release_and_build_version(build: dict) -> DataReleaseAndBuildVersion:
    return DataReleaseAndBuildVersion(
        build.get("data_release"), build.get("build_version")
    )


def _get_index_template(build: dict) -> str | None:
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


class Configuration(_extensions.SerializableDataclass):
    aws: aws.AWS
    build: build.Build
    builders: builders.Builders
    elasticsearch: elasticsearch.Elasticsearch
    environment: environment.Environment
    indexd: indexd.IndexD
    spark: spark.Spark

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

    # TODO: DEV-2690 When a Builder can have multiple exporters, we won't need to patch path.
    @marshmallow.pre_load
    def _update_backup_paths(self, data: dict, **kwargs: Any) -> dict:
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

    @classmethod
    def _default_files(cls) -> Iterable[abc.Traversable]:
        """Load all default files associated with this configuration.

        Returns:
            An iterable or the default files.
        """
        yield resources.files(app.ROOT_MODULE) / app.CONFIGURATION_FILE

    @classmethod
    def load(
        cls,
        source: abc.Traversable | Mapping[str, Any],
        *sources: abc.Traversable | Mapping[str, Any],
    ) -> Self:
        """Loads and instance of this class from the given sources.

        Args:
            source: An initial source which can be supplemented and/or overridden by any
                additional sources.
            sources: Any additional sources will supersede the initial or any proceeding
                source w/ the same configured data point.

        Returns:
            A validated instance of this class containing the data loaded from the given
            sources.
        """
        data = functools.reduce(cls._merger.merge, map(_load_data, (source, *sources)))

        return cls._schema.load(data)

    def dump(self, file: abc.Traversable, is_obfuscated: bool) -> None:
        """Dumps the contents of this instance into a file.

        Args:
            file: The path to the file to which the data should be written.
            is_obfuscated: A flag indicating if secret strings should be obfuscated when
                they are serialized.
        """
        with resources.as_file(file) as path, path.open("wb+") as fp:
            tomli_w.dump(self._schema.dump(self, is_obfuscated), fp)

    def _write_manifest(self) -> None:
        """Writes a copy of this configuration to the configured `build.manifest_dir`.

        NOTE: The manifest copy *is* obfuscated to help manage password security.
        """
        file_name = f"{datetime.datetime.now().isoformat()}-{self.build.build_id}.toml"

        self.build.manifest_dir.mkdir(parents=True, exist_ok=True)
        self.dump(self.build.manifest_dir / file_name, is_obfuscated=True)

    @classmethod
    @contextlib.contextmanager
    def client_context(cls, user_files: Iterable[abc.Traversable]) -> Iterator[Self]:
        """Creates an instance of the config from the user files w/ runtime values.

        NOTE: This method does several important things for the client
        * It loads all default configurations associated with the configuration and
          apply any overrides contained in the user files.
        * It merges all of the data in the user files. In the case of duplicate data
          points in any two files, the data point from the file which appears last in
          order will appear in the final data.
        * It ensures that both `spark.pyspark.python` & `spark.pyspark.driver.python`
          are configured to point the the alias in `app.PEX_FILE` which should be used
          when uploading `build.pex_file` in the spark submit command.
        * It writes the final representation of the configuration to a temporary file
          with will be found at the path in `build.config_file`. This file should be
          uploaded with spark submit for the driver to load.

        Args:
            user_files: The user configuration files whose data will be read and merged
                in a "last in wins" manor.

        Returns:
            A context manager wrapping the loaded data from the merged files which
            ensures that all runtime file representations of this configuration are
            existent for the duration of the context & deleted afterward.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = pathlib.Path(tmpdir, app.CONFIGURATION_FILE)
            # These values must be controlled by the client for spark submit to be able
            # to upload and run the driver.
            required_data = {
                "build": {"config_file": str(config_file)},
                "spark": {
                    "pyspark": {
                        "python": f"./{app.PEX_FILE}",
                        "driver": {"python": f"./{app.PEX_FILE}"},
                    }
                },
            }
            config = cls.load(*cls._default_files(), *user_files, required_data)

            config._write_manifest()
            config.dump(config_file, is_obfuscated=False)

            yield config
