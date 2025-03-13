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
import tempfile
import types
from collections.abc import Mapping, Sequence
from importlib import resources
from typing import IO, Any, ClassVar, Generic, Iterable, Iterator, TypeVar, cast

import marshmallow
import marshmallow_dataclass
import toml
from deepmerge import merger
from importlib_resources import abc
from typing_extensions import Self, dataclass_transform

from mutation_indexer.configuration import (
    aws,
    build,
    builders,
    elasticsearch,
    indexd,
    spark,
)
from mutation_indexer.constants import app

T = TypeVar("T")

_DEFAULT_DICT = {}
_DEFAULT_ACL = ("open",)


class _Schema(Generic[T]):
    def __init__(self, cls: type) -> None:
        """A wrapper around a marshmallow schema allowing type hinting & custom logic.

        Args:
            cls: The dataclass which will the schema will be based on.
        """
        schema_cls = marshmallow_dataclass.class_schema(cls)

        self._schema = schema_cls(many=False, unknown=marshmallow.EXCLUDE)

    def load(self, data: Mapping[str, Any]) -> T:
        """Loads the data contained in the mapping into the object T.

        Args:
            data: The data which needs to be used to populate the object from which the
                schema was created from.

        """
        return cast(T, self._schema.load(data))

    def dump(self, obj: T, is_obfuscated: bool = True) -> Mapping[str, Any]:
        """Dumps the data in the object into a mapping.

        This method by default will obfuscate any data which is marked as being a secret
        string.

        NOTE: Currently we are manually switching on/off the is_obfuscated context. This
        is more in line with the changes coming down the line from marshmallow. It also
        allows us to maintain a single backing marshmallow schema instance. For more see
        this link:
        https://marshmallow.readthedocs.io/en/latest/upgrading.html#new-context-api

        Args:
            obj: The object to be serialized.
            is_obfuscated: Indicates that the data marked as secret strings should be
                obfuscated when its serialized. Default is true.

        Returns:
            A mapping object with the data converted to appropriate serialized values.
        """
        if is_obfuscated:
            self._schema.context["is_obfuscated"] = True

        try:
            return cast(Mapping[str, Any], self._schema.dump(obj))
        finally:
            self._schema.context.pop("is_obfuscated", None)


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


@dataclass_transform(frozen_default=True)
class _Configuration:
    """A base configuration controlling the serialization of the data within."""

    _schema: ClassVar[_Schema[Self]]
    """The schema to be used to serialize/deserialize this object."""
    _merger: ClassVar[merger.Merger] = merger.Merger(
        type_strategies=((dict, ["merge"]), (list, ["override"]), (set, ["override"])),
        fallback_strategies=["override"],
        type_conflict_strategies=["override"],
    )
    """A merger for merging partial configurations together into a single value."""

    build: build.Build

    def __init_subclass__(cls) -> None:
        """Insures that the schema is generated for any subclass."""
        cls = dataclasses.dataclass(frozen=True)(cls)
        cls._schema = _Schema(cls)

    @classmethod
    def _default_configs(cls) -> Sequence[abc.Traversable]:
        """Gets all configuration resources associated with the configuration.

        Returns:
            An ordered sequence of configuration files. Any values in the later files
            take precedence over those in earlier files i.e. "last in wins."
        """
        return (resources.files(app.ROOT_MODULE) / app.CONFIGURATION_FILE,)

    @classmethod
    def load(cls, config: abc.Traversable) -> Self:
        return cls._schema.load(toml.loads(config.read_text() or ""))

    @classmethod
    @contextlib.contextmanager
    def initialize(cls, configs: Iterable[abc.Traversable]) -> Iterator[Self]:
        """Loads the config data from the files supplemented with any cls defaults.

        NOTE: This method works in a "last in wins" model. Thus any value from a
        earlier file in configs that appears in a later file will be overridden by the
        later value. All default values associated with the class are loaded before any
        user supplied value other than `build.config_file` which is controlled by this
        method.

        NOTE: As part of loading the data this method writes a manifest to a configured
        directory. See `_write_manifest.

        Args:
            configs: A iterable of config files which should be used to load the
                resulting configuration object.

        Returns:
            A context manager containing the final configuration object. This data has
            been written to a temporary file at `build.config_file` which will be
            cleaned up once the context manager is exited.
        """

        def load_toml(file: abc.Traversable) -> Mapping[str, Any]:
            return toml.loads(file.read_text() or "")

        unmerged_data = map(load_toml, itertools.chain(cls._default_configs(), configs))

        with tempfile.NamedTemporaryFile("wt+") as f:
            data = functools.reduce(
                cls._merger.merge, unmerged_data, {"build": {"config_file": f.name}}
            )
            config = cls._schema.load(data)

            config.dump(f, is_obfuscated=False)
            config._write_manifest()

            yield config

    def dump(self, buffer: IO[str], is_obfuscated: bool = True) -> None:
        """Dumps the data contained in this instance to the given buffer.

        Args:
            buffer: The text io buffer to which the data will be written.
            is_obfuscated: True if any secret strings should be obfuscated during this
                serialization process.
        """
        toml.dump(self._schema.dump(self, is_obfuscated), buffer)

    def _write_manifest(self) -> None:
        """Writes a manifest file to the configured directory: `build.manifest_dir`."""
        build = self.build
        toml_name = f"{datetime.datetime.now().isoformat()}-{build.build_id}.toml"
        file_name = build.manifest_dir / toml_name

        build.manifest_dir.mkdir(parents=True, exist_ok=True)

        with open(file_name, "w+") as f:
            self.dump(f, is_obfuscated=True)


class Configuration(_Configuration):
    aws: aws.AWS
    build: build.Build
    builders: builders.Builders
    elasticsearch: elasticsearch.Elasticsearch
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
