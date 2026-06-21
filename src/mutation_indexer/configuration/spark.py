"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration

For further docuentation on spark configuration please refer to:
https://spark.apache.org/docs/2.4.5/configuration.html
"""

import dataclasses
import types
from collections.abc import Iterable, Mapping
from typing import Any


def _to_camel_case(value: str) -> str:
    parts = value.split("_")

    return f"{parts[0].lower()}{''.join(part.title() for part in parts[1:])}"


@dataclasses.dataclass(frozen=True)
class ConfigArgumentMixin:
    def _format_field(self, field: str) -> str:
        return _to_camel_case(field)

    def _get_field_argument(self, path: str, field: str, value: Any) -> tuple[str, str]:
        if isinstance(value, str) and " " in value:
            return ("--conf", f'"{path}{field}=\\"{value}\\""')

        return ("--conf", f"{path}{field}={value}")

    def _get_arguments(self, path: str = "") -> Iterable[tuple[str, str]]:
        fields = ((field, getattr(self, field)) for field in self.__dataclass_fields__.keys())

        for field, value in fields:
            field = self._format_field(field)

            if isinstance(value, ConfigArgumentMixin):
                yield from value._get_arguments(f"{path}{field}.")

            elif isinstance(value, Mapping):
                subpath = f"{path}{field}."

                for key, subvalue in value.items():
                    yield self._get_field_argument(subpath, key, subvalue)

            else:
                yield self._get_field_argument(path, field, value)


@dataclasses.dataclass(frozen=True)
class Driver(ConfigArgumentMixin):
    max_result_size: str
    memory: str


@dataclasses.dataclass(frozen=True)
class Shuffle(ConfigArgumentMixin):
    partitions: int


@dataclasses.dataclass(frozen=True)
class PythonDriver(ConfigArgumentMixin):
    python: str


@dataclasses.dataclass(frozen=True)
class Pyspark(ConfigArgumentMixin):
    python: str
    driver: PythonDriver


@dataclasses.dataclass(frozen=True)
class SQL(ConfigArgumentMixin):
    auto_broadcast_join_threshold: int
    case_sensitive: bool
    shuffle: Shuffle


@dataclasses.dataclass(frozen=True)
class Executor(ConfigArgumentMixin):
    memory: str
    cores: int
    instances: int


@dataclasses.dataclass(frozen=True)
class App(ConfigArgumentMixin):
    name: str


@dataclasses.dataclass(frozen=True)
class Submit(ConfigArgumentMixin):
    deploy_mode: str


@dataclasses.dataclass(frozen=True)
class Yarn(ConfigArgumentMixin):
    app_master_env: Mapping[str, str] = types.MappingProxyType({})


@dataclasses.dataclass(frozen=True)
class Spark(ConfigArgumentMixin):
    """
    Config values for the spark-submit/spark session.
    """

    master: str
    app: App
    driver: Driver
    executor: Executor
    pyspark: Pyspark
    sql: SQL
    submit: Submit
    yarn: Yarn
    executor_env: Mapping[str, str] = types.MappingProxyType({})

    def get_arguments(self) -> Iterable[tuple[str, str]]:
        """
        Converts the values in this object to a series of cli arguments (name, value)
        which should be included with the `spark-submit` command.

        Returns:
            An iterable of tuple pairs where each tuple is a flag and its value for the
            spark-submit command.
        """
        return self._get_arguments("spark.")
