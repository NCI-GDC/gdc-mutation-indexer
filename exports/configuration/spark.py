import dataclasses
from typing import Any, Iterable, Tuple

from exports.configuration import marshmallow_extensions


def _to_camel_case(value: str) -> str:
    parts = value.split("_")

    return f"{parts[0].lower()}{''.join(part.title() for part in parts[1:])}"


class ConfigArgumentMixin:
    def _format_field(self, field: str) -> str:
        return _to_camel_case(field)

    def _get_field_argument(self, path: str, field: str, value: Any) -> Tuple[str, str]:
        return ("--conf", f"{path}{field}={value}")

    def _get_arguments(self, path: str = "") -> Iterable[Tuple[str, str]]:
        fields = (
            (field, getattr(self, field)) for field in self.__dataclass_fields__.keys()  # type: ignore
        )

        for field, value in fields:
            field = self._format_field(field)

            if isinstance(value, ConfigArgumentMixin):
                yield from value._get_arguments(f"{path}{field}.")

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
class Env(ConfigArgumentMixin):
    pex_python: str
    pex_root: str

    def _format_field(self, field: str) -> str:
        return field.upper()


@dataclasses.dataclass(frozen=True)
class Yarn(ConfigArgumentMixin):
    app_master_env: Env
    executor_env: Env


@dataclasses.dataclass(frozen=True)
class Key(ConfigArgumentMixin):
    key: marshmallow_extensions.SecretString


@dataclasses.dataclass(frozen=True)
class S3A(ConfigArgumentMixin):
    access: Key
    secret: Key


@dataclasses.dataclass(frozen=True)
class FS(ConfigArgumentMixin):
    s3a: S3A


@dataclasses.dataclass(frozen=True)
class Hadoop(ConfigArgumentMixin):
    fs: FS


@dataclasses.dataclass(frozen=True)
class Spark(ConfigArgumentMixin):
    master: str
    app: App
    driver: Driver
    executor: Executor
    pyspark: Pyspark
    sql: SQL
    submit: Submit
    yarn: Yarn
    hadoop: Hadoop

    def get_arguments(self) -> Iterable[Tuple[str, str]]:
        """
        Converts the values in this object to a series of cli arguments (name, value)
        which should be included with the `spark-submit` command.
        """
        return self._get_arguments("spark.")
