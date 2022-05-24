import dataclasses
from typing import Any, Iterable, Tuple


def _to_camel_case(value: str) -> str:
    parts = value.split("_")

    return f"{parts[0].lower()}{''.join(part.title() for part in parts[1:])}"


class ArgumentMixin:
    def _get_field_argument(self, path: str, field: str, value: Any) -> Tuple[str, str]:
        return (f"--{path}{field}", f"{value}")

    def get_arguments(self, path: str = "") -> Iterable[Tuple[str, str]]:
        fields = (
            (field, getattr(self, field)) for field in self.__dataclass_fields__.keys()  # type: ignore
        )

        for field, value in fields:
            field = _to_camel_case(field)

            if isinstance(value, ArgumentMixin):
                yield from value.get_arguments(f"{path}{field}.")

            else:
                yield self._get_field_argument(path, field, value)


class ConfigArgumentMixin(ArgumentMixin):
    def _get_field_argument(self, path: str, field: str, value: Any) -> Tuple[str, str]:
        return ("--conf", f"{path}{field}={value}")


@dataclasses.dataclass(frozen=True)
class Arguments(ArgumentMixin):
    master: str
    name: str
    num_executors: int


@dataclasses.dataclass(frozen=True)
class Driver(ConfigArgumentMixin):
    auto_broadcast_join_threshold: int
    max_result_size: str
    memory: str


@dataclasses.dataclass(frozen=True)
class Shuffle(ConfigArgumentMixin):
    partitions: int


@dataclasses.dataclass(frozen=True)
class Pyspark:
    python: str


@dataclasses.dataclass(frozen=True)
class SQL(ConfigArgumentMixin):
    case_sensitive: bool
    shuffle: Shuffle
    python: str


@dataclasses.dataclass(frozen=True)
class Executor(ConfigArgumentMixin):
    cores: int
    memory: str


@dataclasses.dataclass(frozen=True)
class Spark(ConfigArgumentMixin):
    driver: Driver
    sql: SQL
    executor: Executor

    def get_arguments(self, path: str = "spark.") -> Iterable[Tuple[str, str]]:
        return super().get_arguments(path)
