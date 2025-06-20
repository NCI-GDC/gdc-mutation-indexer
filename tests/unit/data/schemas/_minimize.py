import contextlib
import pathlib
import re
from collections.abc import Iterable, Iterator
from ctypes import ArgumentError
from importlib import resources, util
from os import path


def _get_types(data: str) -> Iterable[str]:
    return frozenset(re.findall(r"type: (.*)", data))


def _init_type(data: str, type: str) -> str:
    return re.sub(
        rf"- (metadata: \{{\}}\n(\s+)name: .*\n\s+nullable: true\n\s+type: {type})",
        rf"- &{type}_field\n\2\1",
        data,
        count=1,
    )


def _minimize_type(data: str, type: str) -> str:
    return re.sub(
        rf"- metadata: \{{\}}\n(\s+name: .*)\n\s+nullable: true\n\s+type: {type}",
        rf"- <<: *{type}_field\n\1",
        data,
    )


def _minimize_file(path: pathlib.Path) -> None:
    with open(path) as f:
        data = "".join(f.readlines())

    types = _get_types(data)

    for type in types:
        data = _init_type(data, type)
        data = _minimize_type(data, type)

    with open(path, "w") as f:
        f.write(data)


@contextlib.contextmanager
def _get_file_paths(module_or_path: str) -> Iterator[Iterable[pathlib.Path]]:
    if path.isfile(module_or_path):
        yield (pathlib.Path(module_or_path),)
    elif util.find_spec(module_or_path):
        with resources.as_file(resources.files(module_or_path)) as root:
            yield root.glob("**/*.yaml")
    elif path.isdir(module_or_path):
        yield pathlib.Path(module_or_path).glob("**/*.yaml")
    else:
        raise ArgumentError(f"No valid module, file, or directory found at: {module_or_path}.")


def minimize_files(schemas: str) -> None:
    with _get_file_paths(schemas) as paths:
        for path in paths:
            _minimize_file(path)
