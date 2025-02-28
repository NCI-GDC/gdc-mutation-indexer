import io
import types
from collections.abc import Iterable, Iterator
from typing import Any, TypedDict, Union

import inflect
import more_itertools
import yaml

PYTHON_TYPES = types.MappingProxyType(
    {
        "string": "str",
        "byte": "int",
        "short": "int",
        "integer": "int",
        "long": "int",
        "float": "float",
        "double": "float",
        "decimal": "decimal.decimal",
        "boolean": "bool",
        "datetime": "datetime.datetime",
    }
)
TAB = "    "
MODULE_HEADER = """import dataclasses
from typing import Optional

from pyspark import sql\n\n\n"""

CLASS_TEMPLATE = "{indent}@dataclasses.dataclass(frozen=True)\n{indent}class {name}:\n"
FIELD_TEMPLATE = "{indent}{name}: Optional[{type}] = {value}\n"
ARRAY_FIELD_TEMPLATE = "{indent}{name}: Optional[tuple[{type}, ...]] = {value}\n"

ASSERT_ROW_DEF = "\n{indent}def assert_equals(self, row: sql.Row) -> bool:\n"

ASSERT_ROW = "{indent}assert row\n"
ASSERT_STATEMENT_ATOMIC = "{indent}assert row.{prop} == self.{prop}\n"
ASSERT_STATEMENT_STRUCT = """{indent}assert (
{indent}    (row.{prop} is None and self.{prop} is None)
{indent}    or (self.{prop} and self.{prop}.assert_equals(row.{prop}))
{indent})
"""
ASSERT_STATEMENT_ARRAY_ATOMIC = "{indent}assert tuple(row.{prop}) == self.{prop}\n"
ASSERT_STATEMENT_ARRAY_STRUCT = "{indent}assert all(e.assert_equals(r) for r, e in zip(row.{prop} or (), self.{prop} or ()))\n"
ASSERT_RETURN = "\n{indent}return True\n"


class Field(TypedDict):
    name: str
    type: Union[str, "Struct", "Array"]


class Struct(TypedDict):
    fields: Iterable[Field]
    type: str


class Array(TypedDict):
    elementType: str | Struct
    containsNull: bool
    type: str


class AssertTypes(TypedDict):
    basic_fields: list[str]
    basic_array_fields: list[str]
    struct_fields: list[str]
    struct_array_fields: list[str]


class ClassGenerator:
    __slots__ = ("_defaults", "_include_asserts", "_inflection")

    def __init__(
        self,
        defaults: dict[str, Any],
        include_asserts: bool,
        inflection: inflect.engine | None = None,
    ) -> None:
        self._defaults = defaults
        self._include_asserts = include_asserts
        self._inflection = inflection or inflect.engine()

    def _get_python_class_name(self, field_name: str) -> str:
        name_parts = field_name.split("_")
        singular_noun = self._inflection.singular_noun(name_parts[-1]) or name_parts[-1]

        assert singular_noun is not True

        return "".join(
            p.capitalize()
            for p in more_itertools.value_chain(name_parts[:-1], singular_noun)
        )

    def _get_array_field(
        self, name: str, spark_type: Array, indents: int
    ) -> tuple[str, Iterator[str] | None]:
        element_type = spark_type["elementType"]
        default = self._defaults.get(name)
        model = None

        if isinstance(element_type, str):
            python_type = PYTHON_TYPES[element_type]

            if isinstance(default, str):
                default = (default,)
            elif isinstance(default, Iterable):
                default = tuple(default)
        else:
            python_type = self._get_python_class_name(name)
            default = f"({python_type}(),)"
            model = self.create_model(python_type, element_type, indents)

        return (
            ARRAY_FIELD_TEMPLATE.format(
                indent=TAB * indents, name=name, type=python_type, value=default
            ),
            model,
        )

    def _get_field(self, name: str, spark_type: str, indents: int) -> str:
        python_type = PYTHON_TYPES[spark_type]
        default = self._defaults.get(name)

        if isinstance(default, str) or python_type == "str":
            default = f'"{default}"' if default else None
        elif isinstance(default, Iterable):
            default = more_itertools.first(default, None)
        elif python_type == "float" and isinstance(default, int):
            default = default * 1.0
        elif python_type == "str" and isinstance(default, bool):
            default = f'"{default}"'.lower()

        return FIELD_TEMPLATE.format(
            indent=TAB * indents, name=name, type=python_type, value=default
        )

    def _get_struct_field(
        self, name: str, spark_type: Struct, indents: int
    ) -> tuple[str, Iterator[str]]:
        python_type = self._get_python_class_name(name)
        default = f"{python_type}()"

        return FIELD_TEMPLATE.format(
            indent=TAB * indents, name=name, type=python_type, value=default
        ), self.create_model(python_type, spark_type, indents)

    def _create_assert(self, asserts: AssertTypes, indents: int) -> Iterator[str]:
        yield ASSERT_ROW_DEF.format(indent=TAB * indents)

        indents += 1
        indent = TAB * indents

        yield ASSERT_ROW.format(indent=indent)

        for field in asserts["basic_fields"]:
            yield ASSERT_STATEMENT_ATOMIC.format(indent=indent, prop=field)

        for field in asserts["basic_array_fields"]:
            yield ASSERT_STATEMENT_ARRAY_ATOMIC.format(indent=indent, prop=field)

        for field in asserts["struct_fields"]:
            yield ASSERT_STATEMENT_STRUCT.format(indent=indent, prop=field)

        for field in asserts["struct_array_fields"]:
            yield ASSERT_STATEMENT_ARRAY_STRUCT.format(indent=indent, prop=field)

        yield ASSERT_RETURN.format(indent=indent)

    def _get_body(
        self,
        fields: Iterable[Field],
        indents: int,
    ) -> Iterator[str]:
        lines = []
        asserts = AssertTypes(
            basic_fields=[],
            basic_array_fields=[],
            struct_fields=[],
            struct_array_fields=[],
        )

        for field in fields:
            name = field["name"]
            spark_type = field["type"]
            model = None
            cls_field = None

            if isinstance(spark_type, str):
                cls_field = self._get_field(name, spark_type, indents)

                asserts["basic_fields"].append(name)
            elif "fields" not in spark_type:
                cls_field, model = self._get_array_field(name, spark_type, indents)
                assert_type = "struct_array_fields" if model else "basic_array_fields"

                asserts[assert_type].append(name)
            elif "elementType" not in spark_type:
                cls_field, model = self._get_struct_field(name, spark_type, indents)

                asserts["struct_fields"].append(name)

            if cls_field:
                lines.append(cls_field)

            if model is not None:
                yield from model

        yield from lines

        if self._include_asserts:
            yield from self._create_assert(asserts, indents)

    def create_model(
        self,
        name: str,
        schema: Struct,
        indents: int = 0,
    ) -> Iterator[str]:
        yield CLASS_TEMPLATE.format(indent=TAB * indents, name=name)
        yield from self._get_body(schema["fields"], indents + 1)
        yield "\n"


def create_model(
    name: str,
    input_schema: io.BytesIO,
    default_values: io.BytesIO,
    output_file: io.TextIOBase,
    include_asserts: bool,
) -> None:
    schema = yaml.safe_load(input_schema)
    defaults = yaml.safe_load(default_values)
    generator = ClassGenerator(defaults, include_asserts)
    lines = generator.create_model(name, schema)

    output_file.write(MODULE_HEADER)
    output_file.writelines(lines)
