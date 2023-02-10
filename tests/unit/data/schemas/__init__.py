import dataclasses
import re
import importlib_resources as resources
from typing import Type

import yaml
from pyspark.sql import types

PYTHON_PASCAL_CASE = re.compile("((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))")


@dataclasses.dataclass(frozen=True)
class Schema:
    _package: str
    _resource: str

    def load(self) -> types.StructType:
        data = resources.read_text(self._package, self._resource)

        return types.StructType.fromJson(yaml.safe_load(data))


def _get_schama(cls: Type, schema: str) -> Schema:
    package = ".".join(
        PYTHON_PASCAL_CASE.sub(r"_\1", p).lower() for p in cls.__qualname__.split(".")
    )
    package = f"{__name__}.{package}"
    resource = f"{schema.lower()}.yaml"

    return Schema(package, resource)


def _init_schemas(cls: Type) -> Type:
    annoations = getattr(cls, "__annotations__", {})
    schemas = {
        s: _get_schama(cls, s) for s, t in annoations.items() if issubclass(t, Schema)
    }

    for name, schema in schemas.items():
        setattr(cls, name, schema)

    return cls


class Builders:
    @_init_schemas
    class GeneModel:
        FINAL: Schema
        RAW: Schema


class Viz:
    class Builders:
        @_init_schemas
        class ASCAT:
            FINAL: Schema
            DOCUMENT: Schema
            FILE: Schema

        @_init_schemas
        class Case:
            FINAL: Schema
            RAW: Schema

        @_init_schemas
        class CaseCentric:
            FINAL: Schema
            CASE: Schema
            SAMPLE: Schema

        @_init_schemas
        class CIVICAnnotation:
            FINAL: Schema
            MAF: Schema

        @_init_schemas
        class Consequence:
            FINAL: Schema

            @_init_schemas
            class AAChange:
                FINAL: Schema

            @_init_schemas
            class Gene:
                FINAL: Schema

        @_init_schemas
        class MAF:
            FINAL: Schema
            AGGREGATED_SOMATIC_MUTATION: Schema
            MASKED_SOMATIC_MUTATION: Schema

        @_init_schemas
        class MAFMetadata:
            FINAL: Schema
            FILE: Schema

        class Observation:
            @_init_schemas
            class CNV:
                FINAL: Schema

            @_init_schemas
            class Other:
                FINAL: Schema

            @_init_schemas
            class SSM:
                FINAL: Schema

        @_init_schemas
        class PrimaryAliquot:
            FINAL: Schema
            FILE: Schema


class GeneExpression:
    class Builders:
        @_init_schemas
        class Case:
            FINAL: Schema

        @_init_schemas
        class GeneExpression:
            FINAL: Schema

        @_init_schemas
        class PrimaryAliquot:
            FINAL: Schema
            FILE: Schema

        @_init_schemas
        class Value:
            FINAL: Schema
            STAR_COUNTS: Schema
