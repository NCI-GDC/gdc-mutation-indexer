import dataclasses
import re
from importlib import resources
from typing import Type

import yaml
from pyspark.sql import types

from tests.unit.data.schemas import _minimize

PYTHON_PASCAL_CASE = re.compile(r"((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))")


@dataclasses.dataclass(frozen=True)
class Schema:
    _package: str
    _resource: str

    def load(self) -> types.StructType:
        data = resources.read_text(self._package, self._resource)

        return types.StructType.fromJson(yaml.safe_load(data))

    def update(self, schema: types.StructType) -> None:
        with resources.as_file(
            resources.files(self._package).joinpath(self._resource)
        ) as path, open(path, "w") as f:
            yaml.dump(schema.jsonValue(), f)
        
        _minimize.minimize_files(self._package)


def _get_schema(cls: Type, schema: str) -> Schema:
    package = ".".join(
        PYTHON_PASCAL_CASE.sub(r"_\1", p).lower() for p in cls.__qualname__.split(".")
    )
    package = f"{__name__}.{package}"
    resource = f"{schema.lower()}.yaml"

    return Schema(package, resource)


def _init_schemas(cls: Type) -> Type:
    annotations = getattr(cls, "__annotations__", {})
    schemas = {s: _get_schema(cls, s) for s, t in annotations.items() if t is Schema}

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
            @_init_schemas
            class SSM:
                FINAL: Schema

                @_init_schemas
                class WithoutAAChange:
                    FINAL: Schema

                @_init_schemas
                class WithoutGene:
                    FINAL: Schema

            @_init_schemas
            class CNV:
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

        class DFBuilders:
            @_init_schemas
            class SSM:
                FINAL: Schema

                @_init_schemas
                class Occurrence:
                    FINAL: Schema

                @_init_schemas
                class Other:
                    FINAL: Schema

            @_init_schemas
            class CNV:
                FINAL: Schema

                @_init_schemas
                class Occurrence:
                    FINAL: Schema

                @_init_schemas
                class Other:
                    FINAL: Schema

        @_init_schemas
        class SSMCentric:
            FINAL: Schema

        @_init_schemas
        class SSMOccurrenceCentric:
            FINAL: Schema


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
