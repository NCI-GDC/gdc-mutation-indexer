import dataclasses
import enum
from typing import Optional, Sequence

from marshmallow import fields

from mutation_indexer.core.configuration import marshmallow_extensions


class BackupMode(enum.Enum):
    READ = enum.auto()
    WRITE = enum.auto()
    NEITHER = enum.auto()


@dataclasses.dataclass(frozen=True)
class Backup:
    mode: BackupMode
    path: str


@dataclasses.dataclass(frozen=True)
class Builder:
    is_cached: bool
    backup: Backup


@dataclasses.dataclass(frozen=True)
class AscatBuilder(Builder):
    omit_cnv_data: bool


@dataclasses.dataclass(frozen=True)
class CaseBuilder(Builder):
    excluded_fields: Sequence[str] = dataclasses.field(
        metadata={
            "marshmallow_field": marshmallow_extensions.ArbitraryLengthTuple(
                fields.String()
            )
        }
    )
    included_fields: Sequence[str] = dataclasses.field(
        metadata={
            "marshmallow_field": marshmallow_extensions.ArbitraryLengthTuple(
                fields.String()
            )
        }
    )


@dataclasses.dataclass(frozen=True)
class GeneModelBuilder(Builder):
    census_file: str
    citobands_file: str
    gene_model_file: str


@dataclasses.dataclass(frozen=True)
class IndexBuilder(Builder):
    array_size_threshold: int


@dataclasses.dataclass(frozen=True)
class Viz:
    ascat: AscatBuilder
    case: CaseBuilder
    gene_model: GeneModelBuilder
    maf_metadata: Builder
    maf: Builder
    primary_aliquot: Builder
    case_centric: IndexBuilder
    gene_centric: IndexBuilder
    cnv_centric: IndexBuilder
    cnv_occurrence_centric: IndexBuilder
    ssm_centric: IndexBuilder
    ssm_occurrence_centric: IndexBuilder


@dataclasses.dataclass(frozen=True)
class GeneExpression:
    gene_model: GeneModelBuilder
    case: Builder
    value: Builder
    primary_aliquot: Builder
    gene_expression: IndexBuilder


@dataclasses.dataclass(frozen=True)
class Builders:
    viz: Optional[Viz]
    gene_expression: Optional[GeneExpression]
