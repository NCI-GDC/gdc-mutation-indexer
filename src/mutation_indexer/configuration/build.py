"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""

import dataclasses
import pathlib
import uuid
from typing import Any, Sequence

import marshmallow_enum
from marshmallow import exceptions, fields, validate

from mutation_indexer.configuration import marshmallow_extensions
from mutation_indexer.constants import build

_VIZ_INDICES = frozenset(
    (
        build.IndexType.CASE_CENTRIC,
        build.IndexType.CNV_CENTRIC,
        build.IndexType.CNV_OCCURRENCE_CENTRIC,
        build.IndexType.GENE_CENTRIC,
        build.IndexType.SSM_CENTRIC,
        build.IndexType.SSM_OCCURRENCE_CENTRIC,
    )
)
_GENE_EXPRESSION_INDICES = frozenset((build.IndexType.GENE_EXPRESSION,))


class IndexTypesValidator(validate.Validator):
    def __call__(self, value: Any) -> Any:
        types = frozenset(value)

        if (types <= _GENE_EXPRESSION_INDICES) ^ (types <= _VIZ_INDICES):
            return value

        raise exceptions.ValidationError(
            "Can only build indices exclusively for Gene Expression or Viz."
        )


@dataclasses.dataclass(frozen=True)
class Build:
    """
    Configuration values for the entire build being process by mutation indexer.
    """

    build_version: str
    config_file: str
    data_release: str
    driver: marshmallow_extensions.ResolvedPath
    error_log: marshmallow_extensions.ResolvedPath
    index_types: Sequence[build.IndexType] = dataclasses.field(
        metadata={
            "metadata": {
                "marshmallow_field": marshmallow_extensions.ArbitraryLengthTuple(
                    marshmallow_enum.EnumField(build.IndexType)
                ),
            },
            "validate": IndexTypesValidator(),
        }
    )
    jar_dir: marshmallow_extensions.ResolvedPath
    manifest_dir: marshmallow_extensions.ResolvedPath
    output_log: marshmallow_extensions.ResolvedPath
    pex_file: marshmallow_extensions.ResolvedPath
    projects: Sequence[str] = dataclasses.field(
        metadata={
            "metadata": {
                "marshmallow_field": marshmallow_extensions.ArbitraryLengthTuple(
                    fields.String()
                )
            },
        }
    )
    spark_submit: marshmallow_extensions.ResolvedPath
    study_label: str
    build_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)
    acl: Sequence[str] = dataclasses.field(
        metadata={
            "metadata": {
                "marshmallow_field": marshmallow_extensions.ArbitraryLengthTuple(
                    fields.String()
                )
            },
        },
        default=("open",),
    )

    def is_viz_build(self) -> bool:
        return build.IndexType.GENE_EXPRESSION not in self.index_types
