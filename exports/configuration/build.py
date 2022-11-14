"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""
import dataclasses
import uuid
from typing import Any, Iterable, Sequence

import marshmallow_enum
from marshmallow import fields, validate

from exports.configuration import marshmallow_extensions
from exports.constants import build

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

        if types <= _GENE_EXPRESSION_INDICES or types <= _VIZ_INDICES:
            return value

        raise validate.ValidationError(
            "Can only build indices exclusively for Gene Expresion or Viz."
        )


@dataclasses.dataclass(frozen=True)
class Build:
    """
    Configuration values for the entire build being process by mutation indexer.
    """

    study_label: str
    data_release: str
    build_version: str
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
    projects: Sequence[str] = dataclasses.field(
        metadata={
            "metadata": {
                "marshmallow_field": marshmallow_extensions.ArbitraryLengthTuple(
                    fields.String()
                )
            },
        }
    )
    jar_dir: str
    manifest_dir: str
    config_file: str
    build_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)

    def is_viz_build(self) -> bool:
        return build.IndexType.GENE_EXPRESSION not in self.index_types
