"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""

import dataclasses
import pathlib
import uuid
from collections.abc import Sequence
from typing import Annotated, Any

from marshmallow import exceptions, fields, validate

from mutation_indexer.configuration import _extensions
from mutation_indexer.constants import build

_VIZ_INDICES = frozenset(
    (
        build.IndexType.CASE_CENTRIC,
        build.IndexType.CNV_CENTRIC,
        build.IndexType.CNV_OCCURRENCE_CENTRIC,
        build.IndexType.GENE_CENTRIC,
        build.IndexType.SEGMENT_CNV_CENTRIC,
        build.IndexType.SEGMENT_CNV_OCCURRENCE_CENTRIC,
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
    driver: Annotated[pathlib.Path, _extensions.ResolvedPathField]
    error_log: Annotated[pathlib.Path, _extensions.ResolvedPathField]
    index_types: Annotated[
        Sequence[build.IndexType],
        _extensions.ArrayTupleField(
            fields.Enum(build.IndexType), validate=IndexTypesValidator()
        ),
    ]
    jar_dir: Annotated[pathlib.Path, _extensions.ResolvedPathField]
    manifest_dir: Annotated[pathlib.Path, _extensions.ResolvedPathField]
    output_log: Annotated[pathlib.Path, _extensions.ResolvedPathField]
    projects: Annotated[Sequence[str], _extensions.ArrayTupleField(fields.String)]
    spark_submit: Annotated[pathlib.Path, _extensions.ResolvedPathField]
    study_label: str
    build_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)
    acl: Annotated[Sequence[str], _extensions.ArrayTupleField(fields.String)] = ("open",)
