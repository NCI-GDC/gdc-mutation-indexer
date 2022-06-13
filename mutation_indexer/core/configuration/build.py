import dataclasses
import uuid
from os import path
from typing import Any, Dict, FrozenSet, Mapping, Sequence

import marshmallow
from marshmallow import fields

from mutation_indexer.core.configuration import marshmallow_extensions
from mutation_indexer.core.constants import master

VALID_INDEX_TYPES: Mapping[master.Driver, FrozenSet[str]] = {
    master.Driver.VIZ: frozenset(
        (
            "case_centric",
            "cnv_centric",
            "cnv_occurrence_centric",
            "gene_centric",
            "ssm_centric",
            "ssm_occurrence_centric",
        )
    ),
    master.Driver.GENE_EXPRESSION: frozenset(("gene_expression",)),
}


@dataclasses.dataclass(frozen=True)
class Build:
    study_label: str
    data_release: str
    build_version: str
    driver: master.Driver
    index_types: Sequence[str] = dataclasses.field(
        metadata={
            "marshmallow_field": marshmallow_extensions.ArbitraryLengthTuple(
                fields.String()
            )
        }
    )
    projects: Sequence[str] = dataclasses.field(
        metadata={
            "marshmallow_field": marshmallow_extensions.ArbitraryLengthTuple(
                fields.String()
            )
        }
    )
    jar_dir: str
    config_dir: str
    build_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)

    def _get_index_template(self) -> str:
        if self.study_label:
            return f"{self.data_release}_viz_closed_{self.build_version}__{{}}__{self.study_label}__controlled"

        return f"{self.data_release}_viz_open_{self.build_version}__{{}}"

    @marshmallow.validates_schema
    def validate_index_types(self, data: Dict[str, Any], **kwargs) -> None:
        driver = data["driver"]
        index_types = data["index_types"]
        valid_index_types = VALID_INDEX_TYPES[driver]

        if not valid_index_types.issuperset(index_types):
            invalid_index_types = frozenset(index_types).difference(valid_index_types)

            raise marshmallow.ValidationError(
                f"The following are not valid index types: {', '.join(invalid_index_types)}."
            )

    @property
    def indices(self) -> Mapping[str, str]:
        template = self._get_index_template()

        return {
            index_type: template.format(index_type) for index_type in self.index_types
        }

    @property
    def config_file(self) -> str:
        return path.join(self.config_dir, f"config-{self.build_id}.toml")
