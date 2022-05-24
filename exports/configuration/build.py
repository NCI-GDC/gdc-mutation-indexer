import dataclasses
from typing import Mapping, Sequence

from marshmallow import fields

from exports.configuration import marshmallow_extensions


@dataclasses.dataclass(frozen=True)
class Build:
    study_label: str
    data_release: str
    build_version: str
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

    def _get_index_template(self) -> str:
        if self.study_label:
            return f"{self.data_release}_viz_closed_{self.build_version}__{{}}__{self.study_label}__controlled"

        return f"{self.data_release}_viz_open_{self.build_version}__{{}}"

    @property
    def indices(self) -> Mapping[str, str]:
        template = self._get_index_template()

        return {
            index_type: template.format(index_type) for index_type in self.index_types
        }
