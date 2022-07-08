import dataclasses
import uuid
from typing import Sequence

import marshmallow_enum
from marshmallow import fields

from exports.configuration import marshmallow_extensions
from exports.constants import build


@dataclasses.dataclass(frozen=True)
class Build:
    study_label: str
    data_release: str
    build_version: str
    index_types: Sequence[build.IndexType] = dataclasses.field(
        metadata={
            "marshmallow_field": marshmallow_extensions.ArbitraryLengthTuple(
                marshmallow_enum.EnumField(build.IndexType)
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
    manifest_dir: str
    config_file: str
    build_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)
