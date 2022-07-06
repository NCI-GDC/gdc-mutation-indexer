import dataclasses
from typing import Mapping

from exports.configuration import marshmallow_extensions
from exports.constants import build


@dataclasses.dataclass(frozen=True)
class Connection:
    nodes: str
    user: str
    password: marshmallow_extensions.SecretString
    use_ssl: bool
    verify_certs: bool


@dataclasses.dataclass(frozen=True)
class Read:
    case_index: str
    file_index: str


@dataclasses.dataclass(frozen=True)
class Write:
    batch_size_bytes: str
    batch_size_entities: int
    indices: Mapping[build.IndexType, str]


@dataclasses.dataclass(frozen=True)
class Elasticsearch:
    connection: Connection
    read: Read
    write: Write
