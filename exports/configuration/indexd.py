import dataclasses

from exports.configuration import marshmallow_extensions


@dataclasses.dataclass(frozen=True)
class IndexD:
    host: str
    port: int
    user: str
    password: marshmallow_extensions.SecretString
