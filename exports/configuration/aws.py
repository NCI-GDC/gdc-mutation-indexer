import dataclasses

from exports.configuration import marshmallow_extensions


@dataclasses.dataclass(frozen=True)
class S3:
    host: str
    access_key: str
    secret_key: marshmallow_extensions.SecretString


@dataclasses.dataclass(frozen=True)
class AWS:
    s3: S3
