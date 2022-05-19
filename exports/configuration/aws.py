import dataclasses


@dataclasses.dataclass(frozen=True)
class S3:
    host: str
    access_key: str
    secret_key: str


@dataclasses.dataclass(frozen=True)
class AWS:
    s3: S3
