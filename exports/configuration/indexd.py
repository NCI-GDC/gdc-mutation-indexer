import dataclasses


@dataclasses.dataclass(frozen=True)
class IndexD:
    host: str
    port: int
    user: str
    password: str
