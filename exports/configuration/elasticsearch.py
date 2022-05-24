import dataclasses


@dataclasses.dataclass(frozen=True)
class Connection:
    nodes: str
    user: str
    password: str
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
    repartition_size: int
    coalese_size: int


@dataclasses.dataclass(frozen=True)
class Elasticsearch:
    connection: Connection
    read: Read
    write: Write
