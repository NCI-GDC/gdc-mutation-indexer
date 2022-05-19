import dataclasses


@dataclasses.dataclass(frozen=True)
class Elasticsearch:
    case_index: str
    file_index: str
    nodes: str
    user: str
    password: str
    use_ssl: bool
    verify_certs: bool
