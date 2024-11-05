import dataclasses


@dataclasses.dataclass(frozen=True)
class SQLiteDatabase:
    @dataclasses.dataclass(frozen=True)
    class Destination:
        bucket: str
        key: str

    batch_size: int
    destination: Destination


@dataclasses.dataclass(frozen=True)
class Databases:
    gene_expression: SQLiteDatabase
