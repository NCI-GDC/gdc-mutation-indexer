from mutation_indexer import client
from mutation_indexer.gene_expression import configuration


class Client(client.Client):
    def __init__(self) -> None:
        super().__init__(
            configuration.SCHEMA,
            configuration.OBFUSCATED_SCHEMA,
            "exports.gene_expression",
        )
