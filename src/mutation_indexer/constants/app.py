import types

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s:%(lineno)d] %(message)s"
DRIVERS = types.MappingProxyType(
    {
        "gene-expression": "mutation_indexer.gene_expression",
        "viz": "mutation_indexer.viz",
    }
)
