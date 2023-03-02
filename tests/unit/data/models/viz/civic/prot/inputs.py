import dataclasses


@dataclasses.dataclass(frozen=True)
class PROT:
    civic_var_id: str = "4"
    civic_gene_id: str = "2"
    hugo_symbol: str = "NPM1"
    gene: str = "ENSG00000181163.12"
    hgvsp: str = "p.w288Cfs*"
    source: str = "Protein"
