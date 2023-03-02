import dataclasses


@dataclasses.dataclass(frozen=True)
class PROT:
    hgvsp_short: str = "p.w288Cfs*"
    name: str = "NPM1"
    prot_civic_gene_id: str = "13"
    prot_civic_var_id: str = "67"
