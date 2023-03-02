import dataclasses


@dataclasses.dataclass(frozen=True)
class DNA:
    civic_var_id: str = "4"
    civic_gene_id: str = "2"
    source: str = "gDNA"
    chromosome: str = "chr14"
    start_position: str = "104780214"
    reference_allele: str = "C"
    alternative_allele: str = "T"
