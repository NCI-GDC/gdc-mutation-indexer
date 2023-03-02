import dataclasses


@dataclasses.dataclass(frozen=True)
class DNA:
    chromosome: str = "chr14"
    dna_civic_var_id: str = "4"
    dna_civic_gene_id: str = "2"
    reference_allele: str = "C"
    start_position: int = 104_780_214
    tumor_allele: str = "T"
