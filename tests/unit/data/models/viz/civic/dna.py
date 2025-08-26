import dataclasses


@dataclasses.dataclass(frozen=True)
class DNA:
    chromosome: str | None = "chr17"
    civic_gene_id: str | None = None
    civic_variant_id: str | None = None
    reference_allele: str | None = None
    start_position: int | None = None
    tumor_allele: str | None = None
