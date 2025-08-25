import dataclasses


@dataclasses.dataclass(frozen=True)
class Protein:
    civic_gene_id: str | None = None
    civic_variant_id: str | None = None
    hgvsp_short: str | None = None
    name: str | None = "Baylor College of Medicine"
