import dataclasses


@dataclasses.dataclass(frozen=True)
class CNV:
    @dataclasses.dataclass(frozen=True)
    class Consequence:
        @dataclasses.dataclass(frozen=True)
        class Gene:
            biotype: str | None = "transcribed_unprocessed_pseudogene"
            gene_id: str | None = "ENSG00000238009"
            is_cancer_gene_census: str | None = "true"
            symbol: str | None = "CSMD2"

        consequence_id: str | None = "cons-0"
        gene: Gene | None = Gene()

    cnv_id: str | None = "cnv-0"
    consequence: tuple[Consequence, ...] | None = (Consequence(),)
