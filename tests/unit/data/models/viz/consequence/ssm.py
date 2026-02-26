import dataclasses


@dataclasses.dataclass(frozen=True)
class SSM:
    @dataclasses.dataclass(frozen=True)
    class Consequence:
        @dataclasses.dataclass(frozen=True)
        class Transcript:
            @dataclasses.dataclass(frozen=True)
            class Annotation:
                amino_acids: str | None = "A/S"
                ccds: str | None = "CCDS380.1"
                cdna_position: str | None = "1734/13108"
                cds_end: int | None = 12169
                cds_length: int | None = 10464
                cds_position: str | None = "1705/10464"
                cds_start: int | None = 1705
                clin_sig: str | None = "ss"
                codons: str | None = "Gct/Tct"
                dbsnp_rs: str | None = "novel"
                dbsnp_val_status: str | None = None
                domains: str | None = "dfjks;sksk"
                ensp: str | None = "ENSP00000241312"
                existing_variation: str | None = None
                hgvsc: str | None = "c.1705G>T"
                hgvsp: str | None = "p.Ala569Ser"
                hgvsp_short: str | None = "p.A569S"
                polyphen_impact: str | None = "benign"
                polyphen_score: float | None = 0.305
                protein_position: str | None = "569/3487"
                pubmed: str | None = None
                sift_impact: str | None = "tolerated"
                sift_score: float | None = 0.12
                swissprot: str | None = "Q7Z408.146"
                transcript_id: str | None = "ENST00000241312"
                trembl: str | None = "DKD"
                uniparc: str | None = "UPI00004561AB"
                vep_impact: str | None = "MODERATE"

            @dataclasses.dataclass(frozen=True)
            class Gene:
                @dataclasses.dataclass(frozen=True)
                class ExternalDBIds:
                    entrez_gene: tuple[str, ...] | None = (
                        "100287596",
                        "100287102",
                        "727856",
                        "84771",
                    )
                    hgnc: tuple[str, ...] | None = ("HGNC:37102",)
                    omim_gene: tuple[str, ...] | None = ()
                    uniprotkb_swissprot: tuple[str, ...] | None = ()

                biotype: str | None = "transcribed_unprocessed_pseudogene"
                canonical_transcript_id: str | None = "ENST00000456328"
                cytoband: tuple[str, ...] | None = ("1p36.33",)
                external_db_ids: ExternalDBIds | None = ExternalDBIds()
                gene_chromosome: str | None = "1"
                gene_end: int | None = 14409
                gene_id: str | None = "ENSG00000238009"
                gene_start: int | None = 11869
                gene_strand: int | None = 1
                is_cancer_gene_census: bool | None = True
                symbol: str | None = "CSMD2"
                synonyms: tuple[str, ...] | None = ()

            transcript_id: str | None = "ENST00000241312"
            aa_change: str | None = "R450H"
            aa_end: int | None = 451
            aa_start: int | None = 461
            consequence_type: str | None = "missense_variant;NMD_transcript_variant"
            is_canonical: bool | None = None
            ref_seq_accession: str | None = None
            annotation: Annotation | None = Annotation()
            gene: Gene | None = Gene()

        consequence_id: str | None = "377b6f05-34e8-51d0-81a6-3a8781032253"
        transcript: Transcript | None = Transcript()

    ssm_id: str | None = "ssm-0"
    consequence: tuple[Consequence, ...] | None = (Consequence(),)
    gene_aa_change: tuple[str, ...] | None = ("change",)
