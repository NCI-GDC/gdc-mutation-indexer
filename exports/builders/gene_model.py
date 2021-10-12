import logging
from typing import Tuple

import config
from exports.builders import base_input_builder
from pyspark import sql
from pyspark.sql import types
from pyspark.sql import functions as F

logging.basicConfig(format=config.LOG_FORMAT)

TRANSCRIPTS_SCHEMA = types.ArrayType(
    types.StructType(
        [
            types.StructField("biotype", types.StringType()),
            types.StructField("cdna_coding_end", types.LongType()),
            types.StructField("cdna_coding_start", types.LongType()),
            types.StructField("coding_region_end", types.LongType()),
            types.StructField("coding_region_start", types.LongType()),
            types.StructField(
                "domains",
                types.ArrayType(
                    types.StructType(
                        [
                            types.StructField("description", types.StringType()),
                            types.StructField("end", types.LongType()),
                            types.StructField("gff_source", types.StringType()),
                            types.StructField("hit_name", types.StringType()),
                            types.StructField("interpro_id", types.StringType()),
                            types.StructField("start", types.LongType()),
                        ]
                    ),
                ),
            ),
            types.StructField("end", types.LongType()),
            types.StructField("end_exon", types.LongType()),
            types.StructField(
                "exons",
                types.ArrayType(
                    types.StructType(
                        [
                            types.StructField("cdna_coding_end", types.LongType()),
                            types.StructField("cdna_coding_start", types.LongType()),
                            types.StructField("cdna_end", types.LongType()),
                            types.StructField("cdna_start", types.LongType()),
                            types.StructField("end", types.LongType()),
                            types.StructField("end_phase", types.LongType()),
                            types.StructField("genomic_coding_end", types.LongType()),
                            types.StructField(
                                "genomic_coding_stairt", types.LongType()
                            ),
                            types.StructField("genomic_coding_start", types.LongType()),
                            types.StructField("start", types.LongType()),
                            types.StructField("start_phase", types.LongType()),
                        ]
                    ),
                ),
            ),
            types.StructField("transcript_id", types.StringType()),
            types.StructField("is_canonical", types.BooleanType()),
            types.StructField("length", types.LongType()),
            types.StructField("length_amino_acid", types.LongType()),
            types.StructField("length_cds", types.LongType()),
            types.StructField("name", types.StringType()),
            types.StructField("number_of_exons", types.LongType()),
            types.StructField("seq_exon_end", types.LongType()),
            types.StructField("seq_exon_start", types.LongType()),
            types.StructField("start", types.LongType()),
            types.StructField("start_exon", types.LongType()),
            types.StructField("translation_id", types.StringType()),
        ]
    ),
)
GENE_MODEL_SCHEMA = types.StructType(
    [
        types.StructField("_gene_id", types.StringType()),
        types.StructField(
            "_id", types.StructType([types.StructField("$oid", types.StringType())])
        ),
        types.StructField("biotype", types.StringType()),
        types.StructField("canonical_transcript_id", types.StringType()),
        types.StructField("chromosome", types.StringType()),
        types.StructField("cytoband", types.ArrayType(types.StringType())),
        types.StructField("description", types.StringType()),
        types.StructField("end", types.LongType()),
        types.StructField(
            "external_db_ids",
            types.StructType(
                [
                    types.StructField(
                        "entrez_gene", types.ArrayType(types.StringType())
                    ),
                    types.StructField("hgnc", types.ArrayType(types.StringType())),
                    types.StructField("omim_gene", types.ArrayType(types.StringType())),
                    types.StructField(
                        "uniprotkb_swissprot", types.ArrayType(types.StringType())
                    ),
                ]
            ),
        ),
        types.StructField("is_cancer_gene_census", types.StringType()),
        types.StructField("name", types.StringType()),
        types.StructField("start", types.LongType()),
        types.StructField("strand", types.LongType()),
        types.StructField("symbol", types.StringType()),
        types.StructField("synonyms", types.ArrayType(types.StringType())),
        types.StructField(
            "transcripts",
            types.ArrayType(
                types.StructType(
                    [
                        types.StructField("biotype", types.StringType()),
                        types.StructField("cdna_coding_end", types.StringType()),
                        types.StructField("cdna_coding_start", types.StringType()),
                        types.StructField("coding_region_end", types.StringType()),
                        types.StructField("coding_region_start", types.StringType()),
                        types.StructField(
                            "domains",
                            types.ArrayType(
                                types.StructType(
                                    [
                                        types.StructField(
                                            "description", types.StringType()
                                        ),
                                        types.StructField("end", types.LongType()),
                                        types.StructField(
                                            "gff_source", types.StringType()
                                        ),
                                        types.StructField(
                                            "hit_name", types.StringType()
                                        ),
                                        types.StructField(
                                            "interpro_id", types.StringType()
                                        ),
                                        types.StructField("start", types.LongType()),
                                    ]
                                )
                            ),
                        ),
                        types.StructField("end", types.StringType()),
                        types.StructField("end_exon", types.StringType()),
                        types.StructField(
                            "exons",
                            types.ArrayType(
                                types.StructType(
                                    [
                                        types.StructField(
                                            "cdna_coding_end", types.LongType()
                                        ),
                                        types.StructField(
                                            "cdna_coding_start", types.LongType()
                                        ),
                                        types.StructField("cdna_end", types.LongType()),
                                        types.StructField(
                                            "cdna_start", types.LongType()
                                        ),
                                        types.StructField("end", types.LongType()),
                                        types.StructField(
                                            "end_phase", types.LongType()
                                        ),
                                        types.StructField(
                                            "genomic_coding_end", types.LongType()
                                        ),
                                        types.StructField(
                                            "genomic_coding_stairt", types.LongType()
                                        ),
                                        types.StructField(
                                            "genomic_coding_start", types.LongType()
                                        ),
                                        types.StructField("start", types.LongType()),
                                        types.StructField(
                                            "start_phase", types.LongType()
                                        ),
                                    ]
                                )
                            ),
                        ),
                        types.StructField("id", types.StringType()),
                        types.StructField("is_canonical", types.StringType()),
                        types.StructField("length", types.StringType()),
                        types.StructField("length_amino_acid", types.StringType()),
                        types.StructField("length_cds", types.StringType()),
                        types.StructField("name", types.StringType()),
                        types.StructField("number_of_exons", types.StringType()),
                        types.StructField("seq_exon_end", types.StringType()),
                        types.StructField("seq_exon_start", types.StringType()),
                        types.StructField("start", types.StringType()),
                        types.StructField("start_exon", types.StringType()),
                        types.StructField("translation_id", types.StringType()),
                    ]
                )
            ),
        ),
    ]
)


class GeneModelBuilder(base_input_builder.BaseInputBuilder):
    """
    Constructs a Gene Model dataframe from ICGC's gene model json
    """

    def __init__(self, config: config.BaseConfig, sqlContext: sql.SQLContext) -> None:
        super().__init__(config, sqlContext, "gene_model")

        self.config = config
        self.sqlContext = sqlContext
        self.logger = logging.getLogger(self.__class__.__name__)

    def build_from_scratch(self, **kwargs: sql.DataFrame) -> sql.DataFrame:
        """
        Builds Gene Model dataframe

        GeneModel {}
        |---_gene_id
        |---biotype
        |---canonical_transcript_id
        |---chromosome
        |---cytoband [str]
        |---description
        |---entrez_gene [str]
        |---gene_end
        |---gene_strand
        |---gene_start
        |---hgnc [str]
        |---is_cancer_gene_census
        |---omim_gene [str]
        |---name
        |---sybol
        |---uniprotkb_swissprot [str]
        |---synonyms [str]
        |---_id [{}]
        |   +---$oid
        +---transcripts [{}]
            |---biotype
            |---cdna_coding_end
            |---cdna_coding_start
            |---coding_region_end
            |---coding_region_start
            |---domains [str]
            |---end
            |---end_exon
            |---transcript_id
            |---is_canonical
            |---length
            |---length_amino_acid
            |---length_cds
            |---name
            |---number_of_exons
            |---seq_exon_end
            |---seq_exon_start
            |---start
            |---start_exon
            |---translation_id
            +---exons [{}]
                |---cdna_coding_end
                |---cdna_coding_start
                |---cdna_end
                |---cdna_start
                |---end
                |---end_phase
                |---genomic_coding_end
                |---genomic_coding_stairt
                |---genomic_coding_start
                |---start
                +---start_phase
        """
        gene_df, cytobands_df, census_df = self.read_gene_model_files()

        # Join gene model with cytobands data:
        gene_df = gene_df.join(
            cytobands_df, gene_df._gene_id == cytobands_df.ens_gene_id, "left"
        ).join(census_df, gene_df._gene_id == census_df.cancer_gene_id, "left")

        return gene_df.select(
            "_gene_id",
            "_id",
            "biotype",
            "canonical_transcript_id",
            "chromosome",
            "cytoband",
            "description",
            "entrez_gene",
            "hgnc",
            "is_cancer_gene_census",
            "name",
            "omim_gene",
            "symbol",
            "synonyms",
            "uniprotkb_swissprot",
            F.col("end").alias("gene_end"),
            F.col("start").alias("gene_start"),
            F.col("strand").alias("gene_strand"),
            F.col("transcripts").cast(TRANSCRIPTS_SCHEMA),
        )

    def read_gene_model_files(
        self,
    ) -> Tuple[sql.DataFrame, sql.DataFrame, sql.DataFrame]:
        """
        Reads the gene model, cytobands and census files into Spark
        dataframes
        """
        cytobands_df = self.sqlContext.read.csv(
            self.config.citobands_file, sep="\t", header=True
        ).select(
            "ens_gene_id",
            F.coalesce(F.split(F.col("cytoband"), r","), F.array("cytoband")).alias(
                "cytoband"
            ),
        )

        census_df = self.sqlContext.read.csv(
            self.config.census_file,
            sep="\t",
            header=True,
        ).select(
            "cancer_gene_id",
            "is_cancer_gene_census",
        )

        # Flatten, the mapping will re-introduce the structure
        gene_model_df = self.sqlContext.read.json(
            self.config.gene_model_file, schema=GENE_MODEL_SCHEMA
        ).select(
            "_gene_id",
            "_id",
            "biotype",
            "canonical_transcript_id",
            "chromosome",
            "description",
            "end",
            "name",
            "start",
            "strand",
            "symbol",
            "synonyms",
            "transcripts",
            "external_db_ids.hgnc",
            "external_db_ids.omim_gene",
            "external_db_ids.uniprotkb_swissprot",
            "external_db_ids.entrez_gene",
        )

        # fields = []
        # for field in gene_model_df.schema["transcripts"].dataType.elementType.fields:
        #     if field.name == "id":
        #         field = types.StructField("transcript_id", types.StringType())
        #     fields.append(field)

        # print(types.ArrayType(types.StructType(fields)))

        return gene_model_df, cytobands_df, census_df
