"""A module for constants associated with the data model."""


class DataType:
    COPY_NUMBER_SEGMENT = "Copy Number Segment"
    ALLELE_SPECIFIC_COPY_NUMBER_SEGMENT = "Allele-specific Copy Number Segment"


class WorkflowType:
    ABSOLUTE = "ABSOLUTE LiftOver"
    ASCAT3 = "ASCAT3"
    ASCAT2 = "ASCAT2"
    ASCAT_NGS = "AscatNGS"
    GATK4_CNV = "GATK4 CNV"


class ExperimentalStrategy:
    WGS = "WGS"
    GENOTYPING_ARRAY = "Genotyping Array"
