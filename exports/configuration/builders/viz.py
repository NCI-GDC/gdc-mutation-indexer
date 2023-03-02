"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""
import dataclasses
from typing import Sequence

from marshmallow import fields

from exports.configuration import marshmallow_extensions

# these are directly imported to created a better interface when using the viz module
from exports.configuration.builders.common import (
    Builder,
    CentricBuilder,
    GeneModelBuilder,
)


@dataclasses.dataclass(frozen=True)
class ASCATBuilder(Builder):
    """
    Configuration values for running the ascat builder
    """

    omit_cnv_data: bool


@dataclasses.dataclass(frozen=True)
class CaseBuilder(Builder):
    """
    Configuration values for running the case builder
    """

    include_as_arrays: Sequence[str] = dataclasses.field(
        metadata={
            "metadata": {
                "marshmallow_field": marshmallow_extensions.ArbitraryLengthTuple(
                    fields.String()
                )
            }
        }
    )
    repartition_size: int


@dataclasses.dataclass(frozen=True)
class MAFBuilder(Builder):
    """
    Configuration values for running the MAF builder
    """

    repartition_size: int


@dataclasses.dataclass(frozen=True)
class MAFMetadataBuilder(Builder):
    """
    Configuration values for running the MAF metadata builder
    """

    prioritized_experimental_strategies: Sequence[str] = dataclasses.field(
        metadata={
            "marshmallow_filed": marshmallow_extensions.ArbitraryLengthTuple(
                fields.String()
            )
        }
    )


@dataclasses.dataclass(frozen=True)
class PROTBuilder(Builder):
    data_package: str
    data_resource: str


@dataclasses.dataclass(frozen=True)
class DNABuilder(Builder):
    data_package: str
    data_resource: str


@dataclasses.dataclass(frozen=True)
class CaseCentricBuilder(CentricBuilder):
    """
    Configuration values for running the case centric builder
    """

    genes_threshold: int
    include_as_arrays: Sequence[str] = dataclasses.field(
        metadata={
            "metadata": {
                "marshmallow_field": marshmallow_extensions.ArbitraryLengthTuple(
                    fields.String()
                )
            }
        }
    )


@dataclasses.dataclass(frozen=True)
class CNVCentricBuilder(CentricBuilder):
    """
    Configuration values for running the case builder
    """

    occurrences_threshold: int


@dataclasses.dataclass(frozen=True)
class SSMCentricBuilder(CentricBuilder):
    """
    Configuration values for running the SSM centric builder
    """

    occurrences_threshold: int


@dataclasses.dataclass(frozen=True)
class Viz:
    """
    Configuration values for running the export of the viz indices
    """

    ascat: ASCATBuilder
    case: CaseBuilder
    civic_dna: DNABuilder
    civic_prot: PROTBuilder
    gene_model: GeneModelBuilder
    maf_metadata: MAFMetadataBuilder
    maf: MAFBuilder
    primary_aliquot: Builder
    case_centric: CaseCentricBuilder
    gene_centric: CentricBuilder
    cnv_centric: CNVCentricBuilder
    cnv_occurrence_centric: CentricBuilder
    ssm_centric: SSMCentricBuilder
    ssm_occurrence_centric: CentricBuilder
