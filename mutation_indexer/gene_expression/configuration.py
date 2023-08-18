import dataclasses

import marshmallow
import marshmallow_dataclass
from pyparsing import Mapping

from mutation_indexer import configuration
from mutation_indexer.configuration import elasticsearch
from mutation_indexer.configuration.builders import *
from mutation_indexer.gene_expression import constants


@dataclasses.dataclass(frozen=True)
class Builders:
    """
    Configuration values for running the export the gene expression indices
    """

    gene_model: GeneModelBuilder
    case: Builder
    expression_value: Builder
    primary_aliquot: Builder
    gene_expression: IndexBuilder


@dataclasses.dataclass(frozen=True)
class Read(elasticsearch.Read):
    indices: Mapping[constants.IndexType, str]


@dataclasses.dataclass(frozen=True)
class Write(elasticsearch.Write):
    indices: Mapping[constants.IndexType, str]


@dataclasses.dataclass(frozen=True)
class Elasticsearch(elasticsearch.Elasticsearch):
    read: Read
    write: Write


@marshmallow_dataclass.dataclass(frozen=True)
class Configuration(configuration.Configuration[Builders]):
    builders: Builders
    elasticsearch: Elasticsearch


SCHEMA: marshmallow.Schema = Configuration.Schema(unknown="EXCLUDE")  # type: ignore
OBFUSCATED_SCHEMA: marshmallow.Schema = Configuration.Schema(  # type: ignore
    context={"is_obfuscated": True}
)
