import uuid
from typing import Dict, List, Mapping, Sequence

import elasticsearch as es
import marshmallow
import marshmallow_dataclass
from indexclient import client

from mutation_indexer.core.configuration import (
    aws,
    build,
    builders,
    elasticsearch,
    indexd,
    spark,
)
from mutation_indexer.core.constants import master


@marshmallow_dataclass.dataclass(frozen=True)
class Configuration:
    spark_arguments: spark.Arguments
    spark: spark.Spark
    build: build.Build
    builders: builders.Builders
    aws: aws.AWS
    indexd: indexd.IndexD
    elasticsearch: elasticsearch.Elasticsearch

    @marshmallow.validates_schema
    def validate_builders(self, data: dict) -> None:
        if data["build"]["driver"] == master.Driver.VIZ and not data["builders"]["viz"]:
            raise marshmallow.ValidationError(
                "Viz builders must be configured for VIZ driver.", "builders.viz"
            )

        if (
            data["build"]["driver"] == master.Driver.GENE_EXPRESSION
            and not data["builders"]["gene_expression"]
        ):
            raise marshmallow.ValidationError(
                "Gene expression builders must be configured for GENE_EXPRESSION driver.",
                "builders.gene_expression",
            )


CONFIG_SCHEMA: marshmallow.Schema = Configuration.Schema()  # type: ignore


class ConfigAdapter:
    def __init__(
        self,
        config: Configuration,
        elasticsearch: es.Elasticsearch,
        indexd: client.IndexClient,
    ) -> None:
        self._config = config
        self._elasticsearch = elasticsearch
        self._indexd = indexd

    @property
    def maf_data_types(self) -> List[str]:
        raise NotImplementedError()

    @property
    def formatted_maf_keywords(self) -> str:
        raise NotImplementedError()

    @property
    def protected_maf_keywords(self) -> str:
        raise NotImplementedError()

    @property
    def gistic_filename_string(self) -> str:
        raise NotImplementedError()

    @property
    def mappings(self) -> Dict[str, str]:
        raise NotImplementedError()

    @property
    def ssm_namespace(self) -> uuid.UUID:
        raise NotImplementedError()

    @property
    def gene_model_file(self) -> str:
        return self._config.builders.viz.gene_model.gene_model_file

    @property
    def citobands_file(self) -> str:
        return self._config.builders.viz.gene_model.citobands_file

    @property
    def census_file(self) -> str:
        return self._config.builders.viz.gene_model.census_file

    @property
    def maf_path(self) -> str:
        return self._config.builders.viz.maf.backup.path

    @property
    def gistic_path(self) -> str:
        raise NotImplementedError()

    @property
    def aliquot_path(self) -> str:
        raise NotImplementedError()

    @property
    def gene_expression_values_path(self) -> str:
        return self._config.builders.gene_expression.value.backup.path

    @property
    def gene_expression_cases_path(self) -> str:
        return self._config.builders.gene_expression.case.backup.path

    @property
    def primary_aliquot_path(self) -> str:
        return self._config.builders.viz.primary_aliquot.backup.path

    @property
    def ascat_path(self) -> str:
        return self._config.builders.viz.ascat.backup.path

    @property
    def gene_model_path(self) -> str:
        return self._config.builders.viz.gene_model.backup.path

    @property
    def percentile_threshold(self) -> Dict[str, int]:
        return {
            "genes_per_case": self._config.builders.viz.case_centric.array_size_threshold,
            "occurrences_per_ssm": self._config.builders.viz.ssm_centric.array_size_threshold,
            "consequences_per_ssm": 100,
            "observations_per_ssm": 100,
            "occurrences_per_cnv": self._config.builders.viz.cnv_centric.array_size_threshold,
        }

    @property
    def cache_dataframes(self) -> Dict[str, bool]:
        return {
            "mafs": self._config.builders.viz.maf.is_cached,
            "cases": self._config.builders.viz.case.is_cached,
            "case_centric": self._config.builders.viz.case_centric.is_cached,
            "gene_centric": self._config.builders.viz.gene_centric.is_cached,
            "ssm_centric": self._config.builders.viz.ssm_centric.is_cached,
            "ssm_occurrence_centric": self._config.builders.viz.ssm_occurrence_centric.is_cached,
            "cnv_centric": self._config.builders.viz.cnv_centric.is_cached,
            "cnv_occurrence_centric": self._config.builders.viz.cnv_occurrence_centric.is_cached,
            "primary_aliquot": self._config.builders.viz.primary_aliquot.is_cached,
            "gene_model": self._config.builders.viz.gene_model.is_cached,
        }

    @property
    def case_exclude_fields(self) -> Sequence[str]:
        raise NotImplementedError()

    @property
    def exclude_fields(self) -> Sequence[str]:
        return self._config.builders.viz.case.excluded_fields

    @property
    def samples_include_fields(self) -> Sequence[str]:
        return self._config.builders.viz.case.included_fields

    @property
    def projects(self) -> Sequence[str]:
        return self._config.build.projects

    @property
    def index_types(self) -> Sequence[str]:
        return self._config.build.index_types

    @property
    def maf_backup(self) -> str:
        return self._config.builders.viz.maf.backup.mode.name.lower()

    @property
    def gistic_backup(self) -> str:
        raise NotImplementedError()

    @property
    def gene_expression_cases_backup(self) -> str:
        return self._config.builders.gene_expression.case.backup.mode.name.lower()

    @property
    def gene_expression_primary_aliquot_backup(self) -> str:
        return (
            self._config.builders.gene_expression.primary_aliquot.backup.mode.name.lower()
        )

    @property
    def primary_aliquot_backup(self) -> str:
        return self._config.builders.viz.primary_aliquot.backup.mode.name.lower()

    @property
    def gene_model_backup(self) -> str:
        return self._config.builders.viz.gene_model.backup.mode.name.lower()

    @property
    def ascat_backup(self) -> str:
        return self._config.builders.viz.ascat.backup.mode.name.lower()

    @property
    def case_backup(self) -> str:
        return self._config.builders.viz.case.backup.mode.name.lower()

    @property
    def output_raw(self) -> str:
        return self._config.builders.viz.case_centric.backup.mode.name.lower()

    @property
    def debug(self) -> bool:
        return False

    @property
    def skip_normalization(self) -> bool:
        return False

    @property
    def omit_cnv_data(self) -> bool:
        return self._config.builders.viz.ascat.omit_cnv_data

    @property
    def batch_size_entries(self) -> int:
        return self._config.elasticsearch.write.batch_size_entities

    @property
    def batch_size_bytes(self) -> str:
        return self._config.elasticsearch.write.batch_size_bytes

    @property
    def df_repartition(self) -> int:
        return self._config.elasticsearch.write.repartition_size

    @property
    def df_coalesce(self) -> int:
        return self._config.elasticsearch.write.coalesce_size

    @property
    def graph_file_index(self) -> str:
        return self._config.elasticsearch.read.file_index

    @property
    def graph_case_index(self) -> str:
        return self._config.elasticsearch.read.case_index

    @property
    def indices(self) -> Mapping[str, str]:
        return self._config.build.indices

    @property
    def es_nodes(self) -> str:
        return self._config.elasticsearch.connection.nodes

    @property
    def source_es_nodes(self) -> str:
        return self._config.elasticsearch.connection.nodes

    @property
    def es_user(self) -> str:
        return self._config.elasticsearch.connection.user

    @property
    def source_es_user(self) -> str:
        return self._config.elasticsearch.connection.user

    @property
    def es_pass(self) -> str:
        return self._config.elasticsearch.connection.password

    @property
    def source_es_pass(self) -> str:
        return self._config.elasticsearch.connection.password

    @property
    def es_use_ssl(self) -> bool:
        return self._config.elasticsearch.connection.use_ssl

    @property
    def disable_es_verify_certs(self) -> bool:
        return not self._config.elasticsearch.connection.verify_certs

    @property
    def es(self) -> elasticsearch.Elasticsearch:
        return self._elasticsearch

    @property
    def indexd(self) -> client.IndexClient:
        return self._indexd

    def get_raw_output_path(self, index_name: str) -> str:
        paths = {
            "case_centric": self._config.builders.viz.case_centric.backup.path,
            "cnv_centirc": self._config.builders.viz.cnv_centric.backup.path,
            "cnv_occurence_centric": self._config.builders.viz.cnv_occurrence_centric.backup.path,
            "ssm_centric": self._config.builders.viz.ssm_centric.backup.path,
            "ssm_occurence_centric": self._config.builders.viz.ssm_occurrence_centric.backup.path,
            "gene_expression": self._config.builders.gene_expression.gene_expression.backup.path,
        }

        return paths[index_name]

    def list_bucket(self, bucket_name):
        raise NotImplementedError()

    def get_gistic_urls(self) -> List[str]:
        raise NotImplementedError()

    def get_samples_fields_to_exclude(self) -> List[str]:
        raise NotImplementedError()
