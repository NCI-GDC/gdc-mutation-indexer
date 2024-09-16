import uuid
from collections.abc import Mapping, Sequence

import elasticsearch

from mutation_indexer import configuration


class ObsoleteConfig:
    """
    An adapter class mapping the new configuration setting back to the former
    BaseConfig object. This is a temporary measure until DEV-1250 is completed.
    """

    def __init__(
        self,
        config: configuration.Configuration,
        elasticsearch: elasticsearch.Elasticsearch,
    ) -> None:
        self._config = config
        self._elasticsearch = elasticsearch

        self.graph_case_doc_type = None
        self.graph_file_doc_type = None
        self.df_repartition = 2048
        self.df_coalesce = 12

    @property
    def maf_prioritized_experimental_strategies(self) -> Sequence[str]:
        return (
            self._config.builders.viz.maf_metadata.prioritized_experimental_strategies
        )

    @property
    def maf_data_types(self) -> list[str]:  # type: ignore
        raise NotImplementedError()

    @property
    def formatted_maf_keywords(self) -> str:  # type: ignore
        raise NotImplementedError()

    @property
    def protected_maf_keywords(self) -> str:  # type: ignore
        raise NotImplementedError()

    @property
    def gistic_filename_string(self) -> str:  # type: ignore
        raise NotImplementedError()

    @property
    def mappings(self) -> dict[str, str]:  # type: ignore
        raise NotImplementedError()

    @property
    def ssm_namespace(self) -> uuid.UUID:  # type: ignore
        raise NotImplementedError()

    @property
    def gene_model_file(self) -> str:  # type: ignore
        return self._config.builders.viz.gene_model.gene_model_file

    @property
    def citobands_file(self) -> str:  # type: ignore
        return self._config.builders.viz.gene_model.citobands_file

    @property
    def census_file(self) -> str:  # type: ignore
        return self._config.builders.viz.gene_model.census_file

    @property
    def maf_path(self) -> str:  # type: ignore
        return self._config.builders.viz.maf.backup.path

    @property
    def gistic_path(self) -> str:  # type: ignore
        raise NotImplementedError()

    @property
    def aliquot_path(self) -> str:  # type: ignore
        raise NotImplementedError()

    @property
    def gene_expression_values_path(self) -> str:  # type: ignore
        return self._config.builders.gene_expression.expression_value.backup.path

    @property
    def gene_expression_cases_path(self) -> str:  # type: ignore
        return self._config.builders.gene_expression.case.backup.path

    @property
    def primary_aliquot_path(self) -> str:  # type: ignore
        return self._config.builders.viz.primary_aliquot.backup.path

    @property
    def ascat_path(self) -> str:  # type: ignore
        return self._config.builders.viz.ascat.backup.path

    @property
    def gene_model_path(self) -> str:  # type: ignore
        return self._config.builders.viz.gene_model.backup.path

    @property
    def percentile_threshold(self) -> dict[str, int]:  # type: ignore
        return {
            "genes_per_case": self._config.builders.viz.case_centric.genes_threshold,
            "occurrences_per_ssm": self._config.builders.viz.ssm_centric.occurrences_threshold,
            "consequences_per_ssm": 100,
            "observations_per_ssm": 100,
            "occurrences_per_cnv": self._config.builders.viz.cnv_centric.occurrences_threshold,
        }

    @property
    def cache_dataframes(self) -> dict[str, bool]:  # type: ignore
        return {
            "maf_metadata": self._config.builders.viz.maf_metadata.is_cached,
            "maf": self._config.builders.viz.maf.is_cached,
            "case": self._config.builders.viz.case.is_cached,
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
    def case_include_as_arrays(self) -> Sequence[str]:
        return self._config.builders.viz.case.include_as_arrays

    @property
    def samples_include_fields(self) -> Sequence[str]:  # type: ignore
        return ()

    @property
    def projects(self) -> Sequence[str]:
        return self._config.build.projects

    @property
    def index_types(self) -> Sequence[str]:
        return tuple(typ.name.lower() for typ in self._config.build.index_types)

    @property
    def maf_metadata_backup(self) -> str:
        return self._config.builders.viz.maf_metadata.backup.mode.name.lower()

    @property
    def maf_backup(self) -> str:
        return self._config.builders.viz.maf.backup.mode.name.lower()

    @property
    def gistic_backup(self) -> str:
        raise NotImplementedError()

    @property
    def gene_expression_values_backup(self) -> str:
        return (
            self._config.builders.gene_expression.expression_value.backup.mode.name.lower()
        )

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
        return self._config.elasticsearch.write.batch_size_entries

    @property
    def batch_size_bytes(self) -> str:
        return self._config.elasticsearch.write.batch_size_bytes

    @property
    def graph_file_index(self) -> str:
        return self._config.elasticsearch.read.file_index

    @property
    def graph_case_index(self) -> str:
        return self._config.elasticsearch.read.case_index

    @property
    def indices(self) -> Mapping[str, str]:
        return {
            k.name.lower(): v
            for k, v in self._config.elasticsearch.write.indices.items()
        }

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

    def get_gistic_urls(self) -> list[str]:
        raise NotImplementedError()

    def get_samples_fields_to_exclude(self) -> list[str]:
        raise NotImplementedError()
