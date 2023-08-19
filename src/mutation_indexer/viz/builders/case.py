import abc
import collections
import functools
import logging
import types
from collections.abc import Container, Iterable, Iterator, Mapping, Set
from typing import Deque, Final, Optional

from pyspark import sql
from pyspark.sql import functions as F
from typing_extensions import TypedDict

from mutation_indexer import es_utils
from mutation_indexer.builders import bases
from mutation_indexer.viz import configuration, constants

logger = logging.getLogger(__name__)


AVAILABLE_VARIATION_DATA = "available_variation_data"


def _is_included_field(
    excluded_fields: Container[str],
    included_fields: Optional[Iterable[str]],
    field: str,
) -> bool:
    """
    Determines if the given field should be included in the returned values based on the
    given excluded and included fields.

    Args:
        excluded_fields: fields to excluded from the encountered otherwise valid fields.
            Beyond excluding specific fields, this can be used to exclude all children
            of a given property.
        included_fields: a sub set of fields to be included from the encountered fields.
            This is useful for retrieving all children of a given property.
        field: the field in question.

    Returns:
        True if the field is a valid field and should be included in the resulting set
        of fields.
    """
    if field in excluded_fields:
        return False

    return included_fields is None or any(
        field.startswith(prefix) for prefix in included_fields
    )


def _convert_properties(
    properties: Mapping[str, Mapping],
    excluded_fields: Container[str],
    included_fields: Optional[Iterable[str]],
    path: str = "",
) -> Iterator[str]:
    """
    Converts all properties in the given mapping into flat fields which fall within the
    given included fields as well as outside of the excluded fields.

    Args:
        properties: the properties node of a elasticsearch mapping
        excluded_fields: fields to excluded from the encountered otherwise valid fields.
            Beyond excluding specific fields, this can be used to exclude all children
            of a given property.
        included_fields: a sub set of fields to be included from the encountered fields.
            This is useful for retrieving all children of a given property.
        path: the current path to the given set of properties.

    Yields:
        Individual fields from the given properties mapping.
    """
    fields: Iterable[tuple[str, Mapping]] = (
        (f"{path}{prop}", details) for prop, details in properties.items()
    )
    is_included_field = functools.partial(
        _is_included_field, excluded_fields, included_fields
    )
    fields = filter(lambda items: is_included_field(items[0]), fields)

    for field, details in fields:
        if "properties" in details:
            yield from _convert_properties(
                details["properties"],
                excluded_fields,
                included_fields,
                path=f"{field}.",
            )
        else:
            yield field


def _extract_fields(
    properties: Mapping[str, Mapping],
    excluded_fields: Container[str],
    included_fields: Optional[Iterable[str]],
    path_to_fields: Deque[str],
) -> Iterator[str]:
    """
    Extracts all fields which fall under the provided path and fall within the given
    included fields as well as outside of the excluded fields.

    Args:
        properties: the properties node of a elasticsearch mapping
        excluded_fields: fields to excluded from the encountered otherwise valid fields.
            Beyond excluding specific fields, this can be used to exclude all children
            of a given property.
        included_fields: a sub set of fields to be included from the encountered fields.
            This is useful for retrieving all children of a given property.
        path_to_fields: a series of properties which represent the path to the desired
            fields found within the given properties.

    Yields:
        Individual fields from the given properties mapping.
    """
    if not properties:
        return

    if path_to_fields:
        next_prop = path_to_fields.popleft()
        properties = properties.get(next_prop, {}).get("properties", {})

        yield from _extract_fields(
            properties, excluded_fields, included_fields, path_to_fields
        )

    else:
        yield from _convert_properties(properties, excluded_fields, included_fields)


class CaseFieldSelector:
    """A class for selecting the case fields in a given elasticsearch index."""

    __slots__ = ("_mappings_loader",)

    CASE_PREFIXES: Final[Mapping[constants.IndexType, str]] = types.MappingProxyType(
        {
            constants.IndexType.CASE: "",
            constants.IndexType.CASE_CENTRIC: "",
            constants.IndexType.CNV_CENTRIC: "occurrence.case",
            constants.IndexType.CNV_OCCURRENCE_CENTRIC: "case",
            constants.IndexType.SSM_CENTRIC: "occurrence.case",
            constants.IndexType.SSM_OCCURRENCE_CENTRIC: "case",
        }
    )

    def __init__(self, mappings_loader: Optional[es_utils.MappingsLoader] = None) -> None:
        self._mappings_loader = mappings_loader or es_utils.MappingsLoader()

    def _select_fields(
        self,
        index_type: constants.IndexType,
        excluded_fields: Container[str],
        included_fields: Optional[Iterable[str]],
    ) -> Set[str]:
        if index_type not in self.CASE_PREFIXES:
            raise ValueError(f"Index: {index_type} is not supported.")

        prefix = self.CASE_PREFIXES[index_type]
        path_to_fields = (
            collections.deque(prefix.split(".")) if prefix else collections.deque()
        )
        mappings = self._mappings_loader.load_mappings(index_type)["mappings"]
        fields = _extract_fields(
            mappings["properties"], excluded_fields, included_fields, path_to_fields
        )

        return frozenset(fields)

    def select_for(
        self,
        *index_types: constants.IndexType,
        excluded_fields: Container[str] = (),
        included_fields: Optional[Iterable[str]] = None,
    ) -> Iterable[str]:
        """
        Selects all common case fields found in the given indices.

        Args:
            *index_types: any indices which should be included when selecting the case
                fields. Valid types: CASE_CENTRIC, CNV_CENTRIC, CNV_OCCURRENCE_CENTRIC,
                SSM_CENTRIC, and SSM_OCCURRENCE_CENTRIC
            excluded_fields: any fields which should be excluded in the selection. If
                a parent field is excluded then all of its children will be eg. if the
                exclusion is samples, then samples.sample_id is automatically excluded.
            included_fields: restricts the select to only included a subset of fields.
                this is useful when selecting fields nested under a particular parent.
                The default is to include all fields.
        """
        field_sets = (
            self._select_fields(index_type, excluded_fields, included_fields)
            for index_type in index_types
        )

        return functools.reduce(lambda set0, set1: set0 & set1, field_sets) | frozenset(
            ("case_id",)
        )


def _load_available_variation_data(
    maf_metadata_df: sql.DataFrame, ascat_df: sql.DataFrame
) -> sql.DataFrame:
    ssm_data_df = maf_metadata_df.select(
        "case_id", F.lit("ssm").alias(AVAILABLE_VARIATION_DATA)
    )
    cnv_data_df = ascat_df.select("case_id", AVAILABLE_VARIATION_DATA)
    available_variation_df = ssm_data_df.union(cnv_data_df)

    # Finally, group by case
    return available_variation_df.groupby("case_id").agg(
        F.collect_set(AVAILABLE_VARIATION_DATA).alias(AVAILABLE_VARIATION_DATA)
    )


class CaseLoaderMixin(abc.ABC):
    """
    Builds a case dataframe by loading case documents from gdc_from_graph
    """

    @abc.abstractmethod
    def _load_es_case_data(self) -> sql.DataFrame:
        pass

    def _load_cases(
        self,
        maf_metadata_df: sql.DataFrame,
        ascat_df: sql.DataFrame,
        repartition_size: int,
    ) -> sql.DataFrame:
        """
        Builds Case dataframe
        """
        case_df = self._load_es_case_data()
        available_variation_df = _load_available_variation_data(
            maf_metadata_df, ascat_df
        )

        case_df = case_df.join(available_variation_df, on=["case_id"], how="left")

        return case_df.repartition(repartition_size, "case_id")


class CaseInputs(TypedDict):
    maf_metadata_df: sql.DataFrame
    ascat_df: sql.DataFrame


class CaseBuilder(bases.InputBuilder[configuration.CaseBuilder, CaseInputs], CaseLoaderMixin):
    __slots__ = ("_es_dataframe_util", "_field_selector")

    def __init__(
        self,
        config: configuration.CaseBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        field_selector: CaseFieldSelector,
    ) -> None:
        super().__init__(
            config, spark_session, input_type=CaseInputs, output=constants.DataFrame.CASE
        )

        self._es_dataframe_util = es_dataframe_util
        self._field_selector = field_selector

    def _load_es_case_data(self) -> sql.DataFrame:
        if self._config.projects:
            query = {"query": {"terms": {"project.project_id": self._config.projects}}}
        else:
            query = {"query": {"match_all": {}}}

        fields = self._field_selector.select_for(
            constants.IndexType.CASE,
            constants.IndexType.CNV_CENTRIC,
            constants.IndexType.CNV_OCCURRENCE_CENTRIC,
            constants.IndexType.SSM_CENTRIC,
            constants.IndexType.SSM_OCCURRENCE_CENTRIC,
        )

        # Only retrieve the fields we want
        logger.debug(f"Included fields: {fields}")

        # Load cases from graph index
        return self._es_dataframe_util.read(
            constants.IndexType.CASE,
            include_fields=fields,
            include_as_arrays=self._config.include_as_arrays,
            query=query,
        )

    def _build_from_scratch(self, input_dfs: CaseInputs) -> sql.DataFrame:
        return self._load_cases(
            input_dfs["maf_metadata_df"],
            input_dfs["ascat_df"],
            self._config.repartition_size,
        )
