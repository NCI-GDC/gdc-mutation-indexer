"""A module for syncing schema structures with external dependencies."""

from collections.abc import Iterable
from importlib import resources
from typing import NamedTuple

import gdcmodels
from gdcmodels import esmodels

from mutation_indexer import es_utils
from mutation_indexer.constants import app, build
from mutation_indexer.viz import configuration
from tests.unit.data import schemas

CASE_FIELD_SELECTOR = es_utils.CaseFieldSelector()
SCHEMA_LOADER = es_utils.SchemaLoader()


def _load_case_mappings() -> esmodels.ESMapping:
    """Loads the case mappings from GDC models."""
    models = gdcmodels.get_es_models(vestigial_included=False)

    return models["gdc_from_graph"]["case"].mappings


class CaseSync(NamedTuple):
    """A class for managing updating the case schemas for various inputs."""

    indices: Iterable[build.IndexType]
    include_as_arrays: Iterable[str]
    output_schema: schemas.Schema

    def run(self) -> None:
        """Syncs the output schema to reflect the various mappings."""
        mappings = _load_case_mappings()
        properties = CASE_FIELD_SELECTOR.select_for(*self.indices) - frozenset(
            ("case_autocomplete",)
        )
        schema = SCHEMA_LOADER.load(mappings, properties, self.include_as_arrays)

        self.output_schema.update(schema)


def sync_case() -> None:
    """Syncs the input schemas which are based on data loaded from the graph case index."""
    test_config = resources.files("tests.unit.data") / app.CONFIGURATION_FILE

    with configuration.Configuration.client_context((test_config,)) as config:
        syncs = (
            CaseSync(
                indices=(build.IndexType.CASE, build.IndexType.CASE_CENTRIC),
                include_as_arrays=config.builders.case_centric.include_as_arrays,
                output_schema=schemas.Viz.Builders.CaseCentric.CASE,
            ),
            CaseSync(
                indices=(
                    build.IndexType.CASE,
                    build.IndexType.CNV_CENTRIC,
                    build.IndexType.CNV_OCCURRENCE_CENTRIC,
                    build.IndexType.GENE_CENTRIC,
                    build.IndexType.SEGMENT_CNV_CENTRIC,
                    build.IndexType.SEGMENT_CNV_OCCURRENCE_CENTRIC,
                    build.IndexType.SSM_CENTRIC,
                    build.IndexType.SSM_OCCURRENCE_CENTRIC,
                ),
                include_as_arrays=config.builders.case.include_as_arrays,
                output_schema=schemas.Viz.Builders.Case.RAW,
            ),
        )

        for sync in syncs:
            sync.run()
