from collections.abc import Iterable, Mapping
from importlib import resources
from typing import Union

import tomli
import yaml

from mutation_indexer import es_utils, viz
from mutation_indexer.constants import app, build

CASE_FIELD_SELECTOR = es_utils.CaseFieldSelector()
MAPPING_LOADER = es_utils.MappingsLoader()
SCHEMA_LOADER = es_utils.SchemaLoader()
Fields = Mapping[str, Union["Fields", None]]


def _get_array_configuration() -> Iterable[str]:
    resource = resources.files(viz) / app.CONFIGURATION_FILE

    with resource.open("rb") as f:
        config = tomli.load(f)

    case = config["builders"]["case"]["include_as_arrays"]
    case_centric = config["builders"]["case_centric"]["include_as_arrays"]

    return frozenset((*case, *case_centric))


def _sync(
    test_file: str, indices: Iterable[build.IndexType], include_as_arrays: Iterable[str]
) -> None:
    mappings = MAPPING_LOADER.load_mapper(build.IndexType.CASE).mappings
    included_fields = CASE_FIELD_SELECTOR.select_for(*indices)
    schema = SCHEMA_LOADER.load(mappings, included_fields, include_as_arrays)

    with open(test_file, "w") as f:
        yaml.dump(schema.jsonValue(), f, Dumper=yaml.CSafeDumper)


def sync_mappings():
    include_as_arrays = _get_array_configuration()
    _sync(
        "tests/unit/data/schemas/viz/builders/case/raw.yaml",
        (
            build.IndexType.CASE,
            build.IndexType.CNV_CENTRIC,
            build.IndexType.CNV_OCCURRENCE_CENTRIC,
            build.IndexType.SEGMENT_CNV_CENTRIC,
            build.IndexType.SSM_CENTRIC,
            build.IndexType.SSM_OCCURRENCE_CENTRIC,
        ),
        include_as_arrays,
    )
    _sync(
        "tests/unit/data/schemas/viz/builders/case_centric/case.yaml",
        (build.IndexType.CASE, build.IndexType.CASE_CENTRIC),
        include_as_arrays,
    )
