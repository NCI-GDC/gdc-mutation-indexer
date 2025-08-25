"""Module for removing any properties from the test data that has been marked as vestigial."""

import gzip
import json
import re
from collections.abc import Iterable, Sequence
from importlib import abc, resources
from typing import IO, Literal, NamedTuple

import yaml
from gdcmodels import esmodels

CASE_DATA_FILES = (
    "tests/integration/data/input/segment_cnv/segment_cnv-cases.ndjson",
    "tests/integration/data/input/cases.ndjson.gz",
)
"""All files in the integration test suite with case index data."""
FILE_DATA_FILES = (
    "tests/integration/data/input/segment_cnv/ascat_metadata-files.ndjson",
    "tests/integration/data/input/segment_cnv/segment_cnv-files.ndjson",
    "tests/integration/data/input/files_with_linked_cases.ndjson",
    "tests/integration/data/input/files.ndjson.gz",
    "tests/integration/data/input/ge-files.ndjson",
)
"""All files in the integration test suite with file index data."""

GDC_FROM_GRAPH_RESOURCE = resources.files(esmodels) / "gdc_from_graph"
VESTIGIAL_CASE_RESOURCE = GDC_FROM_GRAPH_RESOURCE / "case/vestigial.yaml"
"""The vestigial file resource for the case index"""
VESTIGIAL_FILE_RESOURCE = GDC_FROM_GRAPH_RESOURCE / "file/vestigial.yaml"
"""The vestigial file resource for the file index"""

VESTIGIAL_PATH_PATTERN = re.compile(r"\['properties']\['(.*?)'\]")
"""The patter for extracting the property names for the vestigial keys."""


class DataRemover(NamedTuple):
    """Removes data in the given files associated with the vestigial properties."""

    vestigial_resource: abc.Traversable
    file_paths: Iterable[str]

    def _open_file(self, file_path: str, mode: Literal["rt", "wt"]) -> IO[str]:
        """Opens file at the given path in the given mode.

        Args:
            file_path: The path to the file which needs to be open.
            mode: The mode in which the file should be opened.

        Returns:
            A string IO object which can be written to or read from depending on the given
            mode.
        """
        if file_path.endswith(".gz"):
            return gzip.open(file_path, mode)
        else:
            return open(file_path, mode)

    def _pop_property(self, record: dict, path: Sequence[str]) -> None:
        """Pops the given property from the given record.

        Args:
            record: The record which may contain data at the given path.
            path: The path to a property which should be removed.
        """
        prop, *path = path

        if not path:
            record.pop(prop, None)
            return

        if prop not in record:
            return

        value = record[prop]

        if isinstance(value, dict):
            self._pop_property(value, path)
        elif isinstance(value, Iterable):
            values = (v for v in value if isinstance(v, dict))

            for value in values:
                self._pop_property(value, path)

    def _pop_properties(self, record: dict, paths: Iterable[Sequence[str]]) -> dict:
        """Pops all the properties associated with the given paths from the record.

        Args:
            record: The record which may contain properties associated with the given paths.
            paths: The paths associated with properties which need to be removed from the
                given record.

        Returns:
            The given record with all properties removed.
        """
        for path in paths:
            self._pop_property(record, path)

        return record

    def _clean_file(self, data_file: str, vestigial_paths: Iterable[Sequence[str]]) -> None:
        """Cleans all vestigial properties from records within the given file.

        Args:
            data_file: The file containing test data.
            vestigial_paths: The paths to vestigial properties which need to be removed.
        """
        with self._open_file(data_file, "rt") as f:
            data = tuple(
                self._pop_properties(yaml.load(line, Loader=yaml.CSafeLoader), vestigial_paths)
                for line in f
            )

        with self._open_file(data_file, "wt") as f:
            f.writelines(f"{json.dumps(line, separators=(',', ':'))}\n" for line in data)

    def _load_vestigial_paths(self) -> Iterable[Sequence[str]]:
        """Loads the vestigial properties associated with an index.

        Returns:
            The loaded paths to vestigial properties which were found in the vestigial
            resource.
        """
        data = yaml.load(self.vestigial_resource.read_bytes(), Loader=yaml.CSafeLoader)
        vestigial_paths = data["dictionary_item_added"].keys()

        for path in vestigial_paths:
            yield VESTIGIAL_PATH_PATTERN.findall(path)

    def run(self) -> None:
        """Runs the data remover & cleans the configured data."""
        vestigial_paths = tuple(self._load_vestigial_paths())

        for file_path in self.file_paths:
            self._clean_file(file_path, vestigial_paths)


def remove() -> None:
    """Run the removal of data from data associated with the graph case and file index."""
    removers = (
        DataRemover(VESTIGIAL_CASE_RESOURCE, CASE_DATA_FILES),
        DataRemover(VESTIGIAL_FILE_RESOURCE, FILE_DATA_FILES),
    )

    for remover in removers:
        remover.run()
