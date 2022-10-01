import contextlib
import dataclasses
import logging
from typing import Any, Iterable, Iterator, Sequence, Union

import elasticsearch
import more_itertools
import pytest

from exports.builders import ascat
from tests.integration import config
from tests.integration.utils import test_setup

conf = config.TestConfig()
logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class Analysis:
    workflow_type: str = "ASCAT2"


@dataclasses.dataclass(frozen=True)
class Program:
    name: str = "TCGA"


@dataclasses.dataclass(frozen=True)
class Project:
    program: Program = Program()
    project_id: str = "TCGA-TEST"


@dataclasses.dataclass(frozen=True)
class Case:
    project: Project = Project()


@dataclasses.dataclass(frozen=True)
class File:
    file_id: str
    analysis: Analysis = Analysis()
    cases: Sequence[Case] = (Case(),)
    data_type: str = "Gene Level Copy Number"
    experimental_strategy: str = "Genotyping Array"


@pytest.mark.usefixtures("setup_graph_indices")
class TestFileSelector:
    @pytest.fixture(autouse=True)
    def arrange_fixtures(self, es_client: elasticsearch.Elasticsearch) -> None:
        self._es_client = es_client

    def arrange_selector(self) -> ascat.FileSelector:
        return ascat.FileSelector(conf, self._es_client)

    @contextlib.contextmanager
    def arrange_files(self, files: Union[File, Iterable[File]]) -> Iterator[Any]:
        docs = (
            dataclasses.asdict(files)
            if isinstance(files, File)
            else (dataclasses.asdict(file) for file in files)
        )

        with test_setup.DocumentLoader(conf, self._es_client) as loader:
            yield loader.load_docs("file", docs)

    @pytest.mark.parametrize(
        ("workflow_type", "experimental_strategy"),
        (
            ("ASCAT2", "Genotyping Array"),
            ("AscatNGS", "WGS"),
        ),
    )
    def test__get_ids__ascat_docs_returned(
        self, workflow_type: str, experimental_strategy: str
    ) -> None:
        file = File(
            "ascat_file",
            analysis=Analysis(workflow_type),
            experimental_strategy=experimental_strategy,
        )
        selector = self.arrange_selector()

        with self.arrange_files(file):
            result_ids = selector.get_ids(())

        assert more_itertools.one(result_ids) == file.file_id

    @pytest.mark.parametrize(
        ("workflow_type", "experimental_strategy", "data_type"),
        (
            ("ASCAT2", "WGS", "Gene Level Copy Number"),
            ("AscatNGS", "Genotyping Array", "Gene Level Copy Number"),
            ("ASCAT2", "Genotyping Array", "Tumor Level Copy Number"),
        ),
    )
    def test__get_ids__unkown_docs_not_returned(
        self, workflow_type: str, experimental_strategy: str, data_type: str
    ) -> None:
        file = File(
            "ascat_file",
            analysis=Analysis(workflow_type),
            experimental_strategy=experimental_strategy,
            data_type=data_type,
        )
        selector = self.arrange_selector()

        with self.arrange_files(file):
            result_ids = selector.get_ids(())

        assert not result_ids

    def test__get_ids__only_select_projects(self) -> None:
        file0 = File("ascat0", cases=(Case(project=Project(project_id="TCGA-TEST")),))
        file1 = File(
            "ascat1", cases=(Case(project=Project(project_id="TCGA-SELECTED")),)
        )
        selector = self.arrange_selector()

        with self.arrange_files((file0, file1)):
            result_ids = selector.get_ids(("TCGA-SELECTED",))

        assert more_itertools.one(result_ids) == file1.file_id

    def test__get_ids__ignore_non_tcga(self) -> None:
        file = File(
            "ascat_file",
            cases=(
                Case(
                    project=Project(
                        program=Program(name="TEST"), project_id="TEST-TEST"
                    )
                ),
            ),
        )
        selector = self.arrange_selector()

        with self.arrange_files(file):
            result_ids = selector.get_ids(("TEST-TEST",))

        assert not result_ids
