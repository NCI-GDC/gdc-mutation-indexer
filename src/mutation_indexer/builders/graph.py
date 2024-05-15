from collections.abc import Iterable
import contextlib
import functools
from typing import (
    Any,
    ContextManager,
    Iterator,
    NamedTuple,
    Optional,
    Protocol,
    Sequence,
    cast,
    Union
)
import graphframes
import more_itertools
from pyspark import sql
from pyspark.sql import functions as F, types
import psqlgraph
from psqlgraph import session
from gdcdatamodel2 import models
import sqlalchemy
from psqlgraph import query

from gdc_ng_models.models import submission


SUBMITTABLE_NODES = tuple(n for n in models.Node.get_subclasses() if n._dictionary.get("submittable", False))


def _walk_path(
    gf: graphframes.GraphFrame, origin: str, path: Iterable[str]
) -> sql.DataFrame:
    def build_motif(motif: str, elements: tuple[str, str]) -> str:
        previous = elements[0]
        new = elements[1]

        return f"{motif}-[{previous}_to_{new}]->({new})"

    def build_condition(condition: sql.Column, element: str) -> sql.Column:
        return condition & (F.col(f"{element}.label") == F.lit(element))

    path = tuple(more_itertools.value_chain(origin, path))
    elements = cast(
        Iterable[tuple[str, str]],
        more_itertools.windowed(path, n=2),
    )
    motif = functools.reduce(build_motif, elements, f"({origin})")
    condition = functools.reduce(build_condition, path, F.lit(1) == F.lit(1))

    return gf.find(motif).where(condition).select(f"{path[-1]}.*")


class Configuration(NamedTuple):
    projects: Sequence[str] = ()


EDGE_TYPE = types.StructType(
    [
        types.StructField("src", types.StringType()),
        types.StructField("src_label", types.StringType()),
        types.StructField("dst", types.StringType()),
        types.StructField("dst_label", types.StringType()),
        types.StructField("label", types.StringType()),
    ]
)


class Edge(NamedTuple):
    src: str
    dst: str
    label: str


class Vertex(NamedTuple):
    id: str
    label: str


class Session(Protocol):
    def nodes(self, type: Optional[Union[type[psqlgraph.Node], Sequence[type[psqlgraph.Node]]]] = None) -> query.GraphQuery: ...


class GraphSource(ContextManager["GraphSource"]):
    def __init__(self, graph: psqlgraph.PsqlGraphDriver) -> None:
        self._context = contextlib.ExitStack()
        self._graph = graph
        self.__session: Optional[session.GraphSession] = None

    def __enter__(self) -> "GraphSource":
        self.__session = cast(
            session.GraphSession,
            self._context.enter_context(self._graph.session_scope()),
        )
        _ = self._context.enter_context(self.__session.no_autoflush)

        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> Optional[bool]:
        self._context.close()

        return

    @property
    def _session(self) -> Session:
        assert self.__session, "Cannot access session outside of managed context."

        return self.__session

    def _extract_node(self, node) -> tuple[str, str]:
        return node.label, node.node_id

    def _load_nodes(self):
        projects = self._graph.nodes(models.Project.node_id).props(released=True)
        others = self._graph.nodes().filter(
            sqlalchemy.not_(models.Node._props.has_key("state"))
        )

    def _redacted_node_ids(self, projects: Iterable[str]) -> Iterator[str]:
        redactions: Iterable[models.Annotation] = (
            self._session.nodes(models.Annotation)
            .props(classification="Redaction")
            .not_props(status="Rescinded", category="Subject withdrew consent")
            .prop_in("project_id", list(projects))
        )

        for redaction in redactions:
            for node in redaction.traverse(edge_pointer="out"):
                yield node.node_id

    def _public_nodes(self):
        annotations = self._session.nodes(models.Annotation.node_id).props(state="released", status="Approved")
        non_annotation = self._session.nodes(tuple(n.node_id for n in models.Node.get_subclasses() if n.label != "annotation")).prop_in("state", ["live", "released"])
        version_transaction = self._session.nodes(submission.TransactionSnapshot).filter(
                    submission.TransactionSnapshot.action == "version",
                ).group_by(submission.TransactionSnapshot.id)
        versioned_files = self._session.nodes(tuple(n.node_id for n in SUBMITTABLE_NODES)).prop_in("state", ["validated", "submitted"]).filter(models.Node._props.has_key("file_name") and models.Node.node_id.in_(version_transaction))

        self._session.nodes().filter(sqlalchemy.not_(models.Node._props.has_key("state")) or )

    def load_graph(self, projects: Sequence[str]) -> Iterator:
        redacted_node_ids = frozenset(self._redacted_node_ids(projects))


class GraphEdge:
    def __init__(self) -> None:
        self.differentiated_edges = [
            ("file", "member_of", "archive"),
            ("archive", "member_of", "file"),
            ("file", "describes", "case"),
            ("case", "describes", "file"),
            ("file", "related_to", "file"),
        ]

    def eval(self) -> Iterator[Edge]:
        with self._graph.session_scope() as sxn, sxn.no_autoflush:
            # Cache graph to self.G
            # NOTE: if build_awg or selective_caching are set, will only iterate
            #   over relevant edges
            for e in self._iter_database_edges():
                triple = (e.src.label, e.label, e.dst.label)
                needs_differentiation = triple in self.differentiated_edges
                if triple == ("file", "data_from", "file"):
                    # for files that are "data_from" other files, the
                    # centers and aliquots of the source files count
                    # as neighbors of the dst files
                    for center in e.src.centers:
                        self.G.add_edge(e.dst, center)
                    for aliquot in e.src.aliquots:
                        self.G.add_edge(e.dst, aliquot)
                if e.label == "relates_to" and e.__dst_class__ == "Case":
                    pass
                elif needs_differentiation and e._props:
                    self.G.add_edge(e.src, e.dst, label=e.label, props=e._props)
                elif needs_differentiation and not e._props:
                    self.G.add_edge(e.src, e.dst, label=e.label)
                elif e._props:
                    self.G.add_edge(e.src, e.dst, props=e._props)
                else:
                    self.G.add_edge(e.src, e.dst)


class GraphBuilder:

    def __init__(self, config: Configuration, graph: psqlgraph.PsqlGraphDriver) -> None:
        self._config = config
        self._graph = graph

    def build(self) -> graphframes.GraphFrame:
        x = graphframes.GraphFrame([], []).shortestPaths([])

        return x
