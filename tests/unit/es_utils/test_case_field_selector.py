import functools
from collections.abc import Iterable
from unittest import mock

import pytest

from mutation_indexer import es_utils
from mutation_indexer.constants import build


class TestCaseFieldSelector:
    def arrange_mappings_loader(
        self,
        *mappings: dict,
        indices: Iterable[build.IndexType] = (build.IndexType.CASE_CENTRIC,),
    ) -> es_utils.MappingsLoader:
        index_mappings = {
            index: mock.MagicMock(mappings=mapping)
            for index, mapping in zip(indices, mappings)
        }
        load_mapper = mock.MagicMock(side_effect=lambda i: index_mappings[i])

        return mock.MagicMock(spec=es_utils.MappingsLoader, load_mapper=load_mapper)

    def test__select_for__basic_field(self):
        mapping = {"properties": {"field0": {"type": "keyword"}}}
        loader = self.arrange_mappings_loader(mapping)
        selector = es_utils.CaseFieldSelector(loader)

        fields = frozenset(selector.select_for(build.IndexType.CASE_CENTRIC))

        assert fields == frozenset({"field0", "case_id"})

    def test__select_for__field_with_nested_struct(self):
        mapping = {
            "properties": {"parent0": {"properties": {"sub_field0": {"type": "boolean"}}}}
        }

        loader = self.arrange_mappings_loader(mapping)
        selector = es_utils.CaseFieldSelector(loader)

        fields = frozenset(selector.select_for(build.IndexType.CASE_CENTRIC))

        assert fields == frozenset({"case_id", "parent0.sub_field0"})

    def test__select_for__exclude_field(self):
        mapping = {"properties": {"field0": {"type": "keyword"}}}
        loader = self.arrange_mappings_loader(mapping)
        selector = es_utils.CaseFieldSelector(loader)

        fields = frozenset(
            selector.select_for(build.IndexType.CASE_CENTRIC, excluded_fields=("field0",))
        )

        assert fields == frozenset({"case_id"})

    def test__select_for__exclude_parent_field_and_all_children(self):
        mapping = {
            "properties": {"parent0": {"properties": {"sub_field0": {"type": "boolean"}}}}
        }
        loader = self.arrange_mappings_loader(mapping)
        selector = es_utils.CaseFieldSelector(loader)

        fields = frozenset(
            selector.select_for(build.IndexType.CASE_CENTRIC, excluded_fields=("parent0",))
        )

        assert fields == frozenset({"case_id"})

    def test__select_for__exclude_prefixed_field(self):
        mapping = {"properties": {"case": {"properties": {"field0": {"type": "keyword"}}}}}
        loader = self.arrange_mappings_loader(
            mapping, indices=(build.IndexType.CNV_OCCURRENCE_CENTRIC,)
        )
        selector = es_utils.CaseFieldSelector(loader)

        fields = frozenset(
            selector.select_for(
                build.IndexType.CNV_OCCURRENCE_CENTRIC, excluded_fields=("field0",)
            )
        )

        assert fields == frozenset({"case_id"})

    def test__select_for__include_field(self):
        mapping = {
            "properties": {"field0": {"type": "keyword"}, "field1": {"type": "keyword"}}
        }
        loader = self.arrange_mappings_loader(mapping)
        selector = es_utils.CaseFieldSelector(loader)

        fields = frozenset(
            selector.select_for(build.IndexType.CASE_CENTRIC, included_fields=("field0",))
        )

        assert fields == frozenset({"case_id", "field0"})

    def test__select_for__include_parent_field_and_all_children(self):
        mapping = {
            "properties": {
                "parent0": {
                    "properties": {
                        "sub_field0": {"type": "boolean"},
                        "sub_field1": {"type": "keyword"},
                    }
                }
            }
        }
        loader = self.arrange_mappings_loader(mapping)
        selector = es_utils.CaseFieldSelector(loader)

        fields = frozenset(
            selector.select_for(build.IndexType.CASE_CENTRIC, included_fields=("parent0",))
        )

        assert fields == frozenset({"case_id", "parent0.sub_field0", "parent0.sub_field1"})

    def test__select_for__include_prefixed_field(self):
        mapping = {
            "properties": {
                "case": {
                    "properties": {
                        "field0": {"type": "keyword"},
                        "field1": {"type": "integer"},
                    }
                }
            }
        }
        loader = self.arrange_mappings_loader(
            mapping, indices=(build.IndexType.SSM_OCCURRENCE_CENTRIC,)
        )
        selector = es_utils.CaseFieldSelector(loader)

        fields = frozenset(
            selector.select_for(
                build.IndexType.SSM_OCCURRENCE_CENTRIC, included_fields=("field1",)
            )
        )

        assert fields == frozenset({"case_id", "field1"})

    @pytest.mark.parametrize(
        ("index", "prefix"),
        (
            pytest.param(build.IndexType.CNV_CENTRIC, "occurrence.case"),
            pytest.param(build.IndexType.CNV_OCCURRENCE_CENTRIC, "case"),
            pytest.param(build.IndexType.SSM_CENTRIC, "occurrence.case"),
            pytest.param(build.IndexType.SSM_OCCURRENCE_CENTRIC, "case"),
        ),
    )
    def test__select_for__remove_case_prefix(self, index: build.IndexType, prefix: str):
        mapping = functools.reduce(
            lambda acc, prop: {"properties": {prop: acc}},
            reversed(prefix.split(".")),
            {"properties": {"prop": {"type": "double"}}},
        )

        loader = self.arrange_mappings_loader(mapping, indices=(index,))
        selector = es_utils.CaseFieldSelector(loader)

        fields = frozenset(selector.select_for(index))

        assert fields == frozenset({"case_id", "prop"})

    def test__select_for__filter_non_case_prefixed_fields(self):
        mapping = {
            "properties": {"consequence": {"properties": {"field0": {"type": "keyword"}}}}
        }
        loader = self.arrange_mappings_loader(mapping, indices=(build.IndexType.CNV_CENTRIC,))
        selector = es_utils.CaseFieldSelector(loader)

        fields = frozenset(selector.select_for(build.IndexType.CNV_CENTRIC))

        assert fields == frozenset({"case_id"})

    @pytest.mark.parametrize(
        "index",
        (
            pytest.param(build.IndexType.FILE),
            pytest.param(build.IndexType.GENE_EXPRESSION),
        ),
    )
    def test__select_for__raises_value_error_for_unknown_indices(
        self, index: build.IndexType
    ) -> None:
        mapping = {
            "properties": {"consequence": {"properties": {"field0": {"type": "keyword"}}}}
        }
        loader = self.arrange_mappings_loader(mapping, indices=(index,))
        selector = es_utils.CaseFieldSelector(loader)

        with pytest.raises(ValueError):
            selector.select_for(index)

    def test__select_for__multiple_indices_fields_are_intersected(self) -> None:
        case_centric_mapping = {
            "properties": {
                "field0": {"type": "keyword"},
                "field1": {"type": "double"},
                "field2": {"type": "boolean"},
            }
        }
        cnv_centric_mappings = {
            "properties": {
                "occurrence": {
                    "properties": {
                        "case": {
                            "properties": {
                                "field0": {"type": "keyword"},
                                "field1": {"type": "double"},
                                "field3": {"type": "long"},
                            }
                        }
                    }
                }
            }
        }
        loader = self.arrange_mappings_loader(
            case_centric_mapping,
            cnv_centric_mappings,
            indices=(build.IndexType.CASE_CENTRIC, build.IndexType.CNV_CENTRIC),
        )
        selector = es_utils.CaseFieldSelector(loader)

        fields = frozenset(
            selector.select_for(build.IndexType.CASE_CENTRIC, build.IndexType.CNV_CENTRIC)
        )

        assert fields == frozenset({"case_id", "field0", "field1"})
