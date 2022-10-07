from typing import Callable, Sequence

import pytest
from normalizer import mapper

from exports.builders import utils


@pytest.mark.usefixtures("spark_session")
@pytest.mark.parametrize(
    "index",
    ["ssm_centric", "ssm_occurrence_centric", "cnv_centric", "cnv_occurrence_centric"],
)
def test__struct_select__without_selector(index: str) -> None:
    model_mapper = mapper.ModelMapper(index)

    # Make sure that we are actually testing something
    assert len(model_mapper.nested_mappings) > 0

    for mapping in model_mapper.nested_mappings:
        stmt = utils.struct_select(index, mapping)
        assert stmt


@pytest.mark.usefixtures("spark_session")
@pytest.mark.parametrize(
    "index, selector",
    [
        ("case_centric", lambda x: x[:1]),
        ("case_centric", lambda x: [x[1]] if len(x) > 1 else x[:1]),
        ("gene_centric", lambda x: x[:1]),
        ("gene_centric", lambda x: [x[1]] if len(x) > 1 else x[:1]),
    ],
)
def test__struct_select__with_selector(
    index: str, selector: Callable[[Sequence[str]], Sequence[str]]
) -> None:
    """
    Since case_centric and gene_centric have same nested mappings under
    different paths we need to provide a selector to resolve it.
    """
    model_mapper = mapper.ModelMapper(index)

    # Make sure that we are actually testing something
    assert len(model_mapper.nested_mappings) > 0

    for mapping in model_mapper.nested_mappings:
        stmt = utils.struct_select(index, mapping, selector=selector)
        assert stmt
