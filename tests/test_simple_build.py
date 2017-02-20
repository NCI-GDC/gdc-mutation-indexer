import pytest

from utils import SparkTestCase
from exports.builders import (
    MAFBuilder,
    GeneCentricBuilder,
    CaseCentricBuilder,
    SSMCentricBuilder,
    SSMOccurrenceCentricBuilder
)
from config import TestConfig


conf = TestConfig()
@pytest.yield_fixture(scope='module')
def maf_df(sqlContext):
    yield MAFBuilder(conf, sqlContext).build()


@pytest.mark.parametrize("builder", [
                            CaseCentricBuilder,
                            GeneCentricBuilder,
                            SSMCentricBuilder,
                            SSMOccurrenceCentricBuilder])
def test_simple_build(maf_df, test_index, sqlContext, builder):
    builder(conf, sqlContext).build(maf_df)
