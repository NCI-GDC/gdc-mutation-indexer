import json
from typing import Any, Dict, Set

import elasticsearch
import pytest
from pyspark import sql

from exports import builders, configuration, es_utils
from exports.constants import build
from tests.integration.builders import base_joins_test


def get_generic_stats(
    maf_df: sql.DataFrame,
    cnv_df: sql.DataFrame,
) -> Dict[str, Set[str]]:
    """
    Returns true stats for :doc_type
    """
    stats = {}

    # Add maf and gistic info:
    maf_data = maf_df.select("case_id", "gene_id").collect()
    gistic_data = cnv_df.select("case_id", "gene_id").collect()

    stats["ssm_cases"] = frozenset({r.case_id for r in maf_data})
    stats["cnv_cases"] = frozenset({r.case_id for r in gistic_data})

    stats["ssm_genes"] = frozenset({r.gene_id for r in maf_data})
    stats["cnv_genes"] = frozenset({r.gene_id for r in gistic_data})

    return stats


def get_case_centric_stats(
    maf_df: sql.DataFrame, cnv_df: sql.DataFrame
) -> Dict[str, Any]:
    """
    case{}
            |___ gene[]
                    |___ ssm[]
                    |     |___ consequence[]
                    |     |             |_____ transcript{}
                    |     |                          |_____ annotation{}
                    |     |___ observation[]
                    |
                    |___ cnv[]
                        |___ consequence[]
                        |            |_____ gene{}
                        |
                        |___ observation[]
    """
    # Number of cases in maf_df and cnv_df
    count = (
        (maf_df.select("case_id").union(cnv_df.select("case_id"))).distinct().count()
    )
    stats = {"count": count}
    generic_stats = get_generic_stats(maf_df, cnv_df)

    stats.update(generic_stats)

    return stats


@pytest.mark.usefixtures(
    "sqlContext",
    "maf_df",
    "cnv_df",
    "case_centric_df",
    "ssm_transcript_df",
    "consequence_builder",
    "observation_builder",
)
class TestCaseCentricJoins(base_joins_test.BaseJoinsTest):
    """
    Test case_centric index joins

        case{}
        |___gene[]
            |___ssm[]
                |___consequence[]
                    |___transcript{}
                        |___annotation{}
                |___observation[]

    """

    def test_genes_per_case(
        self,
        maf_df: sql.DataFrame,
        cnv_df: sql.DataFrame,
        case_centric_df: sql.DataFrame,
    ) -> None:
        # Genes per case built:
        df = self.unpack_df_list(case_centric_df, "case_id", "gene", "gene_id")
        gpc = self.get_relationship_map(df, "case_id", "gene_id")

        # Genes per case expected:
        df = (
            maf_df.select("case_id", "gene_id").union(
                cnv_df.select("case_id", "gene_id")
            )
        ).distinct()
        true_gpc = self.get_relationship_map(df, "case_id", "gene_id")

        assert gpc == true_gpc

    def test_ssm_per_gene(
        self, maf_df: sql.DataFrame, case_centric_df: sql.DataFrame
    ) -> None:
        # SSMs per gene built:
        df = self.unpack_df_list(case_centric_df, "case_id", "gene", ["gene_id", "ssm"])
        df = self.unpack_df_list(df, ["case_id", "gene_id"], "ssm", "ssm_id")

        es_spg = {}
        for row in df.toJSON().collect():
            row = json.loads(row)
            es_spg.setdefault(row["case_id"], {})
            es_spg[row["case_id"]].setdefault(row["gene_id"], set())
            es_spg[row["case_id"]][row["gene_id"]].update({row["ssm_id"]})

        # SSMs per gene expected:
        spg = {}
        df = maf_df.select("gene_id", "case_id", "ssm_id")
        for row in df.toJSON().collect():
            row = json.loads(row)
            spg.setdefault(row["case_id"], {})
            spg[row["case_id"]].setdefault(row["gene_id"], set())
            spg[row["case_id"]][row["gene_id"]].update({row["ssm_id"]})

        assert es_spg == spg

    @pytest.mark.case_centric_ssm_subtree
    def test_ssm_subtree(
        self,
        consequence_builder: builders.ConsequenceBuilder,
        observation_builder: builders.ObservationBuilder,
        maf_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
        case_ssm_subtree: sql.DataFrame,
    ) -> None:
        def get_stats(dataframe):
            """
            Extracts ssm, consequence, transcript relationships from a flat dataframe
            """
            res = {}
            for row in dataframe.toJSON().collect():
                row = json.loads(row)
                sid = row["ssm_id"]
                oid = row["observation_id"]
                cid = row["consequence_id"]

                res.setdefault(sid, {"consequences": set(), "observations": set()})
                res[sid]["consequences"].update([cid])
                res[sid]["observations"].update([oid])

            return res

        # ssm_subtree stats expected:
        cons_df = consequence_builder.build_for_ssm(maf_df, "case_centric")
        obs_df = observation_builder.build_for_ssm(
            maf_df, primary_aliquot_df, "case_centric", selector="ssm"
        )

        df = cons_df.join(obs_df, on=["ssm_id"], how="left")

        df = self.unpack_df_list(
            df, ["ssm_id", "observation"], "consequence", "consequence_id"
        )
        df = self.unpack_df_list(
            df, ["ssm_id", "consequence_id"], "observation", "observation_id"
        )
        stats = get_stats(df)

        # ssm_subtree stats built:
        df = self.unpack_df_list(
            case_ssm_subtree, [], "ssm", ["ssm_id", "consequence", "observation"]
        )
        df = self.unpack_df_list(
            df, ["ssm_id", "observation"], "consequence", ["consequence_id"]
        )
        df = self.unpack_df_list(
            df, ["ssm_id", "consequence_id"], "observation", ["observation_id"]
        )
        es_stats = get_stats(df)

        assert stats == es_stats


@pytest.mark.usefixtures(
    "maf_df",
    "cnv_df",
    "all_cases",
    "all_maf_cases",
    "case_centric_df",
)
class TestCaseCentricOther:
    """Other case centric tests"""

    def test_empty_cases(
        self,
        case_centric_df: sql.DataFrame,
        maf_df: sql.DataFrame,
        cnv_df: sql.DataFrame,
        all_cases: Set[str],
        all_maf_cases: Set[str],
    ) -> None:
        """
        Test "empty cases"

        Confirm that we index cases even if they have no maf or cnv data.
        This is necessary for the portal to visualize such cases.
        """
        cases_built = {c.case_id for c in case_centric_df.collect()}
        stats = get_case_centric_stats(maf_df, cnv_df)

        empty_cases = {
            c
            for c in all_cases
            if c not in all_maf_cases and c not in stats["cnv_cases"]
        }

        # confirm that we have at least one empty case in our test data
        assert empty_cases

        # check that all cases were built (even empty ones)
        assert all_cases - cases_built == set()

    def test_available_variation_data(
        self,
        case_centric_df: sql.DataFrame,
        cnv_df: sql.DataFrame,
        all_cases: Set[str],
        all_maf_cases: Set[str],
    ) -> None:
        """
        Test that available_variation_data is correctly populated:
            * ['cnv'] - for cnv-only cases
            * ['ssm'] - for cases in the maf header that do not have cnv data
            * ['cnv', 'ssm'] - for cases that have both maf and cnv data
            * [] - for cases that have no maf or cnv data
        """

        assert "available_variation_data" in case_centric_df.columns

        gistic_cases = {r.case_id for r in cnv_df.collect()}
        maf_cases = all_maf_cases  # includes cases in maf header without ssms

        common_cases = gistic_cases & maf_cases
        cnv_cases = gistic_cases - maf_cases
        ssm_cases = maf_cases - gistic_cases
        empty_cases = all_cases - gistic_cases - maf_cases

        assert common_cases, "there were no common cases found in test data"
        assert cnv_cases, "there were no cnv cases found in test data"
        assert ssm_cases, "there were no ssm cases found in test data"
        assert empty_cases, "there were no empty cases found in test data"

        # Check that 'available_variation_data' is populated correctly
        for row in case_centric_df.collect():
            if row.case_id in common_cases:
                assert row.available_variation_data == ["cnv", "ssm"]
            elif row.case_id in cnv_cases:
                assert row.available_variation_data == ["cnv"]
            elif row.case_id in ssm_cases:
                assert row.available_variation_data == ["ssm"]
            elif row.case_id in empty_cases:
                assert row.available_variation_data == []

    @pytest.mark.parametrize(
        "path",
        [
            "case_id",
            "available_variation_data",
            "gene",
            "gene.ssm",
        ],
    )
    def test_case_centric_path_exists(
        self, case_centric_df: sql.DataFrame, path: str
    ) -> None:
        """
        Chosen paths that have to be present to merge branch
        """
        case_centric_df.select(path)


@pytest.mark.usefixtures("case_centric_df")
def test_case_centric_count(
    default_config: configuration.Configuration,
    es_client: elasticsearch.Elasticsearch,
    all_cases: Set[str],
) -> None:
    # NOTE: there may be cases without cnvs or ssms,
    # we need to ensure empty cases are counted as well
    # hence why we count cases differently than any other doc
    built_count = es_utils.get_es_doc_count(
        es_client,
        default_config.elasticsearch.write.indices[build.IndexType.CASE_CENTRIC],
    )

    assert built_count == len(all_cases)
