import csv
import logging
from typing import Iterable, Optional, Tuple

import pkg_resources
import yaml
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

from mutation_indexer.builders import utils
from mutation_indexer.builders.clinical_annotations import base
from mutation_indexer.configuration import adapter
from mutation_indexer.constants import app

logging.basicConfig(format=app.LOG_FORMAT)


class CivicBuilder(base.ClinicalAnnotationBuilder):
    """
    Class responsible for assembling CIVIC annotation into a single dataframe with
    uniform features
    """

    def __init__(
        self, config: adapter.ObsoleteConfig, sqlContext: sql.SQLContext
    ) -> None:
        super().__init__(config, sqlContext)
        self.sources, self.schema = self._get_resource()

    def merge_columns_by_name(
        self, df: sql.DataFrame, adding_fields: Iterable[str]
    ) -> sql.DataFrame:
        """
        Civic has multiple (two) files to be merged. Each of these two files contain different parts of information to be merged.
        This function will be called multiple times (one time for every file).
        :param df:
        :param adding_fields: fields to be added from a file
        :return:
        """

        def get_first_not_null(*cols):
            for col in cols:
                if col is not None:
                    return col
            return None

        for field in adding_fields:
            cols = ["{}_{}".format(field, k) for k in self.sources.keys()]
            df = df.withColumn(
                field, F.udf(get_first_not_null, types.StringType())(*cols)
            )
            df = df.drop(*cols)
        return df

    def merge_with_maf(self, maf_df: sql.DataFrame) -> sql.DataFrame:
        """
        Builds a CIVIC dataframe by merging with existing MAF and
        augmenting them with additional features
        """
        for k, v in self.sources.items():
            file_path = pkg_resources.resource_filename(
                pkg_resources.Requirement.parse("mutationindexerresource"),
                "clinical_variant_annotation/civic/{}".format(v),
            )
            try:
                # read Civic annotation from csv files and merge with existing dataframe
                with open(file_path) as f:
                    reader = csv.DictReader(f, delimiter="\t")
                    new_df = self.sqlContext.createDataFrame(
                        sql.Row(**d) for d in reader
                    )

                maf_df = self.standardize_schema_with_maf(new_df, maf_df, k)

            except Exception as e:
                self.logger.error(e)
                raise e

        adding_fields = [
            k for (k, v) in self.schema.items() if v.get("src_key") is None
        ]

        maf_df = self.merge_columns_by_name(maf_df, adding_fields)
        return maf_df

    def standardize_schema_with_maf(
        self,
        df: sql.DataFrame,
        maf_df: sql.DataFrame,
        dataset_key: Optional[str] = None,
    ) -> sql.DataFrame:
        """
        Renames and select required columns for Civic annotation
        """
        if dataset_key is None:
            default_to_none = []
        else:
            default_to_none = [
                v.get("src_field")
                for (k, v) in self.schema.items()
                if v.get("src_key") is not None and v.get("src_key") != dataset_key
            ]
        joining_fields = [
            k for (k, v) in self.schema.items() if v.get("src_key") == dataset_key
        ]

        df_columns = set(df.columns)

        # Map old columns to their new names as given in the schema.
        def standardize(new_column, props, dataset_key):
            old_column = props["src_field"]

            if old_column in df_columns:
                if not props.get("src_key"):
                    new_column = utils.get_column_name(new_column, dataset_key)
                return F.col(old_column).alias(new_column)
            elif old_column not in default_to_none:
                raise KeyError(
                    "Required column {} missing from Civic df with columns {}".format(
                        old_column, df_columns
                    )
                )

        df = df.select(
            *[
                standardize(k, v, dataset_key)
                for k, v in self.schema.items()
                if v.get("src_field", "") not in default_to_none
            ]
        )
        df = maf_df.join(df, joining_fields, "left")
        return df

    def _get_resource(self) -> Tuple[dict, dict]:
        # read Civic annotation from csv files into pandas dataset
        path = pkg_resources.resource_filename(
            "mutation_indexer.schemas.clinical_annotations", "civic.yml"
        )
        with open(path, "r") as f:
            s_yaml = yaml.safe_load(f)
            self.logger.info(s_yaml)
            return s_yaml.get("source"), s_yaml.get("schema")
