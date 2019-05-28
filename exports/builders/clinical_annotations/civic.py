import yaml
import logging
import pandas as pd

from pyspark.sql.types import StringType
from pyspark.sql.functions import (
    lit, col, udf
)

from exports.builders.clinical_annotations.base import ClinicalAnnotationBuilder
from exports.builders.utils import remove_columns

from pkg_resources import resource_filename, Requirement

from config import LOG_FORMAT

logging.basicConfig(format=LOG_FORMAT)


class CivicBuilder(ClinicalAnnotationBuilder):
    """
    Class responsible for assembling CIVIC annotation into a single dataframe with
    uniform features
    """

    def __init__(self, config, sqlContext):
        super(CivicBuilder, self).__init__(config, sqlContext)
        self.sources, self.schema = self.get_resouce()

    def merge_columns_by_name(self, df, adding_fields):
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
            cols = [col('{}_{}'.format(field, k)) for k in self.sources.keys()]
            df = df.withColumn(field, udf(get_first_not_null, StringType())(*cols))
            df = remove_columns(df, *cols)
        return df

    def merge_with_maf(self, maf_df):
        """
        Builds a CIVIC dataframe by merging with existing MAF and
        augmenting them with additional features
        """
        for k, v in self.sources.items():
            file_path = resource_filename(Requirement.parse(
                'mutationindexerresource'), 'clinical_variant_annotation/civic/{}'.format(v)
            )
            try:
                # read Civic annotation from csv files into pandas dataset
                pd_df = pd.read_table(file_path)  # , sep='\t')
                new_df = self.sqlContext.createDataFrame(pd_df)
                maf_df = self.standardize_schema_with_maf(new_df, maf_df, k)

            except Exception as e:
                self.logger.error(e)

        adding_fields = [k for (k, v) in self.schema.items() if v.get('src_key') is None]

        maf_df = self.merge_columns_by_name(maf_df, adding_fields)
        return maf_df

    def standardize_schema_with_maf(self, df, maf_df, dataset_key=None):
        """
        Renames and select required columns for Civic annotation
        """
        if dataset_key is None:
            default_to_none = []
        else:
            default_to_none = [v.get('src_field') for (k, v) in self.schema.items()
                               if v.get('src_key') is not None and v.get('src_key') != dataset_key]
        joining_fields = [k for (k, v) in self.schema.items() if v.get('src_key') == dataset_key]

        df_columns = set(df.columns)

        # Map old columns to their new names as given in the schema.
        def standardize(new_column, props, dataset_key):
            old_column = props['src_field']

            if old_column in df_columns:
                if not props.get('src_key'):
                    new_column = '{}_{}'.format(new_column, dataset_key)
                return col(old_column).alias(new_column)
            elif old_column not in default_to_none:
                raise KeyError(
                    'Required column {} missing from Civic df with columns {}'.format(old_column, df_columns))

        df = df.select(*[standardize(k, v, dataset_key) for k, v in self.schema.items()
                         if v.get('src_field', '') not in default_to_none])
        df = maf_df.join(df, joining_fields, 'left')
        return df

    def get_resouce(self):
        # read Civic annotation from csv files into pandas dataset
        path = resource_filename('exports.schemas.clinical_annotations', 'civic.yml')
        with open(path, 'r') as f:
            s_yaml = yaml.load(f, Loader=yaml.SafeLoader)
            self.logger.info(s_yaml)
            return s_yaml.get('source'), s_yaml.get('schema')
