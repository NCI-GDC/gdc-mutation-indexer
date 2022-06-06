import os
import tempfile

import pytest

import config
import master
import parsers

REQUIRED_ARGUMENTS = [
    "--s3-host",
    "fake-s3",
    "--s3-access-key",
    "fake-key",
    "--s3-secret-key",
    "fake-key",
    "--indexd-user",
    "fake-user",
    "--indexd-pass",
    "fake-pass",
]

EXPECTED_PARTIAL_SPARK_ARGS = {
    "yarn",
    "spark.driver.maxResultSize=1g",
    "40g",
    "--executor-memory",
    "--deploy-mode",
    "--master",
    "spark.sql.caseSensitive=True",
    "--executor-cores",
    "--conf",
    "--driver-memory",
    "--num-executors",
    "--jars",
    "25",
    "cluster",
    "spark.sql.shuffle.partitions=1024",
    "spark.sql.autoBroadcastJoinThreshold=-1",
    "8",
    "12g",
    "--name",
}

EXPECTED_PARTIAL_CONFIG_ARGS = {
    "spark.executorEnv.EXECUTOR_MEMORY",
    "spark.yarn.appMasterEnv.ES_NODES",
    "spark.executorEnv.BATCH_SIZE_BYTES",
    "spark.yarn.appMasterEnv.PIPELINES",
    "spark.yarn.appMasterEnv.DF_COALESCE",
    "spark.yarn.appMasterEnv.S3_HOST",
    "spark.yarn.appMasterEnv.INDEX_TYPES",
    "spark.yarn.appMasterEnv.SOURCE_ES_NODES",
    "spark.executorEnv.NAME",
    "spark.yarn.appMasterEnv.PROJECTS",
    "spark.executorEnv.SKIP_ES_MAFS",
    "spark.yarn.appMasterEnv.BUILD_TYPE",
    "spark.yarn.appMasterEnv.S3_SECRET_KEY",
    "spark.executorEnv.BUILD_TYPE",
    "spark.yarn.appMasterEnv.SOURCE_ES_USER",
    "spark.executorEnv.GENE_EXPRESSION_VALUES_BACKUP",
    "spark.executorEnv.INDEXD_PORT",
    "spark.executorEnv.SOURCE_ES_PASS",
    "spark.executorEnv.ES_USER",
    "spark.yarn.appMasterEnv.SKIP_ES_MAFS",
    "spark.yarn.appMasterEnv.BUILD_LABEL",
    "spark.executorEnv.GRAPH_FILE_INDEX",
    "spark.executorEnv.OUTPUT_RAW",
    "spark.executorEnv.BUILD_VERSION",
    "spark.executorEnv.SOURCE_ES_NODES",
    "spark.yarn.appMasterEnv.GENE_EXPRESSION_CASES_BACKUP",
    "spark.executorEnv.INDEXD_PASS",
    "spark.executorEnv.INCLUDE_MAF_URLS",
    "spark.executorEnv.S3_ACCESS_KEY",
    "spark.executorEnv.GRAPH_CASE_INDEX",
    "spark.executorEnv.BUILD_LABEL",
    "spark.executorEnv.ES_NODES",
    "spark.yarn.appMasterEnv.STUDY_LABEL",
    "spark.yarn.appMasterEnv.BATCH_SIZE_BYTES",
    "spark.yarn.appMasterEnv.S3_RAW_BUCKET",
    "spark.yarn.appMasterEnv.OLD_GRAPH_INDEX",
    "spark.executorEnv.DF_COALESCE",
    "spark.yarn.appMasterEnv.NUM_EXECUTORS",
    "spark.executorEnv.INDEXD_HOST",
    "spark.executorEnv.S3_SECRET_KEY",
    "spark.executorEnv.SOURCE_ES_USER",
    "spark.yarn.appMasterEnv.ES_USE_SSL",
    "spark.yarn.appMasterEnv.GISTIC_BACKUP",
    "spark.yarn.appMasterEnv.MAF_BACKUP",
    "spark.executorEnv.S3_GISTIC_BUCKET",
    "spark.yarn.appMasterEnv.BLACKLIST_FIELDS",
    "spark.executorEnv.DRIVER_MEMORY",
    "spark.executorEnv.EXECUTOR_CORES",
    "spark.yarn.appMasterEnv.S3_MAF_BUCKET",
    "spark.executorEnv.DF_REPARTITION",
    "spark.executorEnv.DEBUG",
    "spark.yarn.appMasterEnv.GENE_EXPRESSION_VALUES_BACKUP",
    "spark.executorEnv.OLD_GRAPH_INDEX",
    "spark.yarn.appMasterEnv.SKIP_NORMALIZATION",
    "spark.executorEnv.STUDY_LABEL",
    "spark.yarn.appMasterEnv.DF_REPARTITION",
    "spark.executorEnv.GISTIC_BACKUP",
    "spark.executorEnv.BLACKLIST_FIELDS",
    "spark.executorEnv.ES_PASS",
    "spark.yarn.appMasterEnv.S3_GISTIC_BUCKET",
    "spark.executorEnv.NUM_EXECUTORS",
    "spark.yarn.appMasterEnv.EXECUTOR_CORES",
    "spark.yarn.appMasterEnv.GRAPH_CASE_INDEX",
    "spark.executorEnv.PROJECTS",
    "spark.yarn.appMasterEnv.OUTPUT_RAW",
    "spark.executorEnv.DISABLE_ES_VERIFY_CERTS",
    "spark.executorEnv.S3_MAF_BUCKET",
    "spark.yarn.appMasterEnv.INCLUDE_MAF_URLS",
    "spark.yarn.appMasterEnv.GRAPH_FILE_INDEX",
    "spark.executorEnv.GENE_EXPRESSION_CASES_BACKUP",
    "spark.yarn.appMasterEnv.ES_PASS",
    "spark.yarn.appMasterEnv.MASTER",
    "spark.executorEnv.DEPLOY_MODE",
    "spark.executorEnv.MASTER",
    "spark.yarn.appMasterEnv.DISABLE_ES_VERIFY_CERTS",
    "spark.executorEnv.INDEX_TYPES",
    "spark.yarn.appMasterEnv.BUILD_VERSION",
    "spark.yarn.appMasterEnv.DEBUG",
    "spark.yarn.appMasterEnv.EXECUTOR_MEMORY",
    "spark.yarn.appMasterEnv.INDEXD_PORT",
    "spark.yarn.appMasterEnv.DEPLOY_MODE",
    "spark.yarn.appMasterEnv.ES_USER",
    "spark.executorEnv.ES_USE_SSL",
    "spark.executorEnv.S3_HOST",
    "spark.yarn.appMasterEnv.INDEXD_PASS",
    "spark.yarn.appMasterEnv.DRIVER_MEMORY",
    "spark.executorEnv.PIPELINES",
    "--conf",
    "spark.executorEnv.MAF_BACKUP",
    "spark.yarn.appMasterEnv.INDEXD_USER",
    "spark.yarn.appMasterEnv.SOURCE_ES_PASS",
    "spark.executorEnv.BATCH_SIZE_ENTRIES",
    "spark.executorEnv.SKIP_NORMALIZATION",
    "spark.yarn.appMasterEnv.NAME",
    "spark.yarn.appMasterEnv.INDEXD_HOST",
    "spark.yarn.appMasterEnv.S3_ACCESS_KEY",
    "spark.executorEnv.S3_RAW_BUCKET",
    "spark.yarn.appMasterEnv.BATCH_SIZE_ENTRIES",
    "spark.executorEnv.INDEXD_USER",
}


@pytest.fixture()
def get_args():
    parser = parsers.ParserBuilder.build(
        config.ALL_PARSERS,
        description="Mutation Indexer",
    )
    args = parser.parse_args(REQUIRED_ARGUMENTS)
    return args


def test_get_spark_args(get_args, monkeypatch):
    tmp_dir = tempfile.mkdtemp()
    monkeypatch.setattr(config, "ROOT_DIR", tmp_dir)

    artifacts_dir = os.path.join(tmp_dir, "artifacts")
    jars_dir = os.path.join(artifacts_dir, "jars")
    os.mkdir(artifacts_dir)
    os.mkdir(jars_dir)
    spark_args = master.get_spark_args(get_args)
    assert EXPECTED_PARTIAL_SPARK_ARGS < set(spark_args)


def test_get_config_args(get_args):
    config_args = master.get_config_args(get_args)
    assert EXPECTED_PARTIAL_CONFIG_ARGS <= {
        arg.split("=")[0] for arg in set(config_args)
    }
