#!/bin/bash
unset http_proxy
unset https_proxy


EGGS=''
for EGG in artifacts/eggs/*; do
    EGGS+="$EGG,"
done
# strip the last ','
EGGS=${EGGS%?}

JARS=''
for JAR in artifacts/jars/*; do
    JARS+="$JAR,"
done
# strip the last ','
JARS=${JARS%?}

REVISION=$(git rev-parse HEAD)

python setup.py bdist_egg

exec $SPARK_HOME/bin/spark-submit \
    --name "GDC Mutation Indexer" \
    --master yarn \
    --deploy-mode cluster \
    --executor-memory ${EXEC_MEM:-40g} \
    --driver-memory ${DRIVER_MEM:-12g} \
    --executor-cores ${EXEC_CORES:-8} \
    --num-executors ${NB_EXEC:-25} \
    --py-files dist/gdc_mutation_indexer-0.1.0_rev_$REVISION-py2.7.egg,$EGGS \
    --jars $JARS \
    --conf spark.yarn.appMasterEnv.S3_HOST="$S3_HOST" \
    --conf spark.yarn.appMasterEnv.S3_ACCESS_KEY="$S3_ACCESS_KEY" \
    --conf spark.yarn.appMasterEnv.S3_SECRET_KEY="$S3_SECRET_KEY" \
    --conf spark.yarn.appMasterEnv.S3_BUCKET="$S3_BUCKET" \
    --conf spark.yarn.appMasterEnv.ES_HOST="$ES_HOST" \
    --conf spark.yarn.appMasterEnv.ES_PORT="${ES_PORT:-9200}" \
    --conf spark.yarn.appMasterEnv.ES_USER="$ES_USER" \
    --conf spark.yarn.appMasterEnv.ES_PASS="$ES_PASS" \
    --conf spark.yarn.appMasterEnv.ES_NODES="$ES_NODES" \
    --conf spark.yarn.appMasterEnv.ES_BATCH_SIZE_BYTES="$ES_BATCH_SIZE_BYTES" \
    --conf spark.yarn.appMasterEnv.ES_BATCH_SIZE_ENTRIES="$ES_BATCH_SIZE_ENTRIES" \
    --conf spark.yarn.appMasterEnv.SOURCE_ES_HOST="${SOURCE_ES_HOST:-$ES_HOST}" \
    --conf spark.yarn.appMasterEnv.SOURCE_ES_PORT="${SOURCE_ES_PORT:-$ES_PORT}" \
    --conf spark.yarn.appMasterEnv.SOURCE_ES_USER="${SOURCE_ES_USER:-$ES_USER}" \
    --conf spark.yarn.appMasterEnv.SOURCE_ES_PASS="${SOURCE_ES_PASS:-$ES_PASS}" \
    --conf spark.yarn.appMasterEnv.SOURCE_ES_INDEX="${SOURCE_ES_INDEX:-gdc_from_graph}" \
    --conf spark.yarn.appMasterEnv.SOURCE_ES_DOCUMENT="${SOURCE_ES_DOCUMENT:-case}" \
    --conf spark.yarn.appMasterEnv.MAF_KEYWORDS="$MAF_KEYWORDS" \
    --conf spark.yarn.appMasterEnv.PIPELINES="$PIPELINES" \
    --conf spark.yarn.appMasterEnv.PROJECTS="$PROJECTS" \
    --conf spark.yarn.appMasterEnv.NB_PROJECTS="${NB_PROJECTS:-0}" \
    --conf spark.yarn.appMasterEnv.INDEX_REPARTITION="$INDEX_REPARTITION" \
    --conf spark.yarn.appMasterEnv.INDEX_COALESCE="$INDEX_COALESCE" \
    --conf spark.executorEnv.S3_HOST="$S3_HOST" \
    --conf spark.executorEnv.S3_ACCESS_KEY="$S3_ACCESS_KEY" \
    --conf spark.executorEnv.S3_SECRET_KEY="$S3_SECRET_KEY" \
    --conf spark.executorEnv.S3_BUCKET="$S3_BUCKET" \
    --conf spark.executorEnv.ES_HOST="$ES_HOST" \
    --conf spark.executorEnv.ES_PORT="${ES_PORT:-9200}" \
    --conf spark.executorEnv.ES_USER="$ES_USER" \
    --conf spark.executorEnv.ES_PASS="$ES_PASS" \
    --conf spark.executorEnv.ES_NODES="$ES_NODES" \
    --conf spark.executorEnv.SOURCE_ES_HOST="${SOURCE_ES_HOST:-$ES_HOST}" \
    --conf spark.executorEnv.SOURCE_ES_PORT="${SOURCE_ES_PORT:-$ES_PORT}" \
    --conf spark.executorEnv.SOURCE_ES_USER="${SOURCE_ES_USER:-$ES_USER}" \
    --conf spark.executorEnv.SOURCE_ES_PASS="${SOURCE_ES_PASS:-$ES_PASS}" \
    --conf spark.executorEnv.SOURCE_ES_INDEX="${SOURCE_ES_INDEX:-gdc_from_graph}" \
    --conf spark.executorEnv.SOURCE_ES_DOCUMENT="${SOURCE_ES_DOCUMENT:-case}" \
    --conf spark.executorEnv.MAF_KEYWORDS="$MAF_KEYWORDS" \
    --conf spark.executorEnv.PIPELINES="$PIPELINES" \
    --conf spark.executorEnv.PROJECTS="$PROJECTS" \
    --conf spark.executorEnv.NB_PROJECTS="${NB_PROJECTS:-0}" \
    bin/export.py "$@"
