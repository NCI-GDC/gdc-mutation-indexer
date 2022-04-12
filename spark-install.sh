#!/bin/bash

if [[ -z "${SPARK_HOME}" ]]
then
    SPARK_HOME=$(python -m pyspark.find_spark_home 2> /dev/null)
fi

echo SPARK_HOME: $SPARK_HOME

if [ ! -f $SPARK_HOME/jars/elasticsearch-spark-20_2.11-$ES_VERSION.jar ]
then
    wget --progress=bar:force https://artifacts.elastic.co/downloads/elasticsearch-hadoop/elasticsearch-hadoop-$ES_VERSION.zip
    unzip elasticsearch-hadoop-$ES_VERSION.zip
    echo MOVING - elasticsearch-hadoop-$ES_VERSION/dist/elasticsearch-spark-20_2.11-$ES_VERSION.jar $SPARK_HOME/jars
    mv elasticsearch-hadoop-$ES_VERSION/dist/elasticsearch-spark-20_2.11-$ES_VERSION.jar $SPARK_HOME/jars 
    rm elasticsearch-hadoop-$ES_VERSION.zip
    rm -rf elasticsearch-hadoop-$ES_VERSION
fi
