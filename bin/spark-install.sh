#!/bin/bash

if [[ -z "${SPARK_HOME}" ]]
then
    SPARK_HOME=$(python -m pyspark.find_spark_home 2> /dev/null)
fi

echo SPARK_HOME: $SPARK_HOME
mvn -s ./maven-settings.xml -f ./mutation_indexer_deps.pom.xml dependency:copy-dependencies -DoutputDirectory=$SPARK_HOME/jars
