#!/bin/bash

if [[ -z "${SPARK_HOME}" ]]
then
    SPARK_HOME=$(python -m pyspark.find_spark_home 2> /dev/null)
fi

echo SPARK_HOME: $SPARK_HOME
mvn process-sources \
    --settings maven-settings.xml \
    --file pom.xml \
    -Dgdc.mutationIndexer.artifactDir=$SPARK_HOME/jars;
