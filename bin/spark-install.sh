#!/bin/bash

if [[ -z "${SPARK_HOME}" ]]
then
    SPARK_HOME=$(python -m pyspark.find_spark_home 2> /dev/null)
fi

echo SPARK_HOME: $SPARK_HOME
mvn dependency:copy-dependencies \
    --settings=maven-settings.xml \
    -DoutputDirectory=$SPARK_HOME/jars;
