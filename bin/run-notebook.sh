export SPARK_HOME=~/spark
export ELASTICSEARCH_JAR=~/spark/elasticsearch-hadoop-5.0.0/dist/elasticsearch-spark-20_2.11-5.0.0.jar

export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.open_browser=False --NotebookApp.ip='localhost' --NotebookApp.port=9099"
export PYSPARK_PYTHON=python

exec $SPARK_HOME/bin/pyspark --jars $ELASTICSEARCH_JAR
