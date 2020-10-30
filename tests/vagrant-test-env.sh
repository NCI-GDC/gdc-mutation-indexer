export SPARK_VERSION=2.4.5
export ES_VERSION=7.6.2
export BOTO_CONFIG=/dev/null
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH="${JAVA_HOME}/bin:${PATH}"
export SPARK_HOME=/home/vagrant/spark-${SPARK_VERSION}-bin-hadoop2.7;
export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH
