# syntax=docker/dockerfile:experimental
FROM bitnami/spark:2.4.5

# environment
ENV ES_NODES_TEST="elasticsearch" \
    ES_HOST_TEST="elasticsearch" \
    SOURCE_ES_HOST_TEST="elasticsearch" \
    ES_VERSION="7.6.2" \
    BOTO_CONFIG=/dev/null \
    PYTHONPATH="$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH" \
    PYSPARK_PYTHON=/usr/bin/python2

USER root

# core setup
COPY requirements.txt dev-requirements.txt /root/

RUN apt-get update && \
    apt-get install -y python-pip unzip git ssh && \
    apt-get clean && rm -rf /var/lib/apt

RUN --mount=type=ssh mkdir /root/.ssh && \
    ssh-keyscan -t rsa github.com >> /root/.ssh/known_hosts && \
    pip2 install -r /root/requirements.txt && \
    pip2 install -r /root/dev-requirements.txt

COPY . /app
RUN python2 /app/setup.py develop

# spark setup
ADD https://artifacts.elastic.co/downloads/elasticsearch-hadoop/elasticsearch-hadoop-${ES_VERSION}.zip \
    /root/
RUN unzip -j /root/elasticsearch-hadoop-${ES_VERSION}.zip \
            "elasticsearch-hadoop-7.6.2/dist/elasticsearch-spark-20_2.11-${ES_VERSION}.jar" \
            -d $SPARK_HOME/jars
