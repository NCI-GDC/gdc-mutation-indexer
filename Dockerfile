# syntax=docker/dockerfile:1.0-experimental

ARG base_version=1.0.1
ARG registry=quay.io

FROM ${registry}/ncigdc/python36-builder:${base_version} as build

COPY requirements.txt /root/

RUN --mount=type=ssh mkdir /root/.ssh && \
    ssh-keyscan -t rsa github.com >> /root/.ssh/known_hosts && \
    pip install -r /root/requirements.txt

COPY . /app
WORKDIR /app
RUN pip install --no-deps .

# spark setup
ARG ES_VERSION=7.6.2
ADD https://artifacts.elastic.co/downloads/elasticsearch-hadoop/elasticsearch-hadoop-${ES_VERSION}.zip \
    /root/
RUN unzip -j /root/elasticsearch-hadoop-${ES_VERSION}.zip \
            "elasticsearch-hadoop-7.6.2/dist/elasticsearch-spark-20_2.11-${ES_VERSION}.jar" \
            -d /usr/local/lib/python3.6/dist-packages/pyspark/jars


FROM ${registry}/ncigdc/python36-httpd:${base_version}

ENV ES_NODES_TEST="elasticsearch" \
    ES_HOST_TEST="elasticsearch" \
    SOURCE_ES_HOST_TEST="elasticsearch" \
    BOTO_CONFIG=/dev/nul

# Pick up the installed artifacts from the previous build stage.
COPY --from=build /app /app
COPY --from=build /src /src
COPY --from=build /usr/lib/python3 /usr/lib/python3
COPY --from=build /usr/local/bin /usr/local/bin
COPY --from=build /usr/local/lib/python3.6/dist-packages /usr/local/lib/python3.6/dist-packages
COPY --from=build /usr/local/share/py4j /usr/local/share/py4j

# install java
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update -y && \
    apt-get install -y openjdk-8-jdk && \
    rm -rf /var/lib/apt/lists/*
