# syntax=docker/dockerfile:1.0-experimental

ARG base_version=1.0.1
ARG registry=quay.io

# FROM ${registry}/ncigdc/python27-builder:${base_version} as build
FROM python:2.7.18-stretch AS builder
SHELL ["/bin/bash", "-c"]

ADD https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz /root/
RUN tar -xf /root/spark-2.4.5-bin-hadoop2.7.tgz -C /root/

RUN mkdir -p /app/artifacts/jars /app/artifacts/python_modules /app/artifacts/eggs 

COPY mutation_indexer_deps.pom.xml requirements.txt master-requirements.txt /app/

RUN apt-get update && \
    apt-get install -y maven && \
    rm -rf /var/lib/apt/lists/* && \
    mvn process-sources \
        -f /app/mutation_indexer_deps.pom.xml \
        -Dgdc.mutationIndexer.artifactDir="/app/artifacts/jars" \
        -Dgdc.mutationIndexer.scalaVersion=2.11

RUN mkdir /root/.ssh && \
    ssh-keyscan -t rsa github.com >> /root/.ssh/known_hosts

RUN --mount=type=ssh git clone --depth 1 --branch 1.12.0 git@github.com:NCI-GDC/tungsten.git /root/tungsten && \
                     mkdir -p /etc/hadoop/conf && \
                     cp -r /root/tungsten/salt/srv/services/mutation_indexer/conf/hadoop/* \
                           /etc/hadoop/conf/ 

WORKDIR /root 
ENV VIRTUAL_ENV=/opt/venv
RUN virtualenv ${VIRTUAL_ENV}
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"
RUN --mount=type=ssh pip download -r /app/requirements.txt \
                                  --dest /app/artifacts/python_modules \
                                  --src /app/artifacts/python_modules \
                                  --no-binary :all: && \
                     pip install --no-cache-dir -r /app/requirements.txt && \
                     pip install --no-cache-dir -r /app/master-requirements.txt

WORKDIR /app/artifacts/python_modules
RUN for ARCHIVE in *; do \
    if [ -f "$ARCHIVE" ] && [ "${ARCHIVE: -4}" == ".zip" ]; then \
        unzip -o "$ARCHIVE"; \
        rm "$ARCHIVE"; \
    elif [ -f "$ARCHIVE" ] && [ "${ARCHIVE: -7}" == ".tar.gz" ]; then \
        tar -xzf "$ARCHIVE"; \
        rm "$ARCHIVE"; \
    fi ;\
done && \
for DIR in *; do \
    pushd "$DIR"; \
    # Building the egg works differently depending on if the package uses distutils or setuptools
    python setup.py bdist_egg || python -c "import setuptools; execfile('setup.py')" bdist_egg; \
    mv dist/*.egg ../../eggs/; \
    popd; \
done

COPY . /app
WORKDIR /app
RUN python setup.py bdist_egg || python -c "import setuptools; execfile('setup.py')" bdist_egg


FROM python:2.7.18-stretch
COPY --from=builder /root/spark-2.4.5-bin-hadoop2.7 /root/spark-2.4.5-bin-hadoop2.7
COPY --from=builder /app /app
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /etc/hadoop/conf /etc/hadoop/conf
RUN apt-get update && \
    apt-get install -y openjdk-8-jre-headless && \
    rm -rf /var/lib/apt/lists/*
