ARG BASE_VERSION=4.11.0
ARG REGISTRY=docker.osdc.io/ncigdc
ARG PYTHON_VERSION=python3.13
ARG SERVICE_NAME=gdc-mutation-indexer

FROM ${REGISTRY}/${PYTHON_VERSION}-builder:${BASE_VERSION} AS build
ARG SERVICE_NAME
ENV UV_FROZEN=1
ENV UV_NO_DEV=1
ENV UV_NO_EDITABLE=1

WORKDIR /app
COPY . .

RUN dnf install -y java-17-amazon-corretto-devel maven
RUN mvn process-sources --settings=maven-settings.xml

RUN uv sync --extra client
RUN uv tool install setuptools-scm
RUN bin/generate-scripts.sh gene-expression viz

FROM ${REGISTRY}/${PYTHON_VERSION}:${BASE_VERSION}
ARG BUILD_DATE
ARG COMMIT
ARG GIT_BRANCH
ARG SERVICE_NAME
ARG PYTHON_VERSION
ENV GIT_COMMIT=${COMMIT}

ENV HADOOP_CONF_DIR=/conf/hadoop
ENV HADOOP_USER_NAME=spark
ENV JAVA_HOME=/usr/lib/jvm/jre-17-openjdk

LABEL org.opencontainers.image.title="${SERVICE_NAME}"
LABEL org.opencontainers.image.description="TODO"
LABEL org.opencontainers.image.source="https://github.com/NCI-GDC/${SERVICE_NAME}"
LABEL org.opencontainers.image.vendor="NCI GDC"
LABEL org.opencontainers.image.ref.name="${SERVICE_NAME}:${GIT_BRANCH}"
LABEL org.opencontainers.image.revision="${COMMIT}"
LABEL org.opencontainers.image.version="${COMMIT}"
LABEL org.opencontainers.image.created="${BUILD_DATE}"

RUN dnf install -y java-17-amazon-corretto-headless

WORKDIR /app

COPY --from=build --chown=app:app /app/jars .
COPY --from=build --chown=app:app /app/.venv .
COPY --from=build --chown=app:app /app/gene-expression.py .
COPY --from=build --chown=app:app /app/viz.py .

USER app:app
ENTRYPOINT ["uv", "run", "-m", "mutation_indexer.client"]
