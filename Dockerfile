ARG BASE_VERSION=4.0.0
ARG REGISTRY=docker.osdc.io
ARG SERVICE_NAME=mutation_indexer
ARG PYTHON_VERSION=python3.13

FROM ${REGISTRY}/ncigdc/${PYTHON_VERSION}-builder:${BASE_VERSION} AS build
ARG SERVICE_NAME
ARG PIP_INDEX_URL=https://nexus.osdc.io/repository/pypi-gdc-releases/simple
ARG PYTHON_VERSION

# avoids use of detached head while computing versions in gitlab
ARG GIT_BRANCH_NAME
ENV CI_COMMIT_REF_NAME=$GIT_BRANCH_NAME \
    PIP_INDEX_URL=$PIP_INDEX_URL

WORKDIR /${SERVICE_NAME}
COPY . .

# confirm the version number is expected and does not include +dirty
# this is due to the COPY . . that might be missing some file entries
# due to .dockerignore.
RUN uv run -m setuptools_scm

RUN dnf install -y maven
RUN mvn process-sources \
    --file mutation_indexer_deps.pom.xml \
    -Dgdc.mutationIndexer.artifactDir=/spark/jars \
    -Dgdc.mutationIndexer.scalaVersion=2.12;

RUN uv run --script bin/build.py --output /spark
RUN uv pip install --target /spark/.venv '.[client]'

FROM ${REGISTRY}/ncigdc/${PYTHON_VERSION}:${BASE_VERSION}
ARG NAME
ARG PYTHON_VERSION
ARG SERVICE_NAME
ARG GIT_BRANCH
ARG COMMIT
ARG BUILD_DATE

LABEL org.opencontainers.image.title="${SERVICE_NAME}" \
    org.opencontainers.image.description="Service for performing ETL operations for managing research data." \
    org.opencontainers.image.source="https://github.com/NCI-GDC/${SERVICE_NAME}" \
    org.opencontainers.image.vendor="NCI GDC" \
    org.opencontainers.image.ref.name="${SERVICE_NAME}:${GIT_BRANCH}" \
    org.opencontainers.image.revision="${COMMIT}" \
    org.opencontainers.image.created="${BUILD_DATE}"

RUN dnf install -y shadow-utils java-11-amazon-corretto
RUN useradd \
    --create-home \
    --user-group \
    --home-dir /spark \
    --comment "Default user for running spark applications." \
    spark;

# Pick up the installed artifacts from the previous build stage.
COPY --chown=spark:spark --from=build /spark /spark

USER spark:spark
WORKDIR /spark
ENTRYPOINT ["uv", "run", "mutation-indexer"]
