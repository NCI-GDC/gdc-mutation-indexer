ARG SERVICE_NAME
ARG UV_INDEX

ARG BASE_VERSION=4.0.0
ARG PYTHON_VERSION=python3.13
ARG REGISTRY=docker.osdc.io
ARG UV_INDEX=https://nexus.osdc.io/repository/pypi-gdc-releases/simple

FROM ${REGISTRY}/ncigdc/${PYTHON_VERSION}-builder:${BASE_VERSION} AS build
ARG SERVICE_NAME
ARG UV_INDEX

# avoids use of detached head while computing versions in gitlab
ARG GIT_BRANCH_NAME
ENV CI_COMMIT_REF_NAME=${GIT_BRANCH_NAME} \
    UV_INDEX=${UV_INDEX}

WORKDIR /${SERVICE_NAME}
COPY . .

# confirm the version number is expected and does not include +dirty
# this is due to the COPY . . that might be missing some file entries
# due to .dockerignore.
RUN uvx --with versionista setuptools-scm

RUN dnf install -y maven
RUN mvn process-sources --settings maven-settings.xml --file pom.xml

RUN uv run --script bin/build.py --output /spark
RUN uv pip install --target /spark/.venv '.[client]'

FROM ${REGISTRY}/ncigdc/${PYTHON_VERSION}:${BASE_VERSION}
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
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

USER spark:spark
WORKDIR /spark
ENTRYPOINT ["uv", "run", "mutation-indexer"]
