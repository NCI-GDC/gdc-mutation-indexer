ARG BASE_VERSION=4.2.0
ARG REGISTRY=docker.osdc.io/ncigdc
ARG PYTHON_VERSION=python3.13
ARG SERVICE_NAME=mutation-indexer

FROM ${REGISTRY}/${PYTHON_VERSION}-builder:${BASE_VERSION} AS build
ARG SERVICE_NAME
ENV UV_PROJECT_ENVIRONMENT="/venv"
ENV UV_LOCKED="1"
ENV UV_NO_DEV="1"
ENV UV_NO_EDITABLE="1"

WORKDIR /${SERVICE_NAME}
COPY . .

# MAKE MAIN CLIENT /venv
RUN uv sync --extra client

# MAKE DRIVER ZIPAPPS
# GENE EXPRESSION
RUN uv export --format pylock.toml --extra gene-expression --output-file pylock.gene-expression.toml
RUN uv pip install --requirements pylock.gene-expression.toml --target gene-expression
RUN uv run -m zipapp \
    gene-expression \
    --main mutation_indexer.gene_expression.driver:main \
    --output gene-expression.pyz \
    --python "/usr/bin/env -S uv run";

# VIZ
RUN uv export --format pylock.toml --extra viz --output-file pylock.viz.toml
RUN uv pip install --requirements pylock.viz.toml --target viz
RUN uv run -m zipapp \
    viz \
    --main mutation_indexer.viz.driver:main \
    --output viz.pyz \
    --python "/usr/bin/env -S uv run";

# INSTALL SCALA DEPENDENCIES
RUN dnf install -y java-11-amazon-corretto maven
RUN mvn -s ./maven-settings.xml \
    -f ./mutation_indexer_deps.pom.xml \
    dependency:copy-dependencies \
    -DoutputDirectory=jars;

FROM ${REGISTRY}/${PYTHON_VERSION}:${BASE_VERSION}
ARG BUILD_DATE
ARG COMMIT
ARG GIT_BRANCH
ARG SERVICE_NAME

LABEL org.opencontainers.image.title="${SERVICE_NAME}" \
  org.opencontainers.image.description="An application for building & indexing elasticsearch data." \
  org.opencontainers.image.source="https://github.com/NCI-GDC/${SERVICE_NAME}" \
  org.opencontainers.image.vendor="NCI GDC" \
  org.opencontainers.image.ref.name="${SERVICE_NAME}:${GIT_BRANCH}" \
  org.opencontainers.image.revision="${COMMIT}" \
  org.opencontainers.image.version="${COMMIT}" \
  org.opencontainers.image.created="${BUILD_DATE}"

COPY --from=build --chown=app:app /venv /venv
COPY --from=build --chown=app:app /${SERVICE_NAME}/gene-expression.pyz /app/gene-expression.py
COPY --from=build --chown=app:app /${SERVICE_NAME}/viz.pyz /app/viz.py
COPY --from=build --chown=app:app /${SERVICE_NAME}/jars /app/jars

RUN dnf install -y java-11-amazon-corretto openssh-clients

USER app:app
WORKDIR /app
ENTRYPOINT ["/venv/bin/python", "-m", "mutation_indexer.client"]
