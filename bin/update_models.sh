#!/usr/bin/env bash

set -e

export PYTHONPATH="src"

# UPDATE MODELS PACKAGE
pip install pip-tools
pip-compile --all-extras --index-url=https://nexus.osdc.io/repository/pypi-all/simple --strip-extras -Pgdcmodels
pip install -r requirements.txt

# REMOVE VESTIGIAL PROPERTIES FROM INTEGRATION TEST DATA.
python -m tests.integration.data remove-vestigial

# UPDATE INPUT GRAPH_CASE SCHEMAS FOR UNIT TESTS.
python -m tests.unit.data.schemas sync-case

# CREATE UPDATED MODELS FOR CASE AND CASE CENTRIC BUILDERS.
python -m tests.unit.data.models create-model \
    -n Case \
    -s tests/unit/data/schemas/viz/builders/case/raw.yaml \
    -o tests/unit/viz/builders/case/inputs/raw.py \
    -d tests/unit/data/models/defaults.yaml \
    --include-asserts
python -m tests.unit.data.models create-model \
    -n Case \
    -s tests/unit/data/schemas/viz/builders/case_centric/case.yaml \
    -o tests/unit/viz/builders/case_centric/inputs/case.py \
    -d tests/unit/data/models/defaults.yaml \
    --include-asserts

# UPDATE CASE BUILDER'S FINAL OUTPUT & ITS ASSOCIATED MODEL.
tox -etest -- --update-schemas \
    tests/unit/viz/builders/case/test_builder.py::TestCaseBuilder::test__build__single_row
python -m tests.unit.data.models create-model \
    -n Case \
    -s tests/unit/data/schemas/viz/builders/case/final.yaml \
    -o tests/unit/data/models/viz/case.py \
    -d tests/unit/data/models/defaults.yaml

# UPDATE DOWNSTREAM BUILDERS OF CASE BUILDER.
tox -etest -- --update-schemas -m case_schema_dependent
