# GDC Mutation Index Export

> [!NOTE]
> The code in this repository has been made public as-is for informational purposes. The repository may use private resources for the building and execution of the code. For example, private registries may be used for dependency resolution. 
>
> The documentation may refer to restricted URLs.

[Build Status](https://gitlab.datacommons.io/nci-gdc/development/gdc-mutation-indexer)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)

Backend for exporting mutation indices for visualization on the GDC

### Contents
- [Architecture](#architecture)
- [Style Guide](#style-guide)
- [Pre-Commit](#setup-pre-commit-hook-to-check-for-secrets)
- [Updating Models](#update-gdc-models)
- [Tests](tests/README.md)
  - [Running Tests](tests/README.md#running-tests)
  - [Elasticsearch](tests/README.md#elasticsearch)
  - [Java](tests/README.md#java)
  - [Tools](tests/README.md#tools)
    - [Integration Test Data](tests/integration/data/README.md)
    - [Unit Test Models](tests/unit/data/models/README.md)
    - [Unit Test Schemas](tests/unit/data/schemas/README.md)

## Architecture
![Indexer Architecture](https://user-images.githubusercontent.com/68259544/201140691-64d64079-ef62-4ee9-ac0f-5b16388dd8cd.png)

Mutation indexer is an ETL platform leveraging Spark/Pyspark. It combines data from data
derived from the GDC graph (via the graph indices), static data (e.g. gene model), and
data contained in analysis files (e.g. MAF and ASCAT files) in order to create
structured data which can be used for visualization and further analysis.

## Style Guide

Mutation Indexer strives to follow the best practices set forth here: [pyspark-style-guide](https://github.com/palantir/pyspark-style-guide?tab=readme-ov-file#pyspark-style-guide)

## Pre-Commit
We use [pre-commit](https://pre-commit.com/) to setup pre-commit hooks for this repo.
We use [detect-secrets](https://github.com/Yelp/detect-secrets) to search for secrets being committed into the repo.

To install the pre-commit hook, run
```
pre-commit install
```

To update the .secrets.baseline file run
```
detect-secrets scan --update .secrets.baseline
git add .secrets.baseline
```

`.secrets.baseline` contains all the string that were caught by detect-secrets but are not stored in plain text. Audit the baseline to view the secrets .

```
detect-secrets audit .secrets.baseline
```

## Update GDC Models
After the dictionary is updated, any changes to the case structures in the various indices should be reflected in a new version of the gdc-models package. In order to ensure that Mutation Indexer is building these new mappings, we need to update it with the latest version. We also need to ensure that our test data continues to reflect the expected inputs from the graph/case index and that derived components of the viz mappings are built accordingly. To do this, we have several scripts within the Mutation Indexer test suite which ease this process and an orchestration script which should generally handle most updates. This said, its is encouraged for users to explore these test scripts and their individual functionality as they can be helpful in other stages of development in Mutation Indexer; please see [tools](tests/README.md#tools). Below is the command for running the orchestration script for general updates to the models.

Command:
```bash
# Make sure you have sourced your venv!
bin/update_models.sh
```
