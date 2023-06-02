# GDC Mutation Index Export
[![Build Status](https://travis-ci.com/NCI-GDC/gdc-mutation-indexer.svg?token=KcVLPFGKP2xmZvLzN1nQ&branch=master)](https://travis-ci.com/NCI-GDC/gdc-mutation-indexer)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)

Backend for exporting mutation indices for visualization on the GDC

### Contents
- [Architecture](#architecture)
- [Pre-Commit](#setup-pre-commit-hook-to-check-for-secrets)
- [Tests](tests/README.md)
  - [Running Tests](tests/README.md#running-tests)
  - [Elasticsearch](tests/README.md#elasticsearch)
  - [Java](tests/README.md#java)
  - [Tools](tests/README.md#tools)
    - [Models](tests/unit/data/models/README.md)
    - [Schemas](tests/unit/data/schemas/README.md)

## Architecture
![Indexer Architecture](https://user-images.githubusercontent.com/68259544/201140691-64d64079-ef62-4ee9-ac0f-5b16388dd8cd.png)

Mutation indexer is an ETL platform leveraging Spark/Pyspark. It combines data from data
derived from the GDC graph (via the graph indices), static data (e.g. gene model), and
data contained in analysis files (e.g. MAF and ASCAT files) in order to create
structured data which can be used for visualization and further analysis.

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
