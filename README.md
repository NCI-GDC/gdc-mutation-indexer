# GDC Mutation Index Export
[![Build Status](https://travis-ci.com/NCI-GDC/gdc-mutation-indexer.svg?token=KcVLPFGKP2xmZvLzN1nQ&branch=master)](https://travis-ci.com/NCI-GDC/gdc-mutation-indexer)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)

Backend for exporting mutation indices for visualization on the GDC

- [GDC Mutation Index Export](#gdc-mutation-index-export)
  - [Architecture](#architecture)
  - [Make the docs](#make-the-docs)
  - [Tests](#tests)
  - [Setup pre-commit hook to check for secrets](#setup-pre-commit-hook-to-check-for-secrets)
  - [Contributing](#contributing)

## Architecture
![Indexer Architecture](architecture.png)

The mutation indexer combines mutation data from MAF analysis files and metadata
from the data model to create Elasticsearch indices that may be used for
visualization or further analysis.

## Make the docs

```
cd docs
make html
ghp-import build/html
```

## Tests
### ElasticSearch
Insure you have elasticsearch working on your device:
`service elasticsearch status`

In case you need to install elastic search:
#### Homebrew
```
brew install elasticsearch@7.6
/usr/local/Cellar/elasticsearch\@7.6/7.6.2/bin/elasticsearch-plugin install mapper-size
brew services start elasticsearch@7.6
```

#### Ubuntu/Debian bases systems
```
apt install elasticsearch=7.6.2
apt-mark hold elasticsearch
service elasticsearch start
```
Make sure your elasticsearch server is running at port 9200.

### Tox
Insure tox is install via pip or pipx
```
pipx install tox
```

To run tests:
```
tox -- path/to/test(s)
```



## Setup pre-commit hook to check for secrets

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

### Internal Reference
https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer

### TODO
- Expand background on purpose
- Provide instructions for how to use

## Contributing

Read how to contribute [here](https://github.com/NCI-GDC/gdcapi/blob/master/CONTRIBUTING.md)
