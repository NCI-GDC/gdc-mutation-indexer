# GDC Mutation Index Export
[![Build Status](https://travis-ci.com/NCI-GDC/gdc-mutation-indexer.svg?token=KcVLPFGKP2xmZvLzN1nQ&branch=master)](https://travis-ci.com/NCI-GDC/gdc-mutation-indexer)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)

Backend for exporting mutation indices for visualization on the GDC

- [GDC Mutation Index Export](#gdc-mutation-index-export)
  - [Architecture](#architecture)
  - [Make the docs](#make-the-docs)
  - [Tests](#tests)
  - [Vagrant](#vagrant)
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

Tests depend on `$PYTHONPATH` being configured correctly to find the spark 
python modules. Make sure the paths are correct in `bin/run-tests.sh`.

```
bin/run-tests.sh
```

The mutation indexer is currently deployed with Spark 2.4.3.
If you try to run the tests on a different version, you may need to update
`bin/run-tests.sh` to refer to the specific Py4J build included with your
Spark distribution.



## Vagrant

Testing locally can be hard and `vagrant` support has been added to make our lives
a little bit easier. Make sure to have `vagrant` and `VirtualBox` installed, then
simply do and start making coffee, it's gonna take a while:
```
vagrant up
```

This will spin up a VM and run necessary setup steps like:
* installing some core libs like `jdk`, `python-pip` etc
* downloading and setting up `pyspark`, `elasticsearch` and related plugins
* setting up development environment

The tests should be ran from within the box:

```
vagrant ssh
source venv/bin/activate
pytest /vagrant/tests
```

To get a better understanding of how to tweak/customize provisioning steps read
the docs! Have fun testing.

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


## Contributing

Read how to contribute [here](https://github.com/NCI-GDC/gdcapi/blob/master/CONTRIBUTING.md)
