# GDC Mutation Index Export
[![Build Status](https://travis-ci.com/NCI-GDC/gdc-mutation-indexer.svg?token=KcVLPFGKP2xmZvLzN1nQ&branch=master)](https://travis-ci.com/NCI-GDC/gdc-mutation-indexer)

Backend for exporting mutation indices for visualization on the GDC

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

## Contributing

Read how to contribute [here](https://github.com/NCI-GDC/gdcapi/blob/master/CONTRIBUTING.md)
