[![Build Status](https://travis-ci.com/NCI-GDC/gdc-mutation-indexer.svg?token=KcVLPFGKP2xmZvLzN1nQ&branch=master)](https://travis-ci.com/NCI-GDC/gdc-mutation-indexer)
# GDC Mutation Index Export

Backend for exporting mutaiton indices for visualization on the GDC

## Dev Config

### Set up the environment

Set up a virtual environment by installing dependencies

```
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -r dev-requirements.txt
```

### Running a Developer Cluster

TODO: Write setup shell script for dev install

Versions:
`elasticsearch=5.0.0`
`spark=2.0.1-hadoop=2.7`

#### Spark

A spark cluster must exist to be run against. A standalone cluster on a single
node with a single master and slave is sufficient for developing against.
See the official insructions [instructions](http://spark.apache.org/docs/latest/spark-standalone.html).

#### Elasticsearch hadoop adapter

Install the elasticsearch hadoop adapter by downloading the
[zip](https://www.elastic.co/downloads/hadoop). Extract the contents and 
put the path to the `dist` folder inside the `$SPARK_HOME/conf/spark-defaults.sh`
as the `spark.driver.extraClassPath` variable. Make sure to also set the
`--jars` flag in `bin/run-notebook.sh` to use the adapter in the notebook.

### Elasticsearch

Install and start [elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/5.0/_installation.html)

### Monitoring spark

Once a dev spark cluster has been started, it is useful to view spark's web UI.
This can be done by forwarding the web UI ports as so:

```
ssh -v -L8080:0:8080 -L8081:0:8081 -N dev-machine
```

Worker UIs will increment sequentially from `8081`, so further ports may need to
be added in the case of a clust with more than one worker.

### Submitting to Spark

To run the export, copy the `bin/submit-job.sh.template` to `bin/submit-job.sh` and 
configure the environment variables as needed. Then run `bin/submit-job.sh` to
submit the job to the spark cluster.

### Running in Jupyter

It may be useful to develop with the help of Jupyter. Jupyter notebook will be
installed as part of the dev requirements and can be invoked with
`bin/run-notebook.sh`. Make sure to set the `$SPARK_HOME` variable correctly within
`bin/run-notebook.sh`. To view the notebook, the port (default 9099) will need to
be forwarded to the local machine:

```
ssh -v -L9099:0:9099 -N dev-machine
```


## Tests

Tests depend on `$PYTHONPATH` being configured correctly to find the spark 
python modules. Make sure the paths are correct in `bin/run-tests.sh`.

```
❯  bin/run-tests.sh
```

## Contributing

Read how to contribute [here](https://github.com/NCI-GDC/gdcapi/blob/master/CONTRIBUTING.md)
