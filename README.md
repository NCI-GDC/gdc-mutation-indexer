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

A spark cluster must exist to be run against. A standalone cluster on a single
node with a single master and slave is sufficient for developing against.
See the official insructions [instructions](http://spark.apache.org/docs/latest/spark-standalone.html).

### Monitoring spark

Once a dev spark cluster has been started, it is useful to view spark's web UI.
This can be done by forwarding the web UI ports as so:

```
ssh -v -L8080:0:8080 -L8081:0:8081 -N dev-notebook-0
```

Worker UIs will increment sequentially from `8081`, so further ports may need to
be added in the case of a clust with more than one worker.

### Running

To run the export, copy the `submit-job.sh.template` to `submit-job.sh` and 
configure the environment variables as needed. Then run `submit-job.sh` to
submit the job to the spark cluster.

## Tests

```
❯  py.test tests
```

## Contributing

Read how to contribute [here](https://github.com/NCI-GDC/gdcapi/blob/master/CONTRIBUTING.md)
