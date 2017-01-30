# Monitoring and Logging GDC Mutation Indexer builds

## SparkUI

Once a spark application has been started, it is useful to view spark's web UI.
This can be done by forwarding the web UI ports as so:

```
ssh -v -L4040:0:4040 -L8080:0:8080 -L8081:0:8081 -N dev-machine
```

The main UI will be started on `4040`, if it is unavailable, it will try
ports sequentially. The port it's running on will also be reported in the
console when the build is started.
Port `8080` is the master node and will give an overview of the workers and
currently running applications.
Worker UIs will increment sequentially from `8081`, so further ports may need to
be added in the case of a clust with more than one worker.

## Kibana

[Kibana](https://www.elastic.co/products/kibana) is useful for monitoring the
state of the elasticsearch cluster with regards to how quickly an index is
being loaded and how well the elasticsearch cluster is being utilized.

Kibana starts the dashboard on `5601` and so can be tunneled as above:

```
ssh -v -L5601:0:5601 -N dev-machine
```
