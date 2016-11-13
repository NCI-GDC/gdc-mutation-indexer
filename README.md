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


#### Requirements

- [elasticsearch 5.0.0](https://www.elastic.co/downloads/elasticsearch)
- [spark-2.0.1-hadoop-2.7](http://spark.apache.org/downloads.html)

The following libraries are required by spark. Install by either adding them to
the maven dependencies or download the jars and place them in the class path.
The easiest way to add them to the classpath is by placing them in `spark/jars`.

- [hadoop-aws-2.7.7](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/2.7.3)
- [elasticsearch-hadoop-5.0.0](https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-hadoop/5.0.0)
- [aws-java-sdk-1.7.4](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk/1.7.4)
- [spark-csv-1.5](https://mvnrepository.com/artifact/com.databricks/spark-csv_2.11/1.5.0)

### Elasticsearch

Install and start [elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/5.0/_installation.html)

#### Spark

A spark cluster must exist to be run against. A standalone cluster on a single
node with a single master and slave is sufficient for developing against.
See the official insructions [instructions](http://spark.apache.org/docs/latest/spark-standalone.html).

#### Configure and use S3

The config for the S3 service can be configured in a couple different ways,
easiest is to change `spark/conf/spark-defaults.conf`:

```
spark.hadoop.fs.s3a.access.key=AWS_ACCESS_KEY_ID
spark.hadoop.fs.s3a.secret.key=AWS_SECRET_ACCESS_KEY_ID
spark.hadoop.fs.s3a.endpoint=ACCESSOR_IP
```

Spark uses the hadoop-aws adapter which uses the aws-java-sdk to talk with s3.
The sdk enforces stardards held by aws s3 which may vary slightly from those
allowed by cleversafe or other s3 interfaces. for instance, hostnames are not
typically expected in `s3://` url, and bucket names are *not allowed to contain
underscores*.

When working with the s3 adapter, use urls in the following format so that
hadoop knows what filesystem protocol to use:
`s3a://bucketname/key`

Here are some examples of ways to interact with the s3 adapter:

```python
url = 's3a://test/my_file.maf.gz'
f = sc.textFile(url)

rdd = sc.hadoopFile(url,
                    'org.apache.hadoop.mapred.TextInputFormat',
                    'org.apache.hadoop.io.Text',
                    'org.apache.hadoop.io.LongWritable')

df = sqlContext.read.format('com.databricks.spark.csv')\
                    .options(header='true')\
                    .options(codec="org.apache.hadoop.io.compress.GzipCodec")\
                    .load(url)
```

#### Elasticsearch hadoop adapter

Install the elasticsearch hadoop adapter by downloading the
[zip](https://www.elastic.co/downloads/hadoop). Extract the contents and 
put the path to the `dist` folder inside the `$SPARK_HOME/conf/spark-defaults.sh`
as the `spark.driver.extraClassPath` variable. Make sure to also set the
`--jars` flag in `bin/run-notebook.sh` to use the adapter in the notebook.



### Monitoring spark

Once a dev spark cluster has been started, it is useful to view spark's web UI.
This can be done by forwarding the web UI ports as so:

```
ssh -v -L4040:0:4040 -L8080:0:8080 -L8081:0:8081 -N dev-machine
```

The main UI will be started on `4040`.
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

### Starting a standalone cluster

Spark can be deployed in standalone mode which requires all target machines
to have matching `SPARK_HOME` directories and jars. To tell the master where 
to start workers, add their ips or hostnames in `spark/conf/slaves.conf`.
Note that the same machine may have more than one worker started on it by
listing the host multiple times, such as placing localhost twice to debug two
workers on a dev box.

The cluster can be started using `spark/sbin/start-all.sh` and managed through
the other scripts found in that directory. Executors may be configured through
spark defaults using some of these configuration settings in
`spark/conf/spark-defaults.conf`:

```
spark.executor.instances
spark.executor.cores
spark.executor.memory
```
See other spark configuration settings
[here](http://spark.apache.org/docs/latest/configuration.html)



To run the pyspark notebook environment on standalone workers outside of the
single master cluster set up by default, set `MASTER` in the environment or
in `bin/run-notebook` to the url and port of the master service. Ex:

```
export MASTER='spark://dev-master-av2-dev2-dkolbman-notebook-0:7077' 
```


## Tests

Tests depend on `$PYTHONPATH` being configured correctly to find the spark 
python modules. Make sure the paths are correct in `bin/run-tests.sh`.

```
❯  bin/run-tests.sh
```

## Contributing

Read how to contribute [here](https://github.com/NCI-GDC/gdcapi/blob/master/CONTRIBUTING.md)
