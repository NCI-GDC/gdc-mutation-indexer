Deployment
==========

Spark Application
#################

The indexer is ultimately a spark application that may be submitted to a spark
cluster to be run in a distributed fashion.

Manager Agnostic Settings
*************************

Standalone Specific Settings
****************************

Running on YARN
###############

Setup
*****

In order to submit jobs to the YARN cluster, you will need to install Hadoop on your machine and download Spark. On Ubuntu, follow the 
instructions on the `Hadoop Wiki<https://cwiki.apache.org/confluence/display/BIGTOP/How+to+install+Hadoop+distribution+from+Bigtop+0.5.0>`_ 
to install `bigtop-utils` and `hadoop` packages. The next step is to copy the contents of `/etc/hadoop/conf` from the Spark head node to
the same location on your machine. These files tell your local Hadoop installation about the configuration of the YARN cluster. The last
step in YARN setup is to add an environment variable to your shell named `YARN_CONF_DIR` containing the path (`/etc/hadoop/conf`)to the 
config files.

In addition to Hadoop, it is also necessary to download Spark. Get a recent stable release of 
`Spark<http://spark.apache.org/downloads.html>`_ and extract it somewhere memorable. Create an environment variable named `SPARK_HOME` and 
set it to the location of the Spark distribution. 

Managing Dependencies
*********************

When running on a YARN cluster, all dependencies needed for a job need to be shipped to the cluster at the time the job is submitted. 
This project has both Python and Java dependencies, which somewhat complicates matters but both are dealt with in a similar manner. The idea
is to download redistributable copies of all dependencies, and then give their paths to `spark-submit`. The `bin/submit-job.sh` script 
contains a list of expected files, as well as instructions on how to refresh the Python eggs. 

Configuration
*************

The `yarn` folder in the project root contains configuration files for use with YARN. All files in this folder should be copied to 
`$SPARK_HOME/conf/`. Pay attention to `spark-defaults.conf`, as you will need to add credentials for Elasticsearch and Cleversafe.
When making adjustments to memory settings, be careful to make sure that the driver memory allocation is smaller than the executor memory
allocation. Having a larger allocation can trigger a Spark bug which will cause the job to fail. 

The file `metrics.properties` configures Spark to ship metrics to Graphite. `log4j.properties` is responsible for configuring console logs.
Be careful when setting the log filter to `DEBUG`, as this level of logging will cause all the job's data to be written to the logs as a
means of inspecting the wire protocol between the Python runtime and the underlying JVM. 


Running Jobs
************

Once everything is set up and configured, simply invoke `bin/submit-job.sh` from the project root. With luck, your job will be submitted to 
the cluster and executed. You can monitor the Spark UI while the job is executing by establishing a SOCKS proxy using SSH to the Spark head node and accessing the UI in a browser using the proxy.
