# Tests
The test are run through the tox command which will handle the usual python requirements
as well as a few specific unique requirements such as jar files which need to be install
in order to run the tests.

The test are separated into unit and integration tests. Please limit adding new
integration test to things which can *ONLY* be tested by integrating with elasticsearch,
indexd, or other external services. Otherwise, mock any interfaces onto external
services and use unit tests. It should be noted that even the unit tests do run a local
spark instance through which pyspark/spark functionality are run.

### Contents
- [Running Tests](#running-tests)
- [Elasticsearch](#elasticsearch)
- [Java](#java)
- [Tools](#tools)
  - [Models](unit/data/models/README.md)
  - [Schemas](unit/data/schemas/README.md)

## Running Tests
Run the following command

```bash
tox -e test
```

## Dockerized Elasticsearch and Indexd
Instead of locally installed services, docker imgs for Mac M2 are available.

```
elasticsearch=$(docker run --name elasticsearch -d --rm -p 9200:9200 -p 9300:9300 \
    -e ES_JAVA_OPTS="-Xms1g -Xmx1g" \
    -e "discovery.type=single-node" \
    -e "xpack.security.enabled=false" \
    --entrypoint '/bin/bash' \
    docker.elastic.co/elasticsearch/elasticsearch:7.16.1 \
    -c '/usr/share/elasticsearch/bin/elasticsearch-plugin list | grep -q mapper-size || /usr/share/elasticsearch/bin/elasticsearch-plugin install mapper-size && /usr/local/bin/docker-entrypoint.sh elasticsearch')
echo "Elasticsearch running" $elasticsearch

indexd=$(docker run \
    --name indexd \
    -d \
    --rm \
    -p 80:80 \
    docker.osdc.io/ncigdc/indexd)
echo "Indexd running" $indexd
```

[Java](#java) installation is required.

## Elasticsearch
For the integration tests it is required that you have a working Elasticsearch on your
device. To insure insure you have elasticsearch working on your device run the command
below. If it is not installed see the appropriate section below.

```bash
service elasticsearch status
```

NOTE: Make sure your elasticsearch server is running at port 9200.

### MacOS/Homebrew
```bash
brew install elasticsearch@7.6
/usr/local/Cellar/elasticsearch\@7.6/7.6.2/bin/elasticsearch-plugin install mapper-size
brew services start elasticsearch@7.6
```

### Ubuntu
```bash
sudo apt-get install "elasticsearch=7.6.2"
sudo apt-mark hold elasticsearch
sudo /usr/share/elasticsearch/bin/elasticsearch-plugin install mapper-size
service elasticsearch start
```


## Java
As Spark runs on scala it is required to have a working version of the JDK installed and
to have the JAVA_HOME environment variable set in order to run _*all*_ tests. You can
test this using the command below. If it is not installed you just install a working
Java8 installation.

```bash
# expected output should point to a Java 8 jdk.
echo $JAVA_HOME
```

### MacOS
```bash
brew install openjdk@8  # In theory, upto openjdk@17 should work, openjdk@21 won't.
... Follow extra instructions for PATH, symlinks, source, etc.
# Add JAVA_HOME to your shell
echo "export JAVA_HOME=$HOMEBREW_PREFIX/opt/openjdk@8/openjdk-8.jdk" >> zshrc.sh
. ~/zshrc.sh
brew install maven
```

### Ubuntu
```bash
sudo apt-get install openjdk-8-jdk
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ~/.bashrc
source ~/.bashrc
```

## Tools
There are several tools included in the test suite in order to make several manual
steps for generating data for the tests easier and faster. Please see the specific
tools for more details.

### [Model Tools](unit/data/models/README.md)
These are tools for generating various input models for the test data.

### [Schema Tools](unit/data/schemas/README.md)
These are tools for managing the schemas used to load test data.

### [Sync Tools](integration/data/sync/README.md)
These tools are for syncing the integration test data with gdc-models.
