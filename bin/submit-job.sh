#!/bin/bash
unset http_proxy
unset https_proxy

### How to generate the lists of python dependencies
### 1) Download required Python dependencies using pip: `pip install -r requirements.txt -d artifacts --egg --no-use-wheel`
### 2) Create comma-separated list of files and append to this script. 
###    From the project root, run `find artifacts -name *.tar.gz | paste -sd "," >> bin/submit-job.sh`
EGGS="artifacts/eggs/boto-2.46.1-py2.7.egg,artifacts/eggs/urllib3-1.20-py2.7.egg,artifacts/eggs/elasticsearch-5.0.0-py2.7.egg,artifacts/eggs/addict-0.2.7-py2.7.egg,artifacts/eggs/decorator-4.0.11-py2.7.egg,artifacts/eggs/networkx-1.10-py2.7.egg,artifacts/eggs/consulate-0.4.0-py2.7.egg,artifacts/eggs/yaml.zip,artifacts/eggs/requests-2.6.0-py2.7.egg"

### Java dependencies can be downloaded from the Maven site. 
JARS="artifacts/jars/aws-java-sdk-1.7.4.jar,artifacts/jars/elasticsearch-spark-20_2.11-5.2.2.jar,artifacts/jars/spark-csv_2.11-1.5.0.jar"

REVISION=$(git rev-parse HEAD)

python setup.py bdist_egg
exec $SPARK_HOME/bin/spark-submit \
	--name "GDC Mutation Indexer" \
	--master yarn \
	--deploy-mode cluster \
	--executor-memory 40g \
	--driver-memory 12g \
	--executor-cores 8 \
	--num-executors 36 \
	--py-files dist/gdc_mutation_indexer-0.1.0_rev_$REVISION-py2.7.egg,$EGGS \
	--jars $JARS \
	bin/export.py "$@"
