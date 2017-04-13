#!/bin/bash
unset http_proxy
unset https_proxy

### How to generate the lists of python dependencies
### 1) Download required Python dependencies using pip: `pip install -r requirements.txt -d artifacts --egg --no-use-wheel`
### 2) Create comma-separated list of files and append to this script. 
###    From the project root, run `find artifacts -name *.tar.gz | paste -sd "," >> bin/submit-job.sh`

EGGS="artifacts/PyYAML-3.11-py2.7-linux-x86_64.egg,artifacts/progressbar-2.2-py2.7.egg,artifacts/elasticsearch-5.0.0-py2.7.egg,artifacts/addict-0.2.7-py2.7.egg,artifacts/consulate-0.4.0-py2.7.egg,artifacts/filechunkio-1.6-py2.7.egg"

### Java dependencies can be downloaded from the Maven site. 
JARS="artifacts/elasticsearch-spark-20_2.11-5.2.2.jar,artifacts/aws-java-sdk-1.7.4.jar,artifacts/spark-csv_2.11-1.5.0.jar"

python setup.py bdist_egg
exec $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 12 \
    --py-files dist/gdc_mutation_indexer-0.1.0-py2.7.egg,$EGGS \
    --jars $JARS \
    bin/export.py "$@"
