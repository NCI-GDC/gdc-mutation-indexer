#!/bin/bash

SPARK_HOME="${SPARK_HOME:-~/spark-2.4.5-bin-hadoop2.7}"
# uncomment to run locally
# export SPARK_HOME=~/spark-2.4.5-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH

echo 'SPARK_HOME:' $SPARK_HOME
echo 'PYTHONPATH:' $PYTHONPATH

python -m pytest -m 'not do_not_collect' --cov=gdc-mutation-indexer --cov-report xml --cov-report term
#python -m pytest -vv tests/builders/test_es_index_data.py --pdb
