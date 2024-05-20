#!/usr/bin/bash

export PYTHONUSERBASE=intentionally-disabled

conda env create -f environment.yaml -p /tmp/mutation_indexer
conda-pack -p /tmp/mutation_indexer -o /tmp/mutation_indexer.tar.gz
scp /tmp/mutation_indexer.tar.gz micky@172.23.8.228:/home/micky
rm -fr /tmp/mutation_indexer*
