#!/usr/bin/env bash

for DRIVER in "$@"
do
    export MODULE="${DRIVER//-/_}"
    envsubst < bin/driver.py > "$DRIVER.py";
    cp uv.lock "$DRIVER.py.lock"
    uv export \
        --format requirements.txt \
        --emit-index-url \
        --extra $DRIVER \
        --no-emit-local \
        --no-hashes \
        --output-file "requirements-$DRIVER.txt";
    uv add \
        --script "$DRIVER.py" \
        --requirements "requirements-$DRIVER.txt" \
        gdc-mutation-indexer==1.4.0;
    rm "$DRIVER.py.lock" "requirements-$DRIVER.txt";
done;
