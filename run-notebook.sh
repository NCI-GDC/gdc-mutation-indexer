export SPARK_HOME=~/spark

export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.open_browser=False --NotebookApp.ip='*' --NotebookApp.port=9099"
export PYSPARK_PYTHON=python

exec $SPARK_HOME/bin/pyspark
