Testing
=======

Tests depend on ``$PYTHONPATH`` being configured correctly to find the spark 
python modules. Make sure the paths are correct in ``bin/run-tests.sh``::

    bin/run-tests.sh

Tests use ``pytest`` so any flags desired may be passed to ``run-tests.sh``
and directly to ``pytest``.

Unit Tests
##########


End to End tests
################


