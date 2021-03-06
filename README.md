TASR: Tagged Avro Schema Repository
-----------------------------------
This is an implementation of an Avro Schema Repository of the sort
discussed in AVRO-1124 (and previously in AVRO-1006).  The in-memory
version distributed with Camus is just not sufficient for prod use.
This should be.

Dependencies
------------
TASR is written in Python.  It requires some Python packages (as noted in the
setup.py):

- avro
- bottle
- redis
- requests

TASR also requires a Redis instance to be available.  The easiest way to do this
is to install Redis on the local box and run it on the default port (6379).  If
Redis is elsewhere, you will need to pass "host" and "port" options to the
AvroSchemaRepository instantiation in tasr.app.

Stand-Alone Deployment
----------------------
For dev and simple tests, running TASR in stand-alone mode (coutesy of Bottle)
is simple and probably sufficient.  Assuming you have installed the tasr module
via pip, from the project's home dir, run:

    python src/py/tasr/app_standalone.py --env local

Remember that Redis needs to be available (localhost:6379) before you start up
TASR.  If you want to quickly populate a new (empty) running test instance, run:

    python scripts/populate_via_local_service.py

This should put the schemas used for testing into the repo.  You can then test
the service with:

    curl http://localhost:8080/tasr/subject/gold

This should return the schema for the "gold" topic.

Running TASR Tests
------------------
TASR has some unit tests.  The code and fixtures live under the "tests"
directory.  Please note that the unit tests clean up after themselves by
clearing out any and all entries in the localhost Redis instance.  This ensures
a repeatable, knowable clean slate for the tests -- but don't point the tests
at an instance with important data without care (and test modification).

With that warning out of the way, realize also that the tests rely on a couple
extra Python packages.  Those package are:

- webtest
- httmock

These packages are NOT required to run TASR, just to run the pyunit tests that
live under the "tests" directory.