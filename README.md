tasr: Tagged Avro Schema Repository
-----------------------------------
This is an implementation of an Avro Schema Repository of the sort
discussed in AVRO-1124 (and previously in AVRO-1006).  The in-memory
version distributed with Camus is just not sufficient for prod use.
Hopefully this will be.

This implementation relies on an available Redis instance.  For running
tests, fire up Redis on localhost with the default port (6379).  Note that 
the tests clean up after themselves by clearing out any and all entries in 
the localhost instance.  This ensures a repeatable, knowable clean slate for 
the tests -- but don't point the tests at an instance with important data 
without care (and test modification).

