#!/bin/bash

SCRIPTDIR=$( pwd )/$( dirname "${BASH_SOURCE-$0}" )
LOGDIR=$SCRIPTDIR/../reports
LOGFILE=$LOGDIR/pyunit.log
PYUNIT_DIR=./test/pyunit

if [ ! -d $LOGDIR ]; then
    mkdir $LOGDIR
fi;

if [ -f $LOGFILE ]; then
    rm $LOGFILE
fi;

for test in $PYUNIT_DIR/test_*.py; do
    python $test 2>&1 | tee -a $LOGFILE
done;
