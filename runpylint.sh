#!/bin/bash

LOGDIR=./reports
LOGFILE=$LOGDIR/pylint.log
RCFILE=.pylintrc
PACKAGES=(tasr)

if [ ! -f $RCFILE ]; then
    echo "No rcfile $RCFILE"
    exit 1
fi;

if [ ! -d $LOGDIR ]; then
    mkdir $LOGDIR
fi;

if [ -f $LOGFILE ]; then
    rm $LOGFILE
fi;

for package in ${PACKAGES[*]}; do
    pylint --rcfile=$RCFILE -f parseable -r n $package >> $LOGFILE
done;
