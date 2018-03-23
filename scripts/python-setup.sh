#!/bin/bash
# This can be invoked in CI scripts (TODO: port to travis or other CI available on github)

if [[ -z "$WORKSPACE" ]] ; then
scripts=`dirname "${BASH_SOURCE-$0}"`
scripts=`cd "$scripts">/dev/null; pwd`
export WORKSPACE=`cd $scripts/../.. >/dev/null; pwd`
fi

export SITEOPS_VIRTUALENV=$WORKSPACE/jenkins-venv
export PYTHONPATH=$PYTHONPATH:.

if [ ! -z $VIRTUAL_ENV ] ; then
source $VIRTUAL_ENV/bin/activate
deactivate
fi

if ! [[ -d "$SITEOPS_VIRTUALENV" && -f "$SITEOPS_VIRTUALENV/bin/activate" ]] ; then
if ! which virtualenv ; then
wget http://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.9.1.tar.gz
tar -xzf virtualenv-1.9.1.tar.gz
rm virtualenv-1.9.1.tar.gz
pushd virtualenv-1.9.1
python virtualenv.py $SITEOPS_VIRTUALENV
popd
else
virtualenv $SITEOPS_VIRTUALENV
fi

if [ -d virtualenv-1.9.1 ] ; then
rm -rf virtualenv-1.9.1
fi
fi

export PATH=$PATH:$SITEOPS_VIRTUALENV/bin
source $SITEOPS_VIRTUALENV/bin/activate

if [ -f requirements.txt ]; then
pip install -r requirements.txt --allow-all-external --allow-unverified progressbar
fi

if [ -f requirements-dev.txt ]; then
pip install -r requirements-dev.txt --allow-all-external --allow-unverified progressbar
fi

rm -rf reports

if [ ! -d reports ]; then
mkdir reports
fi
