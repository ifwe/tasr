'''
Created on Apr 14, 2014

@author: cmills

Here we have a WSGI (web server gateway interface) web app supporting a REST
API for the Tagged Avro Schema Repository (TASR).  This module pulls everything
together in a callable WSGI object (TASR_APP).  Related endpoints are collected
in separate modules, then imported and mounted to the main application in this
module.  Currently the app sub-modules are: core, topic, and subject.

Configuration is pulled from a 'tasr.cfg' file.  This app expects that file to
be in one of three places.  It will check, in order, the execution directory,
a 'conf' subdir of the execution directory, and /etc.


Running under mod_wsgi
----------------------
The TASR_APP object is the "callable object" for mod_wsgi.  The main things
you need to set are WSGIScriptAlias and WSGICallableObject.  In our standard
conf file (for Apache httpd with mod_wsgi), these look like this:

    WSGIScriptAlias / /usr/lib/python2.7/site-packages/tasr/app.py
    WSGICallableObject TASR_APP

It is also a good idea to set the WSGIProcessGroup and the WSGIDaemonProcess.
Our default settings look like this:

    WSGIProcessGroup tasr
    WSGIDaemonProcess tasr processes=2 threads=15 display-name=%{GROUP} python-path=/usr/lib/python2.7/site-packages

For running the app in standalone mode, please see the app_standalone module.
'''
from tasr.app.wsgi import TASRApp
from tasr.app.collection_app import COLLECTION_APP
from tasr.app.id_app import ID_APP
from tasr.app.schema_app import SCHEMA_APP
from tasr.app.subject_app import SUBJECT_APP
from tasr.app.redshift_app import REDSHIFT_APP


TASR_APP = TASRApp()
# core endpoints are non-colliding, so just mount them
TASR_APP.mount('/tasr/collection', COLLECTION_APP)
TASR_APP.mount('/tasr/id', ID_APP)
TASR_APP.mount('/tasr/schema', SCHEMA_APP)

# the RS endpoints are extensions to the subject endpoints, so merge them in
# then mount the merged application
SUBJECT_APP.merge(REDSHIFT_APP)
TASR_APP.mount('/tasr/subject', SUBJECT_APP)
