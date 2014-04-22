Running TASR as a Standalone Process
------------------------------------

In a production context, it should be run as a WSGI application under Apache or
Nginx.  However, for development, it can be run in standalone mode.  Here it is,
as run from the TASR home, logging everything to one file, running as a 
background process:

    python src/py/tasr/service.py >>/var/log/tasr.log 2>&1 &
    
NOTE: The TASR service expects a Redis instance.  Run in standalone mode, it 
expects one on localhost.  Redis has to be running before TASR starts.