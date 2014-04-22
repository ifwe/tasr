Running TASR as a Standalone Process
------------------------------------

In a production context, it should be run as a WSGI application under Apache or
Nginx.  However, for development, it can be run in standalone mode.  Here it is,
as run from the TASR home, bound to 8080 on localhost, logging everything to one 
pre-existing file, running as a background process:

    python src/py/tasr/service.py >>/var/log/tasr.log 2>&1 &

Now, if you want to allow connections from the outside world, you have to specify
a host IP address to bind to.  For dhdp2jump01, that's 10.98.40.46, so: 

    python src/py/tasr/service.py -h 10.98.40.46 >> /var/log/tasr.log 2>&1 &
    
NOTE: The TASR service expects a Redis instance.  Run in standalone mode, it 
expects one on localhost.  Redis has to be running before TASR starts or it will 
exit with an exception.

