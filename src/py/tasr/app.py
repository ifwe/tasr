'''
Created on Apr 14, 2014

@author: cmills

Here we have a WSGI (web server gateway interface) web app supporting a REST
API for the Tagged Avro Schema Repository (TASR).  It is implemented in Bottle,
so it can be run as a stand-alone process (for development), deployed under a
python package that implements a faster WSGI-compliant web server (gunicorn, 
FAPWS, tornado), or deployed under a WSGI plugin on a fast, general purpose web 
server (e.g. -- nginx + uWSGI).

Running in stand-alone mode
---------------------------
To run it as a stand-alone process, either call the main() function explicitly, 
or just run it from the command line:

    python tasr/app.py -h localhost -p 8080

Note that the host and port args are optional, with the defaults shown in the 
example above.

Be aware that running this service in stand-alone mode is not suitable for prod.
Bottle's built-in web server is single-threaded and does not handle all the 
standard POSIX signals as you would expect a (well-behaved) daemon to do.

Running under gunicorn
----------------------
Gunicorn is a faster, more stable, reasonably mature WSGI server written in C
and Python.  It is simple to deploy and suitable for production use (if not as
fast as nginx + uWSGI). Once gunicorn is installed, firing up a service with 
four worker threads and debug-level logging looks like this:

    gunicorn -w 4 -b localhost:8080 --log-level debug tasr.app:app

That's it.  A SIGINT will kill it fast.  A SIGTERM will do a graceful shutdown, 
allowing in-flight requests to be finished before killing the workers.  Adding a
"-D" will daemonize the process.
'''

import sys
from tasr import AvroSchemaRepository
from bottle import Bottle, request, abort, response
from avro.schema import SchemaParseException

# If we can't establish an ASR, there's no point in doing anything else here.
# Note that the issue is usually that Redis isn't running or is inaccessible.
ASR = None
try:
    ASR = AvroSchemaRepository()
except Exception as e:
    print "Exception: %s" % e
    sys.exit(2)

app = Bottle()

def _set_x_schema_headers(response, registered_schema):
    '''The body of the response should only contain the schema itself, so any 
    identifying or contextual (meta) data has to live in the response headers.
    In this case we include the SHA256 and MD5 ids along with the topic-version
    intersections.
    '''
    md5_id = registered_schema.md5_id
    sha256_id = registered_schema.sha256_id
    for t, v in registered_schema.tv_dict.iteritems():
        response.add_header('X-Schema-Topic-Version', '%s=%s' % (t, v))
    for t, ts in registered_schema.ts_dict.iteritems():
        response.add_header('X-Schema-Topic-Version-Timestamp', '%s=%s' % (t, ts))
    response.set_header('X-Schema-MD5-ID', md5_id)
    response.set_header('X-Schema-SHA256-ID', sha256_id)
    return len(registered_schema.tv_dict)

@app.put('/tasr/topic/<topic_name>')
def register(topic_name=None):
    ct = str(request.content_type).split(';')[0].strip()
    if not ct in ['application/json', 'text/json']:
        abort(406, 'Content-Type must be appliction/json or text/json.')

    schema_str = request.body.getvalue()
    if schema_str == None or schema_str == '':
        abort(400, 'Refusing to register nothing for %s' % topic_name)
    
    try:
        rs = ASR.register(topic_name, schema_str)
        if rs:
            _set_x_schema_headers(response, rs)
        if not rs.is_valid:
            abort(400, 'Invalid schema.  Failed to register.')
        return
    except SchemaParseException:
        abort(400, 'Invalid schema.  Failed to register.')

@app.get('/tasr/topic/<topic_name>')
def get_latest_for_topic(topic_name=None):
    if topic_name == None or topic_name == '':
        abort(400, 'Refusing to look for schema for %s' % topic_name)
    rs = ASR.get_latest_for_topic(topic_name)
    if rs:
        _set_x_schema_headers(response, rs)
        return rs.canonical_schema_str
    # return nothing if there is no schema registered for the topic name
    abort(404, 'No schema registered for topic %s.' % topic_name)
        
@app.get('/tasr/topic/<topic_name>/<version>')
def get_for_topic_and_version(topic_name=None, version=None):
    if topic_name == None or topic_name == '':
        abort(400, 'Refusing to look for schema for %s' % topic_name)
    if version == None or version.lower() == 'latest':
        return get_latest_for_topic(topic_name)
    rs = ASR.get_for_topic_and_version(topic_name, version)
    if rs:
        # With multiple versions for a topic, only the latest is included in the 
        # retrieved RS.  If we asked for a version for a topic and got back an RS 
        # with a differing version, it is safe to overwrite the version for the
        # specified topic before generating the response headers.  That way the 
        # client gets headers indicating the version expected.
        rs.tv_dict[topic_name] = version
        _set_x_schema_headers(response, rs)
        return rs.canonical_schema_str
    # return nothing if there is no schema registered for the topic name
    abort(404, 'No schema version %s registered for topic %s.' % 
          (version, topic_name))

@app.get('/tasr/id/<base64_id:path>') # Base64 IDs can include slashes, so :path
def get_for_id(base64_id=None):
    if base64_id == None or base64_id == '':
        abort(400, 'Refusing to look for schema for %s' % base64_id)
    rs = ASR.get_for_id(base64_id)
    if rs:
        _set_x_schema_headers(response, rs)
        return rs.canonical_schema_str
    # return nothing if there is no schema registered for the topic name
    abort(404, 'No schema registered with id %s' % base64_id)

import getopt
def main(argv, out=sys.stdout, err=sys.stderr):
    sys.stdout = out
    sys.stderr = err
    host = 'localhost'
    port = 8080
    debug = False
    try:
        opts = getopt.getopt(argv, "h:p:d", ["host=", "port=", "debug"])[0]
    except getopt.GetoptError:
        print 'app.py -h <hostname> -p <port> [-d]'
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--host"):
            host = arg
        if opt in ("-p", "--port"):
            port = arg
        if opt in ("-d", "--debug"):
            debug = True
    app.run(host = host, port = port, debug = debug)
    sys.stdout.write('TASR running...')


if __name__ == "__main__":
    main(sys.argv[1:], sys.stdout, sys.stderr)

