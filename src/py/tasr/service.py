'''
Created on Apr 14, 2014

@author: cmills
'''

import sys
from tasr import AvroSchemaRepository
from bottle import Bottle, request, abort, response
from avro.schema import SchemaParseException

ASR = None
try:
    ASR = AvroSchemaRepository()
except Exception as e:
    print "Exception: %s" % e
    sys.exit(2)

app = Bottle()

accepted_content_types = ['application/json', 'text/json']

def set_x_schema_headers(response, registered_schema):
    _md5_id = None
    _sha256_id = None
    _topics = []
    _versions = []
    _md5_id = registered_schema.md5_id
    _sha256_id = registered_schema.sha256_id
    for _t, _v in registered_schema.tv_dict.iteritems():
        response.add_header('X-Schema-Topic-Version', '%s=%s' % (_t, _v))
    response.set_header('X-Schema-MD5-ID', _md5_id)
    response.set_header('X-Schema-SHA256-ID', _sha256_id)
    return len(registered_schema.tv_dict)

@app.put('/tasr/topic/<topic_name>')
def register(topic_name=None):
    _ct = str(request.content_type).split(';')[0].strip()
    if not _ct in accepted_content_types:
        abort(406, 'Content-Type must be appliction/json or text/json.')

    _schema_str = request.body.getvalue()
    if _schema_str == None or _schema_str == '':
        abort(400, 'Refusing to register nothing for %s' % topic_name)
    
    try:
        _rs = ASR.register(topic_name, _schema_str)
        if _rs:
            set_x_schema_headers(response, _rs)
        if not _rs.is_valid:
            abort(400, 'Invalid schema.  Failed to register.')
        return
    except SchemaParseException:
        abort(400, 'Invalid schema.  Failed to register.')

@app.get('/tasr/topic/<topic_name>')
def get_latest_for_topic(topic_name=None):
    if topic_name == None or topic_name == '':
        abort(400, 'Refusing to look for schema for %s' % topic_name)
    _rs = ASR.get_latest_for_topic(topic_name)
    if _rs:
        set_x_schema_headers(response, _rs)
        return _rs.canonical_schema_str
    # return nothing if there is no schema registered for the topic name
    abort(404, 'No schema registered for topic %s.' % topic_name)
        
@app.get('/tasr/topic/<topic_name>/<version>')
def get_for_topic_and_version(topic_name=None, version=None):
    if topic_name == None or topic_name == '':
        abort(400, 'Refusing to look for schema for %s' % topic_name)
    if version == None or version.lower() == 'latest':
        return get_latest_for_topic(topic_name)
    _rs = ASR.get_for_topic_and_version(topic_name, version)
    if _rs:
        # With multiple versions for a topic, only the latest is included in the 
        # retrieved RS.  If we asked for a version for a topic and got back an RS 
        # with a differing version, it is safe to overwrite the version for the
        # specified topic before generating the response headers.  That way the 
        # client gets headers indicating the version expected.
        _rs.tv_dict[topic_name] = version
        set_x_schema_headers(response, _rs)
        return _rs.canonical_schema_str
    # return nothing if there is no schema registered for the topic name
    abort(404, 'No schema version %s registered for topic %s.' % 
          (version, topic_name))

@app.get('/tasr/id/<base64_id>')
def get_for_id(base64_id=None):
    if base64_id == None or base64_id == '':
        abort(400, 'Refusing to look for schema for %s' % base64_id)
    _rs = ASR.get_for_id(base64_id)
    if _rs:
        set_x_schema_headers(response, _rs)
        return _rs.canonical_schema_str
    # return nothing if there is no schema registered for the topic name
    abort(404, 'No schema registered with id %s' % base64_id)

import getopt
def main(argv):
    _host = 'localhost'
    _port = 8080
    _debug = False
    try:
        _opts, _args = getopt.getopt(argv, "h:p:d", ["host=", "port=", "debug"])
    except getopt.GetoptError:
        print 'service.py -h <hostname> -p <port> [-d]'
        sys.exit(2)
    for _opt, _arg in _opts:
        if _opt in ("-h", "--host"):
            _host = _arg
        if _opt in ("-p", "--port"):
            _port = _arg
        if _opt in ("-d", "--debug"):
            _debug = True
    app.run(host = _host, port = _port, debug = _debug)


if __name__ == "__main__":
    main(sys.argv[1:])

