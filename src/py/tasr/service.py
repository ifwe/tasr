'''
Created on Apr 14, 2014

@author: cmills
'''

from tasr import AvroSchemaRepository
from bottle import Bottle, request, abort, response
from avro.schema import SchemaParseException

ASR = AvroSchemaRepository()
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
        response.add_header('X-Schema-Topic', _t)
        response.add_header('X-Schema-Version', _v)
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
    except Exception as e:
        abort(500, 'Exception:%s' % e)

@app.get('/tasr/topic/<topic_name>')
def get_latest_for_topic(topic_name=None):
    if topic_name == None or topic_name == '':
        abort(400, 'Refusing to look for schema for %s' % topic_name)
    try:
        _rs = ASR.get_latest_for_topic(topic_name)
        if _rs:
            set_x_schema_headers(response, _rs)
            return _rs.canonical_schema_str
        # return nothing if there is no schema registered for the topic name
        abort(404, 'No schema registered for topic %s.' % topic_name)
    except Exception as e:
        abort(500, 'Exception:%s' % e)

@app.get('/tasr/id/<base64_id>')
def get_for_id(base64_id=None):
    if base64_id == None or base64_id == '':
        abort(400, 'Refusing to look for schema for %s' % base64_id)
    try:
        _rs = ASR.get_for_id(base64_id)
        if _rs:
            set_x_schema_headers(response, _rs)
            return _rs.canonical_schema_str
        # return nothing if there is no schema registered for the topic name
        abort(404, 'No schema registered with id %s' % base64_id)
    except Exception as e:
        abort(500, 'Exception:%s' % e)

if __name__ == "__main__":
    app.run(host='localhost', port=8080, debug=True)

