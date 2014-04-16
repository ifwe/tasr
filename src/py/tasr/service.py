'''
Created on Apr 14, 2014

@author: cmills
'''

from tasr import AvroSchemaRepository
from bottle import Bottle, request, abort, response

ASR = AvroSchemaRepository()
app = Bottle()

@app.put('/tasr/topic/<topic_name>')
def register(topic_name=None):
    if not (request.content_type == 'application/json' or request.content_type == 'text/json'):
        abort(406, 'Content-Type must be appliction/json or text/json.')

    _schema_str = request.body.getvalue()
    if _schema_str == None or _schema_str == '':
        abort(400, 'Refusing to register nothing for %s' % topic_name)
    
    try:
        _rs = ASR.register(topic_name, _schema_str)
        if _rs:
            response.set_header('X-Schema-Topic', _rs.topic)
            response.set_header('X-Schema-Version', _rs.version)
            response.set_header('X-Schema-MD5-ID', _rs.md5_id)
            response.set_header('X-Schema-SHA256-ID', _rs.sha256_id)
        if not _rs.is_valid:
            raise Exception('Invalid schema.')
        return
    except Exception as e:
        abort(400, 'Bad schema.  Failed to register.\n%s' % e)

@app.get('/tasr/topic/<topic_name>')
def getLatestForTopic(topic_name=None):
    return "getLatestForTopic: %s\n" % topic_name

if __name__ == "__main__":
    app.run(host='localhost', port=8080, debug=True)