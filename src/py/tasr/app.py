'''
Created on Apr 14, 2014

@author: cmills

Here we have a WSGI (web server gateway interface) web app supporting a REST
API for the Tagged Avro Schema Repository (TASR).  It is implemented in Bottle,
so it can be run as a stand-alone process (for development), deployed under a
python package that implements a faster WSGI-compliant web server (gunicorn,
FAPWS, tornado), or deployed under a WSGI plugin on a fast, general purpose web
server (e.g. -- nginx + uWSGI or Apache HTTPd + mod_wsgi).

Running in stand-alone mode
---------------------------
To run it as a stand-alone process, either call the main() function explicitly,
or just run it from the command line:

    python app.py

This will fire up with the service running on localhost:8080 and expecting a
Redis instance on localhost:537.  Note that running it this way relies on args
passed in on execution rather

Be aware that running this service in stand-alone mode is not suitable for
prod. Bottle's built-in web server is single-threaded and does not handle all
the standard POSIX signals as you would expect a (well-behaved) daemon to do.
'''

##############################################################################
# First we track down the config/args we intend to use on module import
##############################################################################
import sys
import argparse
from tasr.tasr_config import CONFIG
from ConfigParser import NoSectionError

ENV = 'prod'
CONFIG.set_mode(ENV)
ARGS = None
TASR_APP = None

if __name__ == '__main__':
    ARG_PARSER = argparse.ArgumentParser()
    ARG_PARSER.add_argument('--debug', action='store_true')
    ARG_PARSER.add_argument('--env', default=ENV)

    # if we have a config file, use it for our defaults
    try:
        # use the env arg to set the config file mode
        ARGS = ARG_PARSER.parse_args()
        CONFIG.set_mode(ARGS.env)
        # the mode then determines the arg defaults
        ARG_PARSER.add_argument('--host', default=CONFIG.host)
        ARG_PARSER.add_argument('--port', type=int, default=CONFIG.port)
        ARG_PARSER.add_argument('--redis_host', default=CONFIG.redis_host)
        ARG_PARSER.add_argument('--redis_port',
                                type=int, default=CONFIG.redis_port)
    except NoSectionError:
        # failsafe to hard-coded arg defaults
        CONFIG = None
        ARG_PARSER.add_argument('--host', default='localhost')
        ARG_PARSER.add_argument('--port', type=int, default=8080)
        ARG_PARSER.add_argument('--redis_host', default='localhost')
        ARG_PARSER.add_argument('--redis_port', type=int, default=5379)
    ARGS = ARG_PARSER.parse_args()
else:
    ARGS = CONFIG

##############################################################################
# Next we instantiate the global objects for the module -- the ASR and the APP
# We use globals so that WSGI containers can easily refer to the Bottle app
# object (and so our unit tests can directly hit the ASR).
##############################################################################
from tasr import AvroSchemaRepository
from bottle import Bottle, request, abort, response

ASR = AvroSchemaRepository(host=ARGS.redis_host, port=ARGS.redis_port)
TASR_APP = Bottle()

##############################################################################
# Lastly, with the global ASR and app objects instantiated, we can define the
# methods we will associate with the REST endpoints.  Keep in mind that there
# are really two APIs here: the original TASR API and the subject & version
# (S+V) API from the Avro project's interfaces.  There is a lot of overlap,
# but they do differ.
##############################################################################


def abort_if_content_type_not_json(req):
    '''Many endpoints expect to receive JSON content.'''
    c_type = str(req.content_type).split(';')[0].strip()
    if not c_type in ['application/json', 'text/json']:
        abort(406, 'Content-Type must be appliction/json or text/json.')


def abort_if_body_empty(req):
    '''A common check for PUT and POST endpoints.'''
    bod = req.body.getvalue()
    if bod == None or bod == '':
        abort(400, 'Expected a non-empty request body.')


def abort_if_value_bad(val, label='expected value'):
    '''Many of the S+V endpoints require a subject name.'''
    if val == None or val == '':
        abort(400, 'Missing %s.' % label)


##############################################################################
# S+V API endpoints
##############################################################################
import StringIO
from avro.schema import SchemaParseException
import tasr.headers


@TASR_APP.get('/tasr/subject')
def all_subjects():
    '''The S+V API expects this as a plaintext return body with one subject per
    line (using '\n' as delimiters).  We add X-TASR headers with the subject
    names as well.
    '''
    hbot = tasr.headers.SubjectHeaderBot(response)
    buff = StringIO.StringIO()
    for subject in ASR.get_all_topics():
        hbot.add_subject_name(subject)
        buff.write('%s\n' % subject.name)
    resp_body = buff.getvalue()
    buff.close()
    return resp_body


@TASR_APP.put('/tasr/subject')
def register_subject():
    '''
    Register a subject (i.e. -- initialize a group).  This is implicit using
    the TASR API when a schema is registered, but is a separate method in the
    S+V API.  S+V expects to send the subject name as a form value, not as part
    of the target.  What gets returned is the same as for lookup_subject.
    '''
    subject_name = request.forms.get('subject', None)
    abort_if_value_bad(subject_name)
    subject = ASR.register_subject(subject_name)
    if subject:
        tasr.headers.SubjectHeaderBot(response, subject).standard_headers()
    return


@TASR_APP.get('/tasr/subject/<subject_name>')
def lookup_subject(subject_name=None):
    '''
    Get a subject (group) by name.  This gets the object, which will also pull
    the current schema if one is associated -- though only metadata is sent
    back.  A 200 status code indicates the subject (group) exists, and the
    headers should indicate the version number, timestamp, md5_id and sha256_id
    for the current schema (if there is one).
    '''
    abort_if_value_bad(subject_name)
    subject = ASR.lookup_subject(subject_name)
    if not subject:
        abort(404, 'Subject %s not registered.' % subject_name)
    tasr.headers.SubjectHeaderBot(response, subject).standard_headers()
    return


@TASR_APP.get('/tasr/subject/<subject_name>/config')
def subject_config():
    '''TODO: Gets the subject registration timestamp and any default field
    values associated with a subject.'''
    pass


@TASR_APP.post('/tasr/subject/<subject_name>/config')
def update_subject_config():
    '''TODO: Updates the default field values associated with a subject.'''
    pass


@TASR_APP.put('/tasr/subject/<subject_name>/register')
def register_subject_schema(subject_name=None):
    '''A method to register_schema a schema for a specified group_name.'''
    abort_if_content_type_not_json(request)
    abort_if_body_empty(request)
    try:
        reg_schema = ASR.register_schema(subject_name, request.body.getvalue())
        if not reg_schema or not reg_schema.is_valid:
            abort(400, 'Invalid schema.  Failed to register_schema.')
        tasr.headers.SchemaHeaderBot(response,
                                     reg_schema,
                                     subject_name).standard_headers()
        return reg_schema.canonical_schema_str
    except SchemaParseException:
        abort(400, 'Invalid schema.  Failed to register_schema.')


@TASR_APP.put('/tasr/subject/<subject_name>/id/<version>/register_if_latest')
def register_schema_if_latest():
    '''TODO: If the version is the latest for the subject, then register the
    schema passed as the content body.'''
    pass


@TASR_APP.post('/tasr/subject/<subject_name>/schema')
def lookup_by_schema_str(subject_name=None):
    '''Retrieves the registered schema for the schema string as posted. The S+V
    API specifies a topic (a.k.a -- a subject), which is mostly superfluous for
    our implementation.  This method works with or without a topic specified,
    but if one is, we check that the registered schema is associated with that
    topic before returning the object.  If it wasn't registered for a specified
    topic, we return None (and a 404).
    '''
    abort_if_content_type_not_json(request)
    abort_if_body_empty(request)
    hbot = tasr.headers.SchemaHeaderBot(response)
    try:
        schema_str = request.body.getvalue()
        reg_schema = ASR.get_schema_for_schema_str(schema_str)
        if reg_schema and subject_name in reg_schema.group_names:
            hbot.standard_headers(reg_schema, subject_name)
            return reg_schema.canonical_schema_str

        # For unregistered schemas, the status is a 404 and the return body is
        # empty, but we add the headers with the MD5 and SHA256 IDs so the
        # client has a way to get the ID values.  We also avoid calling the
        # inherited abort() as it would discard the added ID headers.
        unreg_schema = ASR.instantiate_registered_schema()
        unreg_schema.schema_str = schema_str
        hbot.set_ids(unreg_schema)
        response.status = 404
    except SchemaParseException:
        abort(400, 'Invalid schema.  Failed to consider.')


@TASR_APP.get('/tasr/subject/<subject_name>/id/<version>')
def lookup_by_subject_and_version(subject_name=None, version=None):
    '''Retrieves the registered schema for the specified group_name with the
    specified version number.  This correlates to the lookupById() Subject
    method.  Note that versions count from 1, not 0.
    '''
    abort_if_value_bad(subject_name, 'subject name')
    abort_if_value_bad(version, 'subject version')
    reg_schema = ASR.get_schema_for_group_and_version(subject_name, version)
    if reg_schema:
        '''With multiple schema versions for a group, only the latest is
        included in the retrieved RS.  If we asked for a particular schema
        version, we expect to the RS to list that version number, even if it
        was later re-registered for the same subject.  So, we force the version
        here to be the expected one.
        '''
        reg_schema.gv_dict[subject_name] = version
        tasr.headers.SchemaHeaderBot(response,
                                     reg_schema,
                                     subject_name).standard_headers()
        return reg_schema.canonical_schema_str
    # return nothing if there is no schema registered for the group name
    abort(404, 'No schema version %s registered for subject %s.' %
          (version, subject_name))


@TASR_APP.get('/tasr/subject/<subject_name>/latest')
def lookup_latest(subject_name=None):
    '''Retrieves the registered schema for the specified group with the highest
    version number.
    '''
    abort_if_value_bad(subject_name, 'subject name')
    reg_schema = ASR.get_latest_schema_for_group(subject_name)
    if reg_schema:
        tasr.headers.SchemaHeaderBot(response,
                                     reg_schema,
                                     subject_name).standard_headers()
        return reg_schema.canonical_schema_str
    # return nothing if there is no schema registered for the group name
    abort(404, 'No schema registered for subject %s.' % subject_name)


@TASR_APP.get('/tasr/subject/<subject_name>/all')
def lookup_all():
    '''TODO: Get all the schemas for the group, from first to last.'''
    pass

##############################################################################
# TASR API endpoints
##############################################################################


@TASR_APP.get('/tasr/topic')
def all_topics():
    '''This is basically the same as get_all_subjects, but we may want to use
    different header values for TASR vs S+V calls.'''
    hbot = tasr.headers.SubjectHeaderBot(response)
    buff = StringIO.StringIO()
    for topic in ASR.get_all_topics():
        hbot.add_subject_name_current_version(topic)
        buff.write('%s\n' % topic.name)
    resp_body = buff.getvalue()
    buff.close()
    return resp_body


@TASR_APP.get('/tasr/topic/<topic_name>/config')
def topic_config():
    '''TODO: Get the field defaults and other config data for the group.'''
    pass


@TASR_APP.put('/tasr/topic/<topic_name>')
def register_topic_schema(topic_name=None):
    '''A method to register a schema for a specified topic name.  The logic is
    shared with register_subject_schema, but the headers can differ between the
    TASR and S+V APIs.  Also, we return the canonical form of the schema so the
    client has that available.
    '''
    abort_if_content_type_not_json(request)
    abort_if_body_empty(request)
    try:
        reg_schema = ASR.register_schema(topic_name, request.body.getvalue())
        if not reg_schema or not reg_schema.is_valid:
            abort(400, 'Invalid schema.  Failed to register_schema.')
        tasr.headers.SchemaHeaderBot(response,
                                     reg_schema,
                                     topic_name).all_headers()
        return reg_schema.canonical_schema_str
    except SchemaParseException:
        abort(400, 'Invalid schema.  Failed to register_schema.')


@TASR_APP.get('/tasr/topic/<topic_name>/latest')
@TASR_APP.get('/tasr/topic/<topic_name>')
def latest_schema_for_topic(topic_name=None):
    '''Retrieves the registered schema for the specified group with the highest
    version number.  This is the same logic as lookup_latest, but may return
    different response headers.
    '''
    abort_if_value_bad(topic_name, 'topic name')
    reg_schema = ASR.get_latest_schema_for_group(topic_name)
    if reg_schema:
        # we leave out the topic_name to get back ver & ts for all assoc topics
        tasr.headers.SchemaHeaderBot(response, reg_schema).all_headers()
        return reg_schema.canonical_schema_str
    # return nothing if there is no schema registered for the group name
    abort(404, 'No schema registered for topic %s.' % topic_name)


@TASR_APP.get('/tasr/topic/<topic_name>/version/<version>')
def schema_for_topic_and_version(topic_name=None, version=None):
    '''Same logic as lookup_by_subject_and_version -- that is, get a schema
    registered for the group with a specified version.  The method is here to
    allow the TASR API to use different response headers, if needed.
    '''
    abort_if_value_bad(topic_name, 'topic name')
    abort_if_value_bad(version, 'topic version')
    reg_schema = ASR.get_schema_for_group_and_version(topic_name, version)
    if reg_schema:
        '''With multiple schema versions for a group, only the latest is
        included in the retrieved RS.  If we asked for a particular schema
        version, we expect to the RS to list that version number, even if it
        was later re-registered for the same subject.  So, we force the version
        here to be the expected one.
        '''
        reg_schema.gv_dict[topic_name] = version
        # we leave out the topic_name to get back ver & ts for all assoc topics
        tasr.headers.SchemaHeaderBot(response, reg_schema).all_headers()
        return reg_schema.canonical_schema_str
    # return nothing if there is no schema registered for the group name
    abort(404, 'No schema version %s registered for topic %s.' %
          (version, topic_name))


@TASR_APP.get('/tasr/id/<base64_id_str:path>')  # IDs w/ slashes, so :path
def schema_for_id_str(base64_id_str=None):
    '''Retrieves a schema registered to one or more groups as identified by an
    ID string.  The ID string is a base64-encoded 'fingerprint' of the schema
    (a leading flag byte followed by an md5 or sha256 digest of the canonical
    form of the schema JSON doc).

    Schemas retrieved this way can be associated with multiple groups (and
    represent multiple versions for a given group if there were non-sequential
    re-registrations).  As such, the X-SCHEMA headers with the metadata can be
    more complex.  There will be current topic-version entries for each group
    the schema was registered for.  If it has multiple versions for a group,
    only the most recent will be included.

    This access method is not currently supported in the S+V API, or by the
    standard Avro (1124-type) schema repository.  It is only really possible
    with the multi-type ID.
    '''
    abort_if_value_bad(base64_id_str, 'base64 ID string')
    reg_schema = ASR.get_schema_for_id_str(base64_id_str)
    if reg_schema:
        tasr.headers.SchemaHeaderBot(response, reg_schema).all_headers()
        return reg_schema.canonical_schema_str
    # return nothing if there is no schema registered for the topic name
    abort(404, 'No schema registered with id %s' % base64_id_str)


@TASR_APP.post('/tasr/schema')
def schema_for_schema_str():
    '''Retrieves the registered schema for the schema string posted.  As with
    schema_for_id_str, this method is not constrained by a topic.  That is,
    we convert the passed schema into a fingerprint, then do the rest as if the
    request were made with that ID string.  This means the response headers
    will be like the ones for schemas retrieved by ID string.
    '''
    abort_if_content_type_not_json(request)
    abort_if_body_empty(request)
    try:
        schema_str = request.body.getvalue()
        reg_schema = ASR.get_schema_for_schema_str(schema_str)
        if reg_schema:
            tasr.headers.SchemaHeaderBot(response, reg_schema).all_headers()
            return reg_schema.canonical_schema_str

        # For unregistered schemas, the status is a 404 and the return body is
        # empty, but we add the headers with the MD5 and SHA256 IDs so the
        # client has a way to get the ID values.  We also avoid calling the
        # inherited abort() as it would discard the added ID headers.
        unreg_schema = ASR.instantiate_registered_schema()
        unreg_schema.schema_str = schema_str
        tasr.headers.SchemaHeaderBot(response, unreg_schema).set_ids()
        response.status = 404
    except SchemaParseException:
        abort(400, 'Invalid schema.  Failed to consider.')


def main(out=sys.stdout, err=sys.stderr):
    '''Run the app in bottle's built-in WSGI container.'''
    sys.stdout = out
    sys.stderr = err
    if CONFIG:
        sys.stdout.write('TASRConfig [%s:%s, redis: [%s:%s] ]\n' %
                         (CONFIG.host, CONFIG.port, CONFIG.redis_host,
                          CONFIG.redis_port))
    sys.stdout.write('TASR ARGS [%s:%s, redis: [%s:%s] ] starting up...\n' %
                     (ARGS.host, ARGS.port, ARGS.redis_host, ARGS.redis_port))
    sys.stdout.flush()
    TASR_APP.run(host=ARGS.host, port=ARGS.port)


if __name__ == "__main__":
    main(sys.stdout, sys.stderr)
