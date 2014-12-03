'''
Created on November 18, 2014

@author: cmills

The original /tasr endpoints are defined here.  These should be considered
deprecated and are likely to be removed before the next version.  These are
here to preserve compatibility with some older code.
'''
import avro.schema
import bottle
import json
import tasr.app_wsgi
import tasr.group
import tasr.headers


##############################################################################
# TASR topic API endpoints -- mount to /tasr/topic
##############################################################################
TASR_TOPIC_APP = tasr.app_wsgi.TASRApp()


def abort_if_topic_bad(val, label='topic name'):
    '''Many of the S+V endpoints require a subject name. Check that it is valid
    as well as being non-null.'''
    if val == None or val == '':
        TASR_TOPIC_APP.abort(400, 'Missing %s.' % label)
    if not tasr.group.Group.validate_group_name(val):
        TASR_TOPIC_APP.abort(400, 'Bad %s: %s.' % (label, val))


@TASR_TOPIC_APP.get('/')
def all_topics():
    '''Gets the list of active topics -- that is, topics with at least one
    schema registered.  It returns a text/plain body, one topic name per line.
    '''
    hbot = tasr.headers.SubjectHeaderBot(bottle.response)
    topics = TASR_TOPIC_APP.ASR.get_all_subjects()
    tname_list = []
    for topic in topics:
        hbot.add_subject_name_current_version(topic)
        tname_list.append(topic.name)
    bottle.response.content_type = 'text/plain'
    tasr.app_wsgi.log_request()
    return TASR_TOPIC_APP.object_response(tname_list)


@TASR_TOPIC_APP.put('/<topic_name>')
def register_topic_schema(topic_name=None):
    '''Registers a schema for a specified topic name.  Returns the canonical
    form of the schema as the response body if successful.
    '''
    c_type = str(bottle.request.content_type).split(';')[0].strip()
    if not tasr.app_wsgi.is_json_type(c_type):
        TASR_TOPIC_APP.abort(406, 'Content-Type not JSON.')
    bod = bottle.request.body.getvalue()
    if bod == None or bod == '':
        TASR_TOPIC_APP.abort(400, 'Expected a non-empty request body.')
    try:
        reg_fn = TASR_TOPIC_APP.ASR.register_schema
        reg_schema = reg_fn(topic_name, bottle.request.body.getvalue())
        if not reg_schema or not reg_schema.is_valid:
            TASR_TOPIC_APP.abort(400, 'Invalid schema.  Failed to register.')
        tasr.headers.SchemaHeaderBot(bottle.response,
                                     reg_schema,
                                     topic_name).legacy_headers()
        if reg_schema.created:
            bottle.response.status = 201
        #jobj = json.loads(reg_schema.canonical_schema_str)
        return TASR_TOPIC_APP.object_response(reg_schema.canonical_schema_str,
                                              reg_schema.ordered,
                                              'application/json')
    except ValueError:
        TASR_TOPIC_APP.abort(400, 'Invalid.  Failed to register_schema.')


@TASR_TOPIC_APP.get('/<topic_name>/latest')
@TASR_TOPIC_APP.get('/<topic_name>')
def latest_schema_for_topic(topic_name=None):
    '''Retrieves the registered schema for the specified group with the highest
    version number.
    '''
    abort_if_topic_bad(topic_name, 'topic name')
    reg_schema = TASR_TOPIC_APP.ASR.get_latest_schema_for_group(topic_name)
    if reg_schema:
        # we leave out the topic_name to get back ver & ts for all assoc topics
        tasr.headers.SchemaHeaderBot(bottle.response,
                                     reg_schema).legacy_headers()
        #jobj = json.loads(reg_schema.canonical_schema_str)
        return TASR_TOPIC_APP.object_response(reg_schema.canonical_schema_str,
                                              reg_schema.ordered,
                                              'application/json')
    # return nothing if there is no schema registered for the group name
    TASR_TOPIC_APP.abort(404, 'No schema for topic %s.' % topic_name)


@TASR_TOPIC_APP.get('/<topic_name>/version/<version>')
def schema_for_topic_and_version(topic_name=None, version=None):
    '''Get a schema registered for the group with a specified version.'''
    abort_if_topic_bad(topic_name, 'topic name')
    if version == None or version == '':
        TASR_TOPIC_APP.abort(400, 'Bad topic version.')
    get_fn = TASR_TOPIC_APP.ASR.get_schema_for_group_and_version
    reg_schema = get_fn(topic_name, version)
    if reg_schema:
        '''With multiple schema versions for a group, only the latest is
        included in the retrieved RS.  If we asked for a particular schema
        version, we expect to the RS to list that version number, even if it
        was later re-registered for the same subject.  So, we force the version
        here to be the expected one.
        '''
        reg_schema.gv_dict[topic_name] = version
        # we leave out the topic_name to get back ver & ts for all assoc topics
        tasr.headers.SchemaHeaderBot(bottle.response,
                                     reg_schema).legacy_headers()
        #jobj = json.loads(reg_schema.canonical_schema_str)
        return TASR_TOPIC_APP.object_response(reg_schema.canonical_schema_str,
                                              reg_schema.ordered,
                                              'application/json')
    # return nothing if there is no schema registered for the group name
    TASR_TOPIC_APP.abort(404, ('No version %s registered for topic %s.' %
                               (version, topic_name)))
