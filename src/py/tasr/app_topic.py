'''
Created on November 18, 2014

@author: cmills

The original /tasr endpoints are defined here.  These should be considered
deprecated and are likely to be removed before the next version.  These are
here to preserve compatibility with some older code.
'''
import avro.schema
import bottle
import tasr.app_wsgi
import tasr.group
import tasr.headers


def abort_if_topic_bad(val, label='topic name'):
    '''Many of the S+V endpoints require a subject name. Check that it is valid
    as well as being non-null.'''
    tasr.app_wsgi.abort_if_value_bad(val, label)
    if not tasr.group.Group.validate_group_name(val):
        tasr.app_wsgi.abort(400, 'Bad %s: %s.' % (label, val))


##############################################################################
# TASR topic API endpoints -- mount to /tasr/topic
##############################################################################
TASR_TOPIC_APP = tasr.app_wsgi.TASRApp()


@TASR_TOPIC_APP.get('/')
def all_topics():
    '''Gets the list of active topics -- that is, topics with at least one
    schema registered.  It returns a text/plain body, one topic name per line.
    '''
    hbot = tasr.headers.SubjectHeaderBot(bottle.response)
    topics = TASR_TOPIC_APP.ASR.get_all_subjects()
    for topic in topics:
        hbot.add_subject_name_current_version(topic)
    bottle.response.content_type = 'text/plain'
    tasr.app_wsgi.log_request()
    return tasr.app_wsgi.collection_as_text_lines(topics, 'name')


@TASR_TOPIC_APP.put('/<topic_name>')
def register_topic_schema(topic_name=None):
    '''Registers a schema for a specified topic name.  Returns the canonical
    form of the schema as the response body if successful.
    '''
    tasr.app_wsgi.abort_if_content_type_not_json(bottle.request)
    tasr.app_wsgi.abort_if_body_empty(bottle.request)
    try:
        reg_fn = TASR_TOPIC_APP.ASR.register_schema
        reg_schema = reg_fn(topic_name, bottle.request.body.getvalue())
        if not reg_schema or not reg_schema.is_valid:
            tasr.app_wsgi.abort(400, 'Invalid schema.  Failed to register.')
        tasr.headers.SchemaHeaderBot(bottle.response,
                                     reg_schema,
                                     topic_name).legacy_headers()
        if reg_schema.created:
            bottle.response.status = 201
        return tasr.app_wsgi.schema_response(reg_schema)
    except avro.schema.SchemaParseException:
        tasr.app_wsgi.abort(400, 'Invalid schema.  Failed to register_schema.')


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
        return tasr.app_wsgi.schema_response(reg_schema)
    # return nothing if there is no schema registered for the group name
    tasr.app_wsgi.abort(404, 'No schema registered for topic %s.' % topic_name)


@TASR_TOPIC_APP.get('/<topic_name>/version/<version>')
def schema_for_topic_and_version(topic_name=None, version=None):
    '''Get a schema registered for the group with a specified version.'''
    abort_if_topic_bad(topic_name, 'topic name')
    tasr.app_wsgi.abort_if_value_bad(version, 'topic version')
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
        return tasr.app_wsgi.schema_response(reg_schema)
    # return nothing if there is no schema registered for the group name
    tasr.app_wsgi.abort(404, 'No schema version %s registered for topic %s.' %
          (version, topic_name))
