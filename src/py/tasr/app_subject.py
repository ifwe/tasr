'''
Created on November 18, 2014

@author: cmills

This is where the /subject (i.e. -- "S+V") endpoints are defined.  In this way
of interacting with the repo, registered schemas are always accessed via a
single, specified subject.  If a given schema is actually associated with
several subjects, that is hidden from view here.  This API also supports
registering subjects "bare" -- that is, without a schema.  The "bare" subjects
are included in lists of "all" subjects, but are excluded from lists of
"active" subjects.
'''
import avro.schema
import bottle
import json
import tasr.app_core
import tasr.app_wsgi
import tasr.group
import tasr.headers


def abort_if_subject_bad(val, label='subject name'):
    '''Many of the S+V endpoints require a subject name. Check that it is valid
    as well as being non-null.'''
    tasr.app_wsgi.abort_if_value_bad(val, label)
    if not tasr.group.Group.validate_group_name(val):
        tasr.app_wsgi.abort(400, 'Bad %s: %s.' % (label, val))


##############################################################################
# TASR Subject API endpoints -- mount to /tasr/subject
##############################################################################
TASR_SUBJECT_APP = tasr.app_wsgi.TASRApp()


@TASR_SUBJECT_APP.get('/')
def all_subject_names():
    '''
    Get the list of all registered subjects, whether or not they have any
    schemas registered.  This duplicates the function of the /list/subject/all
    endpoint and is likely to be deprecated.
    '''
    return tasr.app_core.all_subject_names()


@TASR_SUBJECT_APP.get('/<subject_name>')
def lookup_subject(subject_name=None):
    '''
    Get a subject (group) by name.  If an Accept header for application/json or
    test/json is present, the response is the JSON form of the subject.  If
    the client is not expecting JSON back, the status code of the response can
    be used to check if the subject has already been registered (200) or not
    (404).  The response headers will include the basic subject metadata in any
    case, including the version number, timestamp, md5_id and sha256_id for the
    subject's current schema (if there is one).
    '''
    abort_if_subject_bad(subject_name)  # a malformed subject returns a 400
    subject = TASR_SUBJECT_APP.ASR.lookup_subject(subject_name)
    if not subject:
        tasr.app_wsgi.abort(404, 'Subject %s not registered.' % subject_name)
    return tasr.app_wsgi.subject_response(subject)


@TASR_SUBJECT_APP.put('/<subject_name>')
def register_subject(subject_name=None):
    '''
    Register a subject (i.e. -- initialize a group).  This is implicit using
    the TASR API when a schema is registered, but is a separate method in the
    S+V API.

    S+V accepts a form as the PUT body.  That form, if present, is stored as
    the group config (that is, group metadata).  A 200 status indicates that
    the subject was already registered.  A 201 (created) status indicates that
    it has been added.  Both cases will have TASR headers holding the subject
    metadata in the response.  If a config map is defined and it matches the
    one stored for an existing subject, a 200 is returned.  However, if a
    config map conflicts with a pre-existing one for the subject, a 409
    (conflict) status will be returned.
    '''
    abort_if_subject_bad(subject_name)
    config_dict = tasr.app_wsgi.request_form_to_dict(bottle.request)
    subject = TASR_SUBJECT_APP.ASR.lookup_subject(subject_name)
    if subject:
        # subject already there, so check for conflicts
        if subject.config == config_dict:
            return tasr.app_wsgi.subject_response(subject)
        else:
            tasr.app_wsgi.abort(409, 'Conflicts with existing subject config.')
    else:
        # subject is new, so register it
        TASR_SUBJECT_APP.ASR.register_subject(subject_name, config_dict)
        subject = TASR_SUBJECT_APP.ASR.lookup_subject(subject_name)
        if not subject:
            tasr.app_wsgi.abort(500,
                                'Failed to create subject %s.' % subject_name)
        bottle.response.status = 201
        return tasr.app_wsgi.subject_response(subject)


@TASR_SUBJECT_APP.get('/<subject_name>/config')
def subject_config(subject_name=None):
    '''
    Get the config map for the subject.  The headers contain the normal subject
    metadata, but the body is plaintext following the java.util.properties
    expected format (i.e. -- "<key>:<value>\n" or "<key>=<value>\n").
    '''
    abort_if_subject_bad(subject_name)
    subject = TASR_SUBJECT_APP.ASR.lookup_subject(subject_name)
    if not subject:
        tasr.app_wsgi.abort(404, 'Subject %s not registered.' % subject_name)
    return tasr.app_wsgi.subject_config_response(subject)


@TASR_SUBJECT_APP.post('/<subject_name>/config')
def update_subject_config(subject_name=None):
    '''Replace the config dict for a subject.'''
    abort_if_subject_bad(subject_name)
    config_dict = tasr.app_wsgi.request_form_to_dict(bottle.request)
    subject = TASR_SUBJECT_APP.ASR.lookup_subject(subject_name)
    if not subject:
        tasr.app_wsgi.abort(404, 'Subject %s not registered.' % subject_name)
    if subject.config != config_dict:
        TASR_SUBJECT_APP.ASR.update_subject_config(subject_name, config_dict)
        subject = TASR_SUBJECT_APP.ASR.lookup_subject(subject_name)
    return tasr.app_wsgi.subject_config_response(subject)


@TASR_SUBJECT_APP.get('/<subject_name>/integral')
def subject_integral(subject_name=None):
    '''
    Indicates whether subject ID strings are guaranteed to parse as integers.
    Since we support both versions (which are always positive integers) AND
    multitype IDs (which are base64-encoded bytes), TASR will return False.
    '''
    abort_if_subject_bad(subject_name)
    subject = TASR_SUBJECT_APP.ASR.lookup_subject(subject_name)
    if not subject:
        tasr.app_wsgi.abort(404, 'Subject %s not registered.' % subject_name)
    # not reused (or likely to be), so not broken out into a function
    tasr.headers.SubjectHeaderBot(bottle.response, subject).standard_headers()
    tasr.app_wsgi.log_request(bottle.response.status_code)
    if tasr.app_wsgi.accept_json(bottle.request):
        return tasr.app_wsgi.json_body(False)
    return u'%s\n' % unicode(False)


@TASR_SUBJECT_APP.get('/<subject_name>/all_ids')
def all_subject_ids(subject_name=None):
    '''For this subject, get the SHA256 IDs, in order, of all the registered
    schema versions.  Note that it is possible for a given ID to appear more
    than once in the list.

    For a JSON response, we return a list of ID string values.  The first in
    the list is version number 1.  For text/plain, we return one ID string per
    line.  The standard subject headers are included in all cases.
    '''
    abort_if_subject_bad(subject_name)
    asr = TASR_SUBJECT_APP.ASR
    subject = asr.lookup_subject(subject_name)
    if not subject:
        tasr.app_wsgi.abort(404, 'Subject %s not registered.' % subject_name)
    hbot = tasr.headers.SubjectHeaderBot(bottle.response)
    hbot.standard_headers(subject)
    tasr.app_wsgi.log_request(bottle.response.status_code)
    id_list = []
    for sha256_id in asr.get_all_version_sha256_ids_for_group(subject_name):
        hbot.add_subject_sha256_id_to_list(sha256_id[3:])
        id_list.append(sha256_id[3:])
    if tasr.app_wsgi.accept_json(bottle.request):
        return tasr.app_wsgi.json_body(id_list)
    return tasr.app_wsgi.collection_as_text_lines(id_list)


@TASR_SUBJECT_APP.get('/<subject_name>/all_schemas')
def all_subject_schemas(subject_name=None):
    '''For this subject, get all the registered schema versions, in order, from
    first to last.  Note that it is possible for the same schema to appear more
    than once in the list.

    For a JSON response, we return a list of schema objects (JSON dicts).  The
    first in the list is version number 1.  For text/plain, we return one
    schema (a JSON dict) per line.  The standard subject headers are included
    in all cases.
    '''
    abort_if_subject_bad(subject_name)
    asr = TASR_SUBJECT_APP.ASR
    subject = asr.lookup_subject(subject_name)
    if not subject:
        tasr.app_wsgi.abort(404, 'Subject %s not registered.' % subject_name)
    hbot = tasr.headers.SubjectHeaderBot(bottle.response)
    hbot.standard_headers(subject)
    tasr.app_wsgi.log_request(bottle.response.status_code)

    schema_list = []
    jobj_list = []
    for schema in asr.get_latest_schema_versions_for_group(subject_name, -1):
        hbot.add_subject_sha256_id_to_list(schema.sha256_id)
        schema_list.append(schema.canonical_schema_str)
        jobj_list.append(json.loads(schema.canonical_schema_str))
    if tasr.app_wsgi.accept_json(bottle.request):
        return tasr.app_wsgi.json_body(jobj_list)
    return tasr.app_wsgi.collection_as_text_lines(schema_list)


@TASR_SUBJECT_APP.put('/<subject_name>/register')
def register_subject_schema(subject_name=None):
    '''A method to register_schema a schema for a specified group_name.'''
    tasr.app_wsgi.abort_if_content_type_not_json(bottle.request)
    tasr.app_wsgi.abort_if_body_empty(bottle.request)
    abort_if_subject_bad(subject_name)
    try:
        asr = TASR_SUBJECT_APP.ASR
        reg_schema = asr.register_schema(subject_name,
                                         bottle.request.body.getvalue())
        if not reg_schema or not reg_schema.is_valid:
            tasr.app_wsgi.abort(400, 'Invalid schema.')
        if reg_schema.created:
            bottle.response.status = 201
        return tasr.app_wsgi.schema_response(reg_schema)
    except avro.schema.SchemaParseException:
        tasr.app_wsgi.abort(400, 'Invalid schema.')


@TASR_SUBJECT_APP.put('/<subject_name>/register_if_latest/<version>')
def register_schema_if_latest(subject_name=None, version=None):
    '''If the version is the latest for the subject, then register the schema
    passed as the content body.
    '''
    tasr.app_wsgi.abort_if_value_bad(version)
    asr = TASR_SUBJECT_APP.ASR
    latest_schema = asr.get_latest_schema_for_group(subject_name)
    if int(version) != latest_schema.current_version(subject_name):
        tasr.app_wsgi.abort(409, '%s not latest.' % version)
    return register_subject_schema(subject_name)


@TASR_SUBJECT_APP.post('/<subject_name>/schema')
def lookup_by_schema_str(subject_name=None):
    '''Retrieves the registered schema for the schema string as posted. The S+V
    API specifies a topic (a.k.a -- a subject), which is mostly superfluous for
    our implementation.  This method works with or without a topic specified,
    but if one is, we check that the registered schema is associated with that
    topic before returning the object.  If it wasn't registered for a specified
    topic, we return None (and a 404).
    '''
    tasr.app_wsgi.abort_if_content_type_not_json(bottle.request)
    tasr.app_wsgi.abort_if_body_empty(bottle.request)
    hbot = tasr.headers.SchemaHeaderBot(bottle.response)
    try:
        schema_str = bottle.request.body.getvalue()
        reg_schema = TASR_SUBJECT_APP.ASR.get_schema_for_schema_str(schema_str)
        if reg_schema and subject_name in reg_schema.group_names:
            hbot.standard_headers(reg_schema, subject_name)
            return tasr.app_wsgi.schema_response(reg_schema)

        # For unregistered schemas, the status is a 404 and the return body is
        # empty, but we add the headers with the MD5 and SHA256 IDs so the
        # client has a way to get the ID values.  We also avoid calling the
        # inherited abort() as it would discard the added ID headers.
        unreg_schema = TASR_SUBJECT_APP.ASR.instantiate_registered_schema()
        unreg_schema.schema_str = schema_str
        hbot.set_ids(unreg_schema)
        bottle.response.status = 404
        # TODO: should we return an object with the IDs here if JSON accepted?
    except avro.schema.SchemaParseException:
        tasr.app_wsgi.abort(400, 'Invalid schema.  Failed to consider.')


@TASR_SUBJECT_APP.get('/<subject_name>/version/<version>')
def lookup_by_subject_and_version(subject_name=None, version=None):
    '''Retrieves the registered schema for the specified group_name with the
    specified version number.  Note that versions count from 1, not 0.
    '''
    abort_if_subject_bad(subject_name)
    tasr.app_wsgi.abort_if_value_bad(version, 'subject version')
    asr = TASR_SUBJECT_APP.ASR
    reg_schema = asr.get_schema_for_group_and_version(subject_name, version)
    if not reg_schema:
        tasr.app_wsgi.abort(404,
                            'No schema version %s registered for subject %s.' %
                            (version, subject_name))
    '''With multiple schema versions for a group, only the latest is
    included in the retrieved RS.  If we asked for a particular schema
    version, we expect to the RS to list that version number, even if it
    was later re-registered for the same subject.  So, we force the version
    here to be the expected one.
    '''
    reg_schema.gv_dict[subject_name] = version
    return tasr.app_wsgi.schema_response(reg_schema)


@TASR_SUBJECT_APP.get('/<subject_name>/id/<id_str:path>')
def lookup_by_subject_and_id_str(subject_name=None, id_str=None):
    '''Retrieves the latest version of a schema registered for the specified
    group_name having the provided multi-type ID.
    '''
    abort_if_subject_bad(subject_name)
    tasr.app_wsgi.abort_if_value_bad(id_str, 'multi-type ID string')
    reg_schema = TASR_SUBJECT_APP.ASR.get_schema_for_id_str(id_str)
    if not reg_schema:
        msg = ('No schema with a multi-type ID %s registered for subject %s.' %
               (id_str, subject_name))
        tasr.app_wsgi.abort(404, msg)
    return tasr.app_wsgi.schema_response(reg_schema)


@TASR_SUBJECT_APP.get('/<subject_name>/latest')
def lookup_latest(subject_name=None):
    '''Retrieves the registered schema for the specified group with the highest
    version number.
    '''
    abort_if_subject_bad(subject_name)
    reg_schema = TASR_SUBJECT_APP.ASR.get_latest_schema_for_group(subject_name)
    if not reg_schema:
        msg = 'No schema registered for subject %s.' % subject_name
        tasr.app_wsgi.abort(404, msg)
    return tasr.app_wsgi.schema_response(reg_schema)
