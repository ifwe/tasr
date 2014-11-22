'''
Created on November 18, 2014

@author: cmills

The endpoints beginning with /id, /schema and /list are defined here.  These
are meant to be mounted by an umbrella instance of TASRApp.  This

'''
import avro.schema
import bottle
import tasr.app_wsgi
from tasr.headers import SubjectHeaderBot, SchemaHeaderBot


##############################################################################
# /id app - reference schemas by multi-type ID
##############################################################################
TASR_ID_APP = tasr.app_wsgi.TASRApp()


@TASR_ID_APP.get('/<base64_id_str:path>')  # IDs w/ slashes, so :path
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
    tasr.app_wsgi.abort_if_value_bad(base64_id_str, 'base64 ID string')
    reg_schema = TASR_ID_APP.ASR.get_schema_for_id_str(base64_id_str)
    if reg_schema:
        return tasr.app_wsgi.schema_response(reg_schema)
    # return nothing if there is no schema registered for the topic name
    tasr.app_wsgi.abort(404, 'No schema registered with id %s' % base64_id_str)

# TODO: Add /<id>/meta to get metadata as JSON?

##############################################################################
# /schema app - reference schemas by JSON schema text
##############################################################################
TASR_SCHEMA_APP = tasr.app_wsgi.TASRApp()


@TASR_SCHEMA_APP.post('/')
def schema_for_schema_str():
    '''Retrieves the registered schema for the schema string posted.  As with
    schema_for_id_str, this method is not constrained by a topic.  That is,
    we convert the passed schema into a fingerprint, then do the rest as if the
    request were made with that ID string.  This means the response headers
    will be like the ones for schemas retrieved by ID string.
    '''
    tasr.app_wsgi.abort_if_content_type_not_json(bottle.request)
    tasr.app_wsgi.abort_if_body_empty(bottle.request)
    try:
        schema_str = bottle.request.body.getvalue()
        reg_schema = TASR_SCHEMA_APP.ASR.get_schema_for_schema_str(schema_str)
        if reg_schema:
            return tasr.app_wsgi.schema_response(reg_schema)

        # For unregistered schemas, the status is a 404 and the return body is
        # empty, but we add the headers with the MD5 and SHA256 IDs so the
        # client has a way to get the ID values.  We also avoid calling the
        # inherited abort() as it would discard the added ID headers.
        unreg_schema = TASR_SCHEMA_APP.ASR.instantiate_registered_schema()
        unreg_schema.schema_str = schema_str
        SchemaHeaderBot(bottle.response, unreg_schema).set_ids()
        bottle.response.status = 404
        bottle.response.content_type = 'text/plain'
        tasr.app_wsgi.log_request(404)
    except avro.schema.SchemaParseException:
        tasr.app_wsgi.abort(400, 'Invalid schema.  Failed to consider.')


##############################################################################
# /collection app - get lists of objects in the repo
##############################################################################
TASR_COLLECTION_APP = tasr.app_wsgi.TASRApp()


@TASR_COLLECTION_APP.get('/subjects/all')
def all_subject_names():
    '''Get the all the registered subjects, whether or not they have any
    schemas registered.  The S+V API expects this as a plaintext return body
    with one subject per line (using '\n' as delimiters).

    We add X-TASR headers with the subject names as well.  If no Accept header
    is specified (or if it is text/plain), the standard S+V return is used.
    If text/json or application/json is specified, the return body will be a
    JSON document containing current metadata for each subject.
    '''
    hbot = tasr.headers.SubjectHeaderBot(bottle.response)
    subjects = TASR_COLLECTION_APP.ASR.get_all_subjects()
    for subject in subjects:
        hbot.add_subject_name(subject)
    return tasr.app_wsgi.subjects_response(subjects)


@TASR_COLLECTION_APP.get('/subjects/active')
def active_subject_names():
    '''The S+V API expects this as a plaintext return body with one subject per
    line (using '\n' as delimiters).  We add X-TASR headers with the subject
    names as well.
    '''
    hbot = SubjectHeaderBot(bottle.response)
    subjects = TASR_COLLECTION_APP.ASR.get_active_subjects()
    for subject in subjects:
        hbot.add_subject_name(subject)
    return tasr.app_wsgi.subjects_response(subjects)
