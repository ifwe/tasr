'''
Created on November 18, 2014

@author: cmills

The endpoints beginning with /id, /schema and /list are defined here.  These
are meant to be mounted by an umbrella instance of TASRApp.  This

'''
import avro.schema
import bottle
import tasr.app_wsgi
from tasr.registered_schema import MasterAvroSchema
from tasr.headers import SchemaHeaderBot


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
    if base64_id_str == None or base64_id_str == '':
        TASR_ID_APP.abort(400, 'Missing base64 ID string.')
    reg_schema = TASR_ID_APP.ASR.get_schema_for_id_str(base64_id_str)
    if reg_schema:
        return TASR_ID_APP.schema_response(reg_schema)
    # return nothing if there is no schema registered for the topic name
    TASR_ID_APP.abort(404, 'No schema registered with id %s' % base64_id_str)

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
    c_type = str(bottle.request.content_type).split(';')[0].strip()
    if not tasr.app_wsgi.is_json_type(c_type):
        TASR_SCHEMA_APP.abort(406, 'Content-Type not JSON.')
    bod = bottle.request.body.getvalue()
    if bod == None or bod == '':
        TASR_SCHEMA_APP.abort(400, 'Expected a non-empty request body.')
    try:
        schema_str = bottle.request.body.getvalue()
        reg_schema = TASR_SCHEMA_APP.ASR.get_schema_for_schema_str(schema_str)
        if reg_schema:
            return TASR_SCHEMA_APP.schema_response(reg_schema)

        # For unregistered schemas, the status is a 404 and the return body is
        # empty, but we add the headers with the MD5 and SHA256 IDs so the
        # client has a way to get the ID values.  We also avoid calling the
        # inherited abort() as it would discard the added ID headers.
        unreg_schema = TASR_SCHEMA_APP.ASR.instantiate_registered_schema()
        unreg_schema.schema_str = schema_str
        # back-compatible with self is the best we can check here (and it does
        # check for required defaults and so on to ensure back-compat later.
        try:
            MasterAvroSchema([unreg_schema, ]).is_compatible(unreg_schema)
        except ValueError as verr:
            # schema does not pass back-compat test with self -- likely missing
            # a required field or default
            _msg = verr.message if verr.message else 'Incompatible schema.'
            TASR_SCHEMA_APP.abort(409, _msg)
        # Schema looks valid and unknown
        SchemaHeaderBot(bottle.response, unreg_schema).set_ids()
        bottle.response.status = 404
        errd = TASR_SCHEMA_APP.error_dict(404, 'Schema not registered.')
        return TASR_SCHEMA_APP.object_response(None, errd)
    except ValueError:
        TASR_SCHEMA_APP.abort(400, 'Invalid schema.  Failed to consider.')


##############################################################################
# /collection app - get lists of objects in the repo
##############################################################################
TASR_COLLECTION_APP = tasr.app_wsgi.TASRApp()


def subject_list_response(sub_list):
    '''Given a list of subjects (Group objects), construct a response with all
    the subjects represented.'''
    hbot = tasr.headers.SubjectHeaderBot(bottle.response)
    s_dicts = dict()
    for subject in sub_list:
        hbot.add_subject_name(subject)
        s_dicts[subject.name] = subject.as_dict()
    return TASR_COLLECTION_APP.object_response(sub_list, s_dicts)


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
    subjects = TASR_COLLECTION_APP.ASR.get_all_groups()
    return subject_list_response(subjects)


@TASR_COLLECTION_APP.get('/subjects/active')
def active_subject_names():
    '''The S+V API expects this as a plaintext return body with one subject per
    line (using '\n' as delimiters).  We add X-TASR headers with the subject
    names as well.
    '''
    subjects = TASR_COLLECTION_APP.ASR.get_active_groups()
    return subject_list_response(subjects)
