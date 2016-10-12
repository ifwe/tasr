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
import base64
import bottle
import hashlib
import io
import json
import re
import requests
import struct
import tasr.app_core
import tasr.app_wsgi
import tasr.group
import tasr.headers
import tasr.registered_schema


##############################################################################
# TASR Subject API endpoints -- mount to /tasr/subject
##############################################################################
TASR_SUBJECT_APP = tasr.app_wsgi.TASRApp()


def abort_if_value_bad(val, label='expected value'):
    '''Bail if val is None or an empty string.'''
    if val is None or val == '':
        TASR_SUBJECT_APP.abort(400, 'Missing %s.' % label)


def abort_if_subject_bad(val, label='subject name'):
    '''Many of the S+V endpoints require a subject name. Check that it is valid
    as well as being non-null.'''
    abort_if_value_bad(val, label)
    if not tasr.group.Group.validate_group_name(val):
        TASR_SUBJECT_APP.abort(400, 'Bad %s: %s.' % (label, val))


def abort_if_content_type_not_json():
    '''Many endpoints expect to receive JSON content.'''
    c_type = str(bottle.request.content_type).split(';')[0].strip()
    if not tasr.app_wsgi.is_json_type(c_type):
        TASR_SUBJECT_APP.abort(406, 'Content-Type not JSON.')


def abort_if_body_empty():
    '''A common check for PUT and POST endpoints.'''
    bod = bottle.request.body.getvalue()
    if bod is None or bod == '':
        TASR_SUBJECT_APP.abort(400, 'Expected a non-empty request body.')


def get_subject(subject_name):
    '''Getting the subject object is common enough to be a method'''
    abort_if_subject_bad(subject_name)
    subject = TASR_SUBJECT_APP.ASR.lookup_group(subject_name)
    if not subject:
        TASR_SUBJECT_APP.abort(404, 'No subject %s.' % subject_name)
    return subject


@TASR_SUBJECT_APP.get('/')
def all_subject_names():
    '''
    Get the list of all registered subjects, whether or not they have any
    schemas registered.  This duplicates the function of the /list/subject/all
    endpoint and is likely to be deprecated.  The core app method logs the
    request, so there is no need to do it here as well.
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
    return TASR_SUBJECT_APP.subject_response(get_subject(subject_name))


@TASR_SUBJECT_APP.put('/<subject_name>')
def register_subject(subject_name=None):
    '''
    Register a subject (i.e. -- initialize a group).  S+V accepts a form as the
    PUT body.  That form, if present, is stored as the group config (that is,
    group metadata).  A 200 status indicates that the subject was already
    registered.  A 201 (created) status indicates that it has been added.  Both
    cases will have TASR headers holding the subject metadata in the response.
    If a config map is defined and it matches the one stored for an existing
    subject, a 200 is returned.  However, if a config map conflicts with a pre-
    existing one for the subject, a 409 (conflict) status will be returned.
    '''
    abort_if_subject_bad(subject_name)
    config_dict = TASR_SUBJECT_APP.request_data_to_dict()
    subject = TASR_SUBJECT_APP.ASR.lookup_group(subject_name)
    if subject:
        # subject already there, so check for conflicts
        diff_set = set(subject.config.keys()) ^ set(config_dict.keys())
        if len(diff_set) > 0:
            _msg = 'Conflict.  Mismatched keys: %s' % diff_set
            TASR_SUBJECT_APP.abort(409, _msg)
        else:
            # keys are the same, so check values
            for k, v in subject.config.iteritems():
                if not v == config_dict[k]:
                    _msg = ('Conflict.  Value mismatch for key %s (%s != %s)' %
                            (k, v, config_dict[k]))
                    TASR_SUBJECT_APP.abort(409, _msg)
        # no change, so do nothing and return a 200
        return TASR_SUBJECT_APP.subject_response(subject)
    else:
        # subject is new, so register it -- config keys need "config." prefix
        # to be included in the metadata_dict for the constructor
        metadata_dict = dict()
        for key, val in config_dict.iteritems():
            ckey = 'config.%s' % key
            metadata_dict[ckey] = val
        TASR_SUBJECT_APP.ASR.register_group(subject_name, metadata_dict)
        subject = TASR_SUBJECT_APP.ASR.lookup_group(subject_name)
        if not subject:
            TASR_SUBJECT_APP.abort(500, ('Failed to create subject %s.'
                                         % subject_name))
        bottle.response.status = 201
        return TASR_SUBJECT_APP.subject_response(subject)


@TASR_SUBJECT_APP.delete('/<subject_name>')
def delete_subject(subject_name=None):
    '''Deletes a subject, it's metadata, and all of its schema versions.
    Schemas that are versions of other subjects remain.'''
    abort_if_subject_bad(subject_name)
    if not TASR_SUBJECT_APP.config.expose_delete:
        TASR_SUBJECT_APP.abort(403, 'Subject deletes are not enabled.')
    try:
        TASR_SUBJECT_APP.ASR.delete_group(subject_name)
    except ValueError:
        TASR_SUBJECT_APP.abort(404, 'No %s subject.' % subject_name)


@TASR_SUBJECT_APP.get('/<subject_name>/config')
def subject_config(subject_name=None):
    '''
    Get the config map for the subject.  The headers contain the normal subject
    metadata, but the body is plaintext following the java.util.properties
    expected format (i.e. -- "<key>:<value>\n" or "<key>=<value>\n").
    '''
    abort_if_subject_bad(subject_name)
    subject = get_subject(subject_name)
    return TASR_SUBJECT_APP.subject_config_response(subject)


@TASR_SUBJECT_APP.post('/<subject_name>/config')
def update_subject_config(subject_name=None):
    '''
    Replace the _whole_ config dict for a subject.  The config entries are a
    subset of the subject metadata with a "config." prefix.
    '''
    abort_if_subject_bad(subject_name)
    # figure out the dict to set
    config_dict = dict()
    for key, val in TASR_SUBJECT_APP.request_data_to_dict().iteritems():
        ckey = 'config.%s' % key
        config_dict[ckey] = val
    asr = TASR_SUBJECT_APP.ASR
    # clear out all old config entries first to avoid unexpected leftovers
    asr.delete_prefixed_group_metadata_entries(subject_name, 'config.')
    asr.set_group_metadata(subject_name, config_dict)
    subject = TASR_SUBJECT_APP.ASR.lookup_group(subject_name)
    return TASR_SUBJECT_APP.subject_config_response(subject)


@TASR_SUBJECT_APP.get('/<subject_name>/config/<key>')
def get_subject_config_entry(subject_name=None, key=None):
    '''Get the value for the KEY in config dict for a subject.'''
    subject = get_subject(subject_name)
    if key not in subject.config:
        TASR_SUBJECT_APP.abort(404, ('No %s in config for %s.' %
                                     (key, subject_name)))
    return TASR_SUBJECT_APP.subject_config_entry_response(subject, key)


@TASR_SUBJECT_APP.post('/<subject_name>/config/<key>')
def update_subject_config_entry(subject_name=None, key=None):
    '''Set or replace the value for the KEY in config dict for a subject.'''
    abort_if_subject_bad(subject_name)
    val = bottle.request.body.getvalue()
    ckey = 'config.%s' % key
    TASR_SUBJECT_APP.ASR.set_group_metadata_entry(subject_name, ckey, val)
    subject = get_subject(subject_name)
    return TASR_SUBJECT_APP.subject_config_response(subject)


@TASR_SUBJECT_APP.delete('/<subject_name>/config/<key>')
def delete_subject_config_entry(subject_name=None, key=None):
    '''Remove the KEY in config dict for a subject.'''
    abort_if_subject_bad(subject_name)
    ckey = 'config.%s' % key
    TASR_SUBJECT_APP.ASR.delete_group_metadata_entry(subject_name, ckey)
    subject = get_subject(subject_name)
    return TASR_SUBJECT_APP.subject_config_response(subject)


@TASR_SUBJECT_APP.put('/<subject_name>/anchor_id/<id_str:path>')
def set_subject_anchor_version(subject_name=None, id_str=None):
    '''Sets the specified ID string as the 'anchor' version for the subject.
    The anchor version, if set, is considered the starting version for purposes
    of compatibility.  All endpoints that consider multiple versions will
    ignore any versions prior to the anchor, if set.  Specifically, the:
    all_ids, all_schemas, master, and all redshift/* endpoints
    WILL NOT CONSIDER PRE-ANCHOR VERSIONS.

    Note this is group metadata, but not considered part of the config.  The
    reason is that an anchor version should really be out of sight in casual
    use.  If we set an anchor version, the rest of TASR should behave as if the
    pre-anchor versions are just not there.
    '''
    abort_if_subject_bad(subject_name)
    abort_if_value_bad(id_str, 'multi-type ID string')
    asr = TASR_SUBJECT_APP.ASR

    id_list = []
    for sha256_id in asr.get_all_version_sha256_ids_for_group(subject_name):
        id_list.append(sha256_id[3:])
    if id_str not in id_list:
        TASR_SUBJECT_APP.abort(404, 'No %s version with ID %s' %
                               (subject_name, id_str))

    # version is real, set the 'anchor' metadata value to the version ID
    asr.set_group_metadata_entry(subject_name, 'anchor', id_str)

    # return the anchor schema
    idx = id_list.index(id_str)
    aschema = asr.get_schema_for_group_and_version(subject_name, idx)
    bottle.response.status = 201
    return TASR_SUBJECT_APP.schema_response(aschema, subject_name)


@TASR_SUBJECT_APP.get('/<subject_name>/anchor_id')
def get_subject_anchor_version(subject_name=None):
    '''Returns an anchor version SHA ID if one has been set.  If no anchor
    version has been set, a 404 is returned.'''
    abort_if_subject_bad(subject_name)
    smd = TASR_SUBJECT_APP.ASR.get_group_metadata(subject_name)
    if 'anchor' not in smd:
        TASR_SUBJECT_APP.abort(404, 'No anchor set for %s.' % subject_name)
    return smd['anchor']


@TASR_SUBJECT_APP.delete('/<subject_name>/anchor_id')
def unset_subject_anchor_version(subject_name=None):
    '''Deletes the 'anchor' group metadata entry along with any set value.'''
    abort_if_subject_bad(subject_name)
    TASR_SUBJECT_APP.ASR.delete_group_metadata_entry(subject_name, 'anchor')


@TASR_SUBJECT_APP.get('/<subject_name>/integral')
def subject_integral(subject_name=None):
    '''
    Indicates whether subject ID strings are guaranteed to parse as integers.
    Since we support both versions (which are always positive integers) AND
    multitype IDs (which are base64-encoded bytes), TASR will return False.
    '''
    subject = get_subject(subject_name)
    # not reused (or likely to be), so not broken out into a function
    tasr.headers.SubjectHeaderBot(bottle.response, subject).standard_headers()
    return TASR_SUBJECT_APP.object_response(False)


@TASR_SUBJECT_APP.get('/<subject_name>/all_ids')
def all_subject_ids(subject_name=None):
    '''For this subject, get the SHA256 IDs, in order, of all the registered
    schema versions.  Note that it is possible for a given ID to appear more
    than once in the list.

    For a JSON response, we return a list of ID string values.  The first in
    the list is version number 1.  For text/plain, we return one ID string per
    line.  The standard subject headers are included in all cases.
    '''
    subject = get_subject(subject_name)
    hbot = tasr.headers.SubjectHeaderBot(bottle.response)
    hbot.standard_headers(subject)
    tasr.app_wsgi.log_request(bottle.response.status_code)
    asr = TASR_SUBJECT_APP.ASR
    id_list = []
    for sha256_id in asr.get_all_version_sha256_ids_for_group(subject_name):
        hbot.add_subject_sha256_id_to_list(sha256_id[3:])
        id_list.append(sha256_id[3:])
    return TASR_SUBJECT_APP.object_response(id_list)


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
    subject = get_subject(subject_name)
    asr = TASR_SUBJECT_APP.ASR
    hbot = tasr.headers.SubjectHeaderBot(bottle.response)
    hbot.standard_headers(subject)
    tasr.app_wsgi.log_request(bottle.response.status_code)

    schema_list = []
    for schema in asr.get_latest_schema_versions_for_group(subject_name, -1):
        hbot.add_subject_sha256_id_to_list(schema.sha256_id)
        schema_list.append(schema.json)
    return TASR_SUBJECT_APP.object_response(schema_list, None)


def get_anchored_version_list(subject_name):
    asr = TASR_SUBJECT_APP.ASR
    olds = asr.get_latest_schema_versions_for_group(subject_name, -1)
    # if the group has an anchor version set, ignore prior versions
    smd = TASR_SUBJECT_APP.ASR.get_group_metadata(subject_name)
    if 'anchor' in smd:
        anchor_version = smd['anchor']
        anchored = False
        anchored_olds = []
        for ver in olds:
            if ver.sha256_id == anchor_version:
                anchored_olds.append(ver)
                anchored = True
            elif anchored:
                anchored_olds.append(ver)
        olds = anchored_olds
    return olds


def get_master_sha256_id(subject_name):
    '''Grabs all the SHA256 version id strings for the subject, concatenates
    them into a single string and hashes that to get a master id we can use to
    check for a cached master schema.
    '''
    buf = io.BytesIO()
    buf.write(struct.pack('>b', tasr.registered_schema.SHA256_BYTES))
    sha = hashlib.sha256()
    for ras in get_anchored_version_list(subject_name):
        sha.update(ras.sha256_id)
    buf.write(sha.digest())
    id_bytes = buf.getvalue()
    buf.close()
    return base64.b64encode(id_bytes)


def recursive_master_schema(versions):
    '''Takes a list of versions and creates a "master", containing all the
    fields from the most recent compatible versions. It tries on the whole
    list, and if it fails it recurses, trying on the list minus its head.
    '''
    try:
        mas = tasr.registered_schema.MasterAvroSchema(versions)
        return (len(versions), mas)
    except Exception:
        if len(versions) > 1:
            return recursive_master_schema(versions[1:])
        return (0, None)


@TASR_SUBJECT_APP.get('/<subject_name>/master')
def subject_master_schema(subject_name=None):
    '''Get the MasterAvroSchema for all the versions.  This includes all of
    the fields defined in any version for the group.  This can be used to
    build the Hive tables that cover all the versions.

    Note that if the versions are incompatible, the master will be composed
    from the most recent compatible versions.  If there is an anchor version
    set, the master will include versions from the anchor forward.'''
    abort_if_subject_bad(subject_name)
    # first try the cached masters
    asr = TASR_SUBJECT_APP.ASR
    master_id = get_master_sha256_id(subject_name)
    mas_d = asr.get_master_dict_for_sha256_id(master_id)
    if mas_d and 'schema' in mas_d:
        return TASR_SUBJECT_APP.json_str_response(mas_d['schema'])

    # with no cached master, create one
    versions = get_anchored_version_list(subject_name)
    if not versions or len(versions) == 0:
        TASR_SUBJECT_APP.abort(404, ('No versions registered for %s.'
                                     % subject_name))
    (depth, mas) = recursive_master_schema(versions)
    mas_str = json.dumps(mas.json_obj)
    # we might want to add a response header to indicate master depth
    if depth < len(versions):
        # master based on an incomplete set of versions
        bottle.response.status = 409
    else:
        # cache masters based on complete version sets
        asr.set_master_dict_entry(master_id, 'schema', mas_str)
    return TASR_SUBJECT_APP.json_str_response(mas_str)


def is_back_compatible(subject_name, schema_str):
    '''A convenience method that checks whether a given schema string is back
    compatible with all the previously registered schema versions (or those
    after the anchor version if one has been set).  This is used whenever we
    are considering registering a new schema version.'''
    asr = TASR_SUBJECT_APP.ASR
    # instantiate a RAS object with the passed schema string
    unreg_schema = asr.instantiate_registered_schema()
    unreg_schema.schema_str = schema_str
    # now grab all the previous schemas from anchor up to the latest
    olds = get_anchored_version_list(subject_name)

    # check that the new schema will be back-compatible -- note that by using
    # the MAS, we are allowing exceptions to bubble up.  This is to allow more
    # informative, field-specific errors to make it to the response.
    if olds and len(olds) > 0:
        mas = tasr.registered_schema.MasterAvroSchema(olds)
        return mas.is_compatible(unreg_schema)
    return True


def update_hdfs_master(subject_name):
    if not TASR_SUBJECT_APP.config.push_masters_to_hdfs:
        return None
    app = TASR_SUBJECT_APP
    versions = app.ASR.get_latest_schema_versions_for_group(subject_name, -1)
    mas = recursive_master_schema(versions)[1]
    normalized_subject_name = re.sub(r"^s_", "", subject_name)
    base_url = '%s%s/%s' % (app.config.webhdfs_url,
                            app.config.hdfs_master_path,
                            normalized_subject_name)
    url = '%s?user.name=%s' % (base_url, app.config.webhdfs_user)
    # first get the current master from HDFS
    resp = requests.get('%s&op=OPEN' % url)
    old_master_str = resp.content if resp else None
    # if there is an old master that matches, reference it and we're done
    if old_master_str and old_master_str == mas.canonical_schema_str:
        bottle.response.add_header('X-TASR-HDFS-MASTER-PATH', base_url)
        return False
    # there is either no existing master or it's out of date, so write to HDFS
    resp = requests.put('%s&op=CREATE&overwrite=true&permission=644' % url,
                        data=mas.canonical_schema_str,
                        headers={'Content-Type': 'application/octet-stream'})
    resp_date = resp.headers['date']
    if resp.status_code >= 200 and resp.status_code < 300:
        bottle.response.add_header('X-TASR-HDFS-MASTER-PATH', base_url)
        bottle.response.add_header('X-TASR-HDFS-MASTER-UPDATED', resp_date)
        return True
    return None


def register_subject_schema(subject_name=None):
    '''A method to register a schema for a specified group_name. This method
    skips the back-compatibility checks, so use it with care. Note that a
    "master" for an incompatible sequence of schemas will only include fields
    from the most recently compatible sequence.
    '''
    abort_if_subject_bad(subject_name)
    abort_if_content_type_not_json()
    abort_if_body_empty()
    try:
        schema_str = bottle.request.body.getvalue()
        asr = TASR_SUBJECT_APP.ASR
        reg_schema = asr.register_schema(subject_name, schema_str)
        if not reg_schema or not reg_schema.is_valid:
            TASR_SUBJECT_APP.abort(400, 'Invalid schema.')
        if reg_schema.created:
            bottle.response.status = 201
            update_hdfs_master(subject_name)
        return TASR_SUBJECT_APP.schema_response(reg_schema, subject_name)
    except avro.schema.SchemaParseException:
        TASR_SUBJECT_APP.abort(400, 'Invalid schema.')
    except ValueError:
        TASR_SUBJECT_APP.abort(400, 'Invalid schema.')


@TASR_SUBJECT_APP.put('/<subject_name>/register')
def register_compatible_subject_schema(subject_name=None):
    '''A method to register a back-compatible schema for a specified group.
    This will reject the request on a 409 if the fields are not a valid
    extension of the previous schemas.  If you must override this, use the
    force_register endpoint (register_subject_schema method) instead.
    '''
    abort_if_subject_bad(subject_name)
    abort_if_content_type_not_json()
    abort_if_body_empty()
    schema_str = bottle.request.body.getvalue()
    try:
        if not is_back_compatible(subject_name, schema_str):
            _msg = 'Schema not compatible with previous versions.'
            TASR_SUBJECT_APP.abort(409, _msg)
    except ValueError as verr:
        _msg = verr.message if verr.message else 'Incompatible schema.'
        TASR_SUBJECT_APP.abort(409, _msg)
    # the new schema is valid and compatible, so register it
    return register_subject_schema(subject_name)


@TASR_SUBJECT_APP.put('/<subject_name>/force_register')
def force_register_subject_schema(subject_name=None):
    if not TASR_SUBJECT_APP.config.expose_force_register:
        TASR_SUBJECT_APP.abort(403, 'Forced schema registration not enabled.')
    return register_subject_schema(subject_name)


@TASR_SUBJECT_APP.put('/<subject_name>/register_if_latest/<version>')
def register_schema_if_latest(subject_name=None, version=None):
    '''If the version is the latest for the subject, then register the schema
    passed as the content body.
    '''
    abort_if_value_bad(version, 'version')
    asr = TASR_SUBJECT_APP.ASR
    latest_schema = asr.get_latest_schema_for_group(subject_name)
    if int(version) != latest_schema.current_version(subject_name):
        TASR_SUBJECT_APP.abort(409, '%s not latest.' % version)
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
    abort_if_subject_bad(subject_name)
    abort_if_content_type_not_json()
    abort_if_body_empty()
    try:
        schema_str = bottle.request.body.getvalue()
        asr = TASR_SUBJECT_APP.ASR
        reg_schema = asr.get_schema_for_schema_str(schema_str)
        if reg_schema and subject_name in reg_schema.group_names:
            return TASR_SUBJECT_APP.schema_response(reg_schema, subject_name)

        # check that schema has all required fields for back-compat -- if not,
        # the raised exception will abort on a 400 right away
        unreg_schema = asr.instantiate_registered_schema()
        unreg_schema.schema_str = schema_str
        tasr.registered_schema.MasterAvroSchema([unreg_schema, ])
        # since the schema is OK, check back-compat
        try:
            if not is_back_compatible(subject_name, schema_str):
                _msg = 'Schema not compatible with previous versions.'
                TASR_SUBJECT_APP.abort(409, _msg)
        except ValueError as verr:
            _msg = verr.message if verr.message else 'Incompatible schema.'
            TASR_SUBJECT_APP.abort(409, _msg)

        # For unregistered schemas, the status is a 404 and the return body is
        # empty, but we add the headers with the MD5 and SHA256 IDs so the
        # client has a way to get the ID values.  We also avoid calling the
        # inherited abort() as it would discard the added ID headers.
        hbot = tasr.headers.SchemaHeaderBot(bottle.response)
        hbot.set_ids(unreg_schema)
        bottle.response.status = 404
        # TODO: should we return an object with the IDs here if JSON accepted?
    except avro.schema.SchemaParseException:
        TASR_SUBJECT_APP.abort(400, 'Invalid schema.  Failed to consider.')
    except ValueError:
        TASR_SUBJECT_APP.abort(400, 'Invalid schema.  Failed to consider.')


@TASR_SUBJECT_APP.get('/<subject_name>/version/<version>')
def lookup_by_subject_and_version(subject_name=None, version=None):
    '''Retrieves the registered schema for the specified group_name with the
    specified version number.  Note that versions count from 1, not 0.
    '''
    abort_if_subject_bad(subject_name)
    abort_if_value_bad(version, 'version')
    asr = TASR_SUBJECT_APP.ASR
    reg_schema = asr.get_schema_for_group_and_version(subject_name, version)
    if not reg_schema:
        TASR_SUBJECT_APP.abort(404, ('No version %s registered for subject %s.'
                                     % (version, subject_name)))
    '''With multiple schema versions for a group, only the latest is
    included in the retrieved RS.  If we asked for a particular schema
    version, we expect to the RS to list that version number, even if it
    was later re-registered for the same subject.  So, we force the version
    here to be the expected one.
    '''
    reg_schema.gv_dict[subject_name] = version
    return TASR_SUBJECT_APP.schema_response(reg_schema, subject_name)


@TASR_SUBJECT_APP.get('/<subject_name>/id/<id_str:path>')
def lookup_by_subject_and_id_str(subject_name=None, id_str=None):
    '''Retrieves the latest version of a schema registered for the specified
    group_name having the provided multi-type ID.
    '''
    abort_if_subject_bad(subject_name)
    abort_if_value_bad(id_str, 'multi-type ID string')
    reg_schema = TASR_SUBJECT_APP.ASR.get_schema_for_id_str(id_str)
    if not reg_schema:
        msg = ('No schema with a multi-type ID %s registered for subject %s.' %
               (id_str, subject_name))
        TASR_SUBJECT_APP.abort(404, msg)
    return TASR_SUBJECT_APP.schema_response(reg_schema, subject_name)


@TASR_SUBJECT_APP.get('/<subject_name>/latest')
def lookup_latest(subject_name=None):
    '''Retrieves the registered schema for the specified group with the highest
    version number.
    '''
    abort_if_subject_bad(subject_name)
    reg_schema = TASR_SUBJECT_APP.ASR.get_latest_schema_for_group(subject_name)
    if not reg_schema:
        msg = 'No schema registered for subject %s.' % subject_name
        TASR_SUBJECT_APP.abort(404, msg)
    return TASR_SUBJECT_APP.schema_response(reg_schema, subject_name)
