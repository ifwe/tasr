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
from tasr.app_core import TASR_COLLECTION_APP, subject_list_response
from tasr.app_wsgi import is_json_type
from tasr.app_subject import (get_subject, get_anchored_version_list,
                              get_master_sha256_id, abort_if_subject_bad)
import bottle
import json
import tasr.redshift


##############################################################################
# TASR Subject API endpoints -- mount to /tasr/subject
##############################################################################
TASR_REDSHIFT_APP = tasr.app_wsgi.TASRApp()


def get_redshift_master(subject_name):
    abort_if_subject_bad(subject_name)
    versions = get_anchored_version_list(subject_name)
    if not versions or len(versions) == 0:
        TASR_REDSHIFT_APP.abort(404, ('No versions for %s.' % subject_name))
    return tasr.redshift.RedshiftMasterAvroSchema(versions)


def get_redshift_master_dict(subject_name):
    abort_if_subject_bad(subject_name)
    # first try for a cached redshift master
    master_id = get_master_sha256_id(subject_name)
    asr = TASR_REDSHIFT_APP.ASR
    mas_d = asr.get_master_dict_for_sha256_id(master_id)
    if mas_d and ('rs_schema' in mas_d and 'rs_create' in mas_d and
                  'rs_create_staging' in mas_d and
                  'rs_insert_from_staging' in mas_d):
        return mas_d
    # cache miss, so get a RS Master object
    subject = get_subject(subject_name)
    rmas = get_redshift_master(subject_name)
    # once we've created the master, we should cache all the generated strings
    mas_d = dict()
    mas_d['rs_schema'] = rmas.rs_master_schema_string(subject)
    mas_d['rs_create'] = rmas.rs_ddl_create(subject)
    mas_d['rs_create_staging'] = rmas.rs_ddl_create_staging(subject)
    mas_d['rs_insert_from_staging'] = rmas.rs_dml_insert_from_staging(subject)
    TASR_REDSHIFT_APP.ASR.update_master_dict(rmas.sha256_id, mas_d)
    return mas_d


def is_redshift_enabled(subject):
    enabled = False
    if 'redshift.enabled' in subject.config:
        val = subject.config['redshift.enabled'].lower()[:1]
        enabled = (val == 't' or val == 'y')
    return enabled


@TASR_COLLECTION_APP.get('/subjects/redshift')
def redshift_subject_names():
    rs_subjects = []
    for sub in TASR_COLLECTION_APP.ASR.get_active_groups():
        if 'redshift.enabled' in sub.config:
            if sub.config['redshift.enabled'] == 'true':
                rs_subjects.append(sub)
    return subject_list_response(rs_subjects)


@TASR_COLLECTION_APP.get('/subjects/redshift/freq/hourly')
def redshift_hourly_subject_names():
    rs_subjects = []
    for sub in TASR_COLLECTION_APP.ASR.get_active_groups():
        if 'redshift.enabled' in sub.config:
            if sub.config['redshift.enabled'] == 'true':
                if 'redshift.frequency' in sub.config:
                    if sub.config['redshift.frequency'] == 'hourly':
                        rs_subjects.append(sub)
    return subject_list_response(rs_subjects)


@TASR_COLLECTION_APP.get('/subjects/redshift/freq/daily')
def redshift_daily_subject_names():
    rs_subjects = []
    for sub in TASR_COLLECTION_APP.ASR.get_active_groups():
        if 'redshift.enabled' in sub.config:
            if sub.config['redshift.enabled'] == 'true':
                if 'redshift.frequency' in sub.config:
                    if sub.config['redshift.frequency'] == 'daily':
                        rs_subjects.append(sub)
    return subject_list_response(rs_subjects)


@TASR_REDSHIFT_APP.get('/<subject_name>/redshift/master')
def subject_redshift_master_schema(subject_name=None):
    '''Get the RedShift-compatible version of the master subject schema.  The
    RedShift version strips the event type prefix from the event-specific
    fields.  It also converts the kvpairs map into a json string field and
    removes any other complex types (e.g. -- meta__handlers).  The namespace
    shifts to tagged.events.redshift.
    '''
    app = TASR_REDSHIFT_APP
    subject = get_subject(subject_name)
    if not is_redshift_enabled(subject):
        app.abort(404, ('RedShift not enabled for %s.' % subject.name))
    rmas_d = get_redshift_master_dict(subject_name)
    return app.json_str_response(rmas_d['rs_schema'])


@TASR_REDSHIFT_APP.get('/<subject_name>/redshift/dml_create')
def subject_redshift_dml_create(subject_name=None):
    return subject_redshift_ddl_create(subject_name)


@TASR_REDSHIFT_APP.get('/<subject_name>/redshift/ddl_create')
def subject_redshift_ddl_create(subject_name=None):
    '''Get the RedShift-compatible version of the master subject schema.  The
    RedShift version strips the event type prefix from the event-specific
    fields.  It also converts the kvpairs map into a json string field and
    removes any other complex types (e.g. -- meta__handlers).  The namespace
    shifts to tagged.events.redshift.
    '''
    app = TASR_REDSHIFT_APP
    subject = get_subject(subject_name)
    if not is_redshift_enabled(subject):
        app.abort(404, ('RedShift not enabled for %s.' % subject.name))
    rmas_d = get_redshift_master_dict(subject_name)
    return app.object_response(rmas_d['rs_create'], None, 'text/plain')


@TASR_REDSHIFT_APP.post('/<subject_name>/redshift/dml_alter')
def subject_redshift_dml_alter(subject_name=None):
    return subject_redshift_ddl_alter(subject_name)


@TASR_REDSHIFT_APP.post('/<subject_name>/redshift/ddl_alter')
def subject_redshift_ddl_alter(subject_name=None):
    app = TASR_REDSHIFT_APP
    subject = get_subject(subject_name)
    if not is_redshift_enabled(subject):
        app.abort(404, ('RedShift not enabled for %s.' % subject.name))
    old = None
    bod = bottle.request.body.getvalue()
    if not bod or bod == None or len(bod) == 0:
        old = []
    else:
        rctype = bottle.request.content_type
        if is_json_type(rctype):
            # if JSON was passed, try to extract the array of field names
            try:
                jbod = json.loads(bod)
                if isinstance(jbod, list):
                    old = jbod
            except:
                pass
        if old == None:
            # not JSON or bad JSON, so try processing bod as a whitespace
            # delimited field name sequence
            old = bod.split()

    rs_mas = get_redshift_master(subject.name)
    return app.object_response(rs_mas.rs_ddl_alter(subject, old),
                               None, 'text/plain')


@TASR_REDSHIFT_APP.get('/<subject_name>/redshift/ddl_create_staging')
def subject_redshift_ddl_create_staging(subject_name=None):
    '''Get the DDL statement to CREATE a staging table in RS.  The staging
    table has the same field names as we have in our cluster (Hive, Spark),
    and timestamps are bigint values measuring ms since the epoch -- not
    timestamp values.
    '''
    app = TASR_REDSHIFT_APP
    subject = get_subject(subject_name)
    if not is_redshift_enabled(subject):
        app.abort(404, ('RedShift not enabled for %s.' % subject.name))
    rmas_d = get_redshift_master_dict(subject_name)
    return app.object_response(rmas_d['rs_create_staging'], None, 'text/plain')


@TASR_REDSHIFT_APP.get('/<subject_name>/redshift/dml_insert_from_staging')
def subject_redshift_dml_insert_from_staging(subject_name=None):
    '''Get the DML statement to insert staging data into the main table,
    converting bigints to timestamps as required.
    '''
    app = TASR_REDSHIFT_APP
    subject = get_subject(subject_name)
    if not is_redshift_enabled(subject):
        app.abort(404, ('RedShift not enabled for %s.' % subject.name))
    rmas_d = get_redshift_master_dict(subject_name)
    return app.object_response(rmas_d['rs_insert_from_staging'],
                               None, 'text/plain')
