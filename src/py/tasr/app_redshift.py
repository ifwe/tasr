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
from tasr.app_subject import get_subject, get_anchored_version_list
import tasr.redshift


##############################################################################
# TASR Subject API endpoints -- mount to /tasr/subject
##############################################################################
TASR_REDSHIFT_APP = tasr.app_wsgi.TASRApp()


def get_redshift_master(subject_name):
    subject = get_subject(subject_name)
    versions = get_anchored_version_list(subject.name)
    if not versions or len(versions) == 0:
        TASR_REDSHIFT_APP.abort(404, ('No versions for %s.' % subject.name))
    return tasr.redshift.RedshiftMasterAvroSchema(versions)


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


@TASR_REDSHIFT_APP.get('/<subject_name>/redshift/master')
def subject_redshift_master_schema(subject_name=None):
    '''Get the RedShift-compatible version of the master subject schema.  The
    RedShift version strips the event type prefix from the event-specific
    fields.  It also converts the kvpairs map into a json string field and
    removes any other complex types (e.g. -- meta__handlers).  The namespace
    shifts to tagged.events.redshift.
    '''
    subject = get_subject(subject_name)
    if not is_redshift_enabled(subject):
        TASR_REDSHIFT_APP.abort(404, ('RedShift not enabled for %s.' %
                                      subject.name))
    rs_mas = get_redshift_master(subject.name)
    return TASR_REDSHIFT_APP.object_response(rs_mas.rs_json_obj(subject),
                                             None, 'application/json')


@TASR_REDSHIFT_APP.get('/<subject_name>/redshift/dml_create')
def subject_redshift_dml_create(subject_name=None):
    '''Get the RedShift-compatible version of the master subject schema.  The
    RedShift version strips the event type prefix from the event-specific
    fields.  It also converts the kvpairs map into a json string field and
    removes any other complex types (e.g. -- meta__handlers).  The namespace
    shifts to tagged.events.redshift.
    '''
    subject = get_subject(subject_name)
    if not is_redshift_enabled(subject):
        TASR_REDSHIFT_APP.abort(404, ('RedShift not enabled for %s.' %
                                      subject.name))
    rs_mas = get_redshift_master(subject.name)
    return TASR_REDSHIFT_APP.object_response(rs_mas.rs_dml_create(subject),
                                             None, 'text/plain')
