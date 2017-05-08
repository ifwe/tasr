'''
Created on November 18, 2014

@author: cmills

/collection endpoints meant to be mounted by an umbrella instance of TASRApp.

'''
import bottle
import tasr.headers
from tasr.app.wsgi import TASRApp


##############################################################################
# /collection app - get lists of objects in the repo
##############################################################################
COLLECTION_APP = TASRApp()


def subject_list_response(sub_list):
    '''Given a list of subjects (Group objects), construct a response with all
    the subjects represented.'''
    hbot = tasr.headers.SubjectHeaderBot(bottle.response)
    s_dicts = dict()
    for subject in sub_list:
        hbot.add_subject_name(subject)
        s_dicts[subject.name] = subject.as_dict()
    return COLLECTION_APP.object_response(sub_list, s_dicts)


@COLLECTION_APP.get('/subjects/all')
def all_subject_names():
    '''Get the all the registered subjects, whether or not they have any
    schemas registered.  The S+V API expects this as a plaintext return body
    with one subject per line (using '\n' as delimiters).

    We add X-TASR headers with the subject names as well.  If no Accept header
    is specified (or if it is text/plain), the standard S+V return is used.
    If text/json or application/json is specified, the return body will be a
    JSON document containing current metadata for each subject.
    '''
    subjects = COLLECTION_APP.ASR.get_all_groups()
    return subject_list_response(subjects)


@COLLECTION_APP.get('/subjects/active')
def active_subject_names():
    '''The S+V API expects this as a plaintext return body with one subject per
    line (using '\n' as delimiters).  We add X-TASR headers with the subject
    names as well.
    '''
    subjects = COLLECTION_APP.ASR.get_active_groups()
    return subject_list_response(subjects)


@COLLECTION_APP.get('/subjects/config/<key>')
def config_value_for_subjects(key=None):
    '''Get the config value for the specified KEY for every active subject.'''
    hbot = tasr.headers.SubjectHeaderBot(bottle.response)

    sv_list = []
    sv_dict = dict()
    active_subject_list = COLLECTION_APP.ASR.get_all_groups()
    for sub in active_subject_list:
        if key in sub.config:
            hbot.add_subject_name(sub)
            sv_dict[sub.name] = sub.config[key]
            sv_list.append('%s=%s' % (sub.name, sub.config[key]))
    return COLLECTION_APP.object_response(sv_list, sv_dict)


@COLLECTION_APP.post('/subjects/match')
def matched_subject_names():
    '''The POST body should include a JSON document.  The top-level keys in the
    JSON doc must be present in the subject metadata for a subject to be
    included in the response set.
    '''
    filter_dict = dict()
    for k, v in COLLECTION_APP.request_data_to_dict().iteritems():
        filter_dict['config.' + k] = v
    subjects = COLLECTION_APP.ASR.get_groups_matching_config(filter_dict)
    return subject_list_response(subjects)


@COLLECTION_APP.get('/subjects/sessionized')
def sessionized_subject_names():
    '''The S+V API expects this as a plaintext return body with one subject per
    line (using '\n' as delimiters).  We add X-TASR headers with the subject
    names as well.
    '''
    match_dict = {'config.sessionize': 'true'}
    subjects = COLLECTION_APP.ASR.get_groups_matching_config(match_dict)
    return subject_list_response(subjects)


@COLLECTION_APP.get('/subjects/camus_consumed')
def camus_consumed_subject_names():
    '''The S+V API expects this as a plaintext return body with one subject per
    line (using '\n' as delimiters).  We add X-TASR headers with the subject
    names as well.
    '''
    match_dict = {'config.camus_consume': 'true'}
    subjects = COLLECTION_APP.ASR.get_groups_matching_config(match_dict)
    return subject_list_response(subjects)
