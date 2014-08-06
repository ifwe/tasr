'''
Created on July 30, 2014

@author: cmills

This contains client-side support for TASR, focusing on the Subject+Version
(S+V) API used by the Avro project's schema repository code.  There is also
support for some TASR-exclusive methods (retrieving by digest-based ID, for
example).  The older TASR API remains available through the client_legacy
module.
'''

import requests
import webtest
from tasr.client import TASRError, reg_schema_from_url
from tasr.headers import SubjectHeaderBot

TASR_HOST = 'localhost'
TASR_PORT = 8080
TIMEOUT = 2  # seconds


#############################################################################
# Subject+Version API methods
#############################################################################


def get_all_subjects(host=TASR_HOST, port=TASR_PORT, timeout=TIMEOUT):
    ''' GET /tasr/subject
    Retrieves available metadata for all the subjects (i.e. -- groups) with
    registered schemas.  A list of subject names is returned.
    '''
    url = 'http://%s:%s/tasr/subject' % (host, port)
    resp = requests.get(url, timeout=timeout)
    if resp == None:
        raise TASRError('Timeout for get all subjects request.')
    if not 200 == resp.status_code:
        raise TASRError('Failed to get all subjects (status code: %s)' %
                        resp.status_code)
    subject_metas = SubjectHeaderBot.extract_metadata(resp)
    return subject_metas.keys()


def register_subject(subject_name, config_dict=None, host=TASR_HOST,
                     port=TASR_PORT, timeout=TIMEOUT):
    ''' PUT /tasr/subject/<subject name>
    Registers a _subject_ (not a schema), ensuring that the group can be
    established before associating schemas with it.  Note that if a form is
    sent as the PUT body, it should be used to set the subject config map.
    Returns a GroupMetadata object on success.
    '''
    url = 'http://%s:%s/tasr/subject/%s' % (host, port, subject_name)
    resp = requests.put(url, data=config_dict, timeout=timeout)
    if resp == None:
        raise TASRError('Timeout for register subject request.')
    if not 200 == resp.status_code:
        raise TASRError('Failed to register subject. (status code: %s)' %
                        resp.status_code)
    subject_metas = SubjectHeaderBot.extract_metadata(resp)
    if subject_metas and len(subject_metas) > 0:
        return subject_metas[subject_name]


def lookup_subject(subject, host=TASR_HOST, port=TASR_PORT, timeout=TIMEOUT):
    ''' GET /tasr/subject/<subject name>
    Checks whether a subject has been registered.  Returns a boolean value.
    '''
    try:
        url = 'http://%s:%s/tasr/subject/%s' % (host, port, subject)
        resp = requests.get(url, timeout=timeout)
        if resp.status_code == 200:
            return True
        return False
    except webtest.AppError:
        return False


def register_schema(subject_name, schema_str,
                    host=TASR_HOST, port=TASR_PORT, timeout=TIMEOUT):
    ''' PUT /tasr/subject/<subject name>/register
    Register a schema string for a subject.  Returns a RegisteredSchema object.
    '''
    url = ('http://%s:%s/tasr/subject/%s/register' %
           (host, port, subject_name))
    headers = {'content-type': 'application/json; charset=utf8', }
    return reg_schema_from_url(url, method='PUT', data=schema_str,
                               headers=headers, timeout=timeout)


def lookup_by_schema_str(subject_name, schema_str,
                         host=TASR_HOST, port=TASR_PORT, timeout=TIMEOUT):
    ''' GET /tasr/subject/<subject name>
    Get a RegisteredAvroSchema back for a given subject and schema string.
    '''
    url = 'http://%s:%s/tasr/subject/%s/schema' % (host, port, subject_name)
    headers = {'content-type': 'application/json; charset=utf8', }
    return reg_schema_from_url(url, method='POST', data=schema_str,
                               headers=headers, timeout=timeout,
                               err_404='Schema not registered.')


def lookup_by_version(subject_name, version,
                      host=TASR_HOST, port=TASR_PORT, timeout=TIMEOUT):
    ''' GET /tasr/subject/<subject name>/id/<version>
    Get a RegisteredAvroSchema back for a given subject name and version.
    '''
    url = ('http://%s:%s/tasr/subject/%s/id/%s' %
           (host, port, subject_name, version))
    return reg_schema_from_url(url, timeout=timeout,
                               err_404='No such version.')


def lookup_latest(subject_name,
                  host=TASR_HOST, port=TASR_PORT, timeout=TIMEOUT):
    ''' GET /tasr/subject/<subject name>/latest
    Get the most recent RegisteredAvroSchema back for a given subject name.
    '''
    url = ('http://%s:%s/tasr/subject/%s/latest' % (host, port, subject_name))
    return reg_schema_from_url(url, timeout=timeout,
                               err_404='No such version.')

#############################################################################
# Wrapped in a class
#############################################################################


class TASRClientSV(object):
    '''An object means you only need to specify the host settings once.
    '''
    def __init__(self, host=TASR_HOST, port=TASR_PORT, timeout=TIMEOUT):
        self.host = host
        self.port = port
        self.timeout = timeout

    # subject calls
    def get_all_subjects(self):
        '''Returns a list of registered subject names.'''
        return get_all_subjects(self.host, self.port, self.timeout)

    def register_subject(self, subject_name):
        '''Registers a subject name.  Returns a GroupMetadata object.'''
        return register_subject(subject_name)

    def lookup_subject(self, subject_name):
        '''Checks whether a subject has been registered.'''
        return lookup_subject(subject_name, self.host, self.port, self.timeout)

    # schema calls
    def register_schema(self, subject_name, schema_str):
        '''Register a schema for a subject.'''
        return register_schema(subject_name, schema_str)

    def lookup_by_schema_str(self, schema_str, subject=None):
        '''Get a registered schema for a specified schema str.'''
        return lookup_by_schema_str(subject, schema_str,
                                    self.host, self.port, self.timeout)

    def lookup_by_version(self, subject_name, version):
        '''Get a registered schema for the subject and version.'''
        return lookup_by_version(subject_name, version,
                                 self.host, self.port, self.timeout)

    def lookup_latest(self, subject_name):
        '''Get the latest registered schema for the subject.'''
        return lookup_latest(subject_name, self.host, self.port, self.timeout)
