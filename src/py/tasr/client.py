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
import tasr.app
import webtest
import StringIO
from tasr.registered_schema import RegisteredAvroSchema
from tasr.headers import SubjectHeaderBot, SchemaHeaderBot

APP = tasr.app.TASR_APP
APP.set_config_mode('local')
TASR_HOST = APP.config.host
TASR_PORT = APP.config.port
TIMEOUT = 2  # seconds


class TASRError(Exception):
    '''Something went wrong with a TASR interaction'''


def reg_schema_from_url(url, method='GET', data=None, headers=None,
                 timeout=TIMEOUT, err_404='No such object.'):
    '''A generic method to call a URL and transform the reply into a
    RegisteredSchema object.  Most of the API calls can use this skeleton.
    '''
    schema_str = None
    resp = None
    try:
        if method.upper() == 'GET':
            resp = requests.get(url, timeout=timeout)
            schema_str = resp.content
        elif method.upper() == 'POST':
            resp = requests.post(url, data=data, headers=headers,
                                 timeout=timeout)
            schema_str = resp.content
        elif method.upper() == 'PUT':
            resp = requests.put(url, data=data, headers=headers,
                                timeout=timeout)
            schema_str = resp.content

        # check for error cases
        if resp == None:
            raise TASRError('Timeout for request to %s' % url)
        if 404 == resp.status_code:
            raise TASRError(err_404)
        if not resp.status_code in [200, 201]:
            raise TASRError('Failed request to %s (status code: %s)' %
                            (url, resp.status_code))
        # OK - so construct the RS and return it
        ras = RegisteredAvroSchema()
        ras.schema_str = schema_str
        ras.created = True if resp.status_code == 201 else False
        schema_meta = SchemaHeaderBot.extract_metadata(resp)
        if schema_str and not schema_meta.sha256_id == ras.sha256_id:
            raise TASRError('Schema was modified in transit.')
        ras.update_from_schema_metadata(schema_meta)
        return ras
    except Exception as exc:
        raise TASRError(exc)


def register_subject(subject_name, config_dict=None, host=TASR_HOST,
                     port=TASR_PORT, timeout=TIMEOUT):
    ''' PUT /tasr/subject/<subject name>
    Registers a _subject_ (not a schema), ensuring that the group can be
    established before associating schemas with it.  Note that if a form is
    sent as the PUT body, it should be used to set the subject config map.
    Returns a GroupMetadata object on success.
    '''
    url = 'http://%s:%s/tasr/subject/%s' % (host, port, subject_name)
    hdrs = {'content-type': 'application/json; charset=utf8', }
    resp = requests.put(url, data=config_dict, headers=hdrs, timeout=timeout)
    if resp == None:
        raise TASRError('Timeout for register subject request.')
    if not resp.status_code in [200, 201]:
        raise TASRError('Failed to register subject. (status code: %s)' %
                        resp.status_code)
    subject_metas = SubjectHeaderBot.extract_metadata(resp)
    if subject_metas and len(subject_metas) > 0:
        return subject_metas[subject_name]


def lookup_subject(subject_name, host=TASR_HOST, port=TASR_PORT,
                   timeout=TIMEOUT):
    ''' GET /tasr/subject/<subject name>
    Checks whether a subject has been registered.  Returns a boolean value.
    '''
    try:
        url = 'http://%s:%s/tasr/subject/%s' % (host, port, subject_name)
        resp = requests.get(url, timeout=timeout)
        if resp.status_code == 200:
            return True
        return False
    except webtest.AppError:
        return False


def get_subject_config(subject_name, host=TASR_HOST, port=TASR_PORT,
                       timeout=TIMEOUT):
    ''' GET /tasr/subject/<subject name>/config
    Retrieves the config map for the subject.  Each key:value pair is returned
    as a line in the format "<key>=<value>\n" in the response body.
    '''
    url = 'http://%s:%s/tasr/subject/%s/config' % (host, port, subject_name)
    resp = requests.get(url, timeout=timeout)
    if resp == None:
        raise TASRError('Timeout for register subject request.')
    if not resp.status_code == 200:
        raise TASRError('Failed to register subject. (status code: %s)' %
                        resp.status_code)
    # construct the dict from the response body
    buff = StringIO.StringIO(resp.content)
    config_dict = dict()
    for line in buff:
        if line and line.strip():
            # this ensures we don't try and split empty lines
            key, value = line.strip().split('=', 1)
            config_dict[key] = value
    buff.close()
    return config_dict


def update_subject_config(subject_name, config_dict,
                          host=TASR_HOST, port=TASR_PORT, timeout=TIMEOUT):
    ''' POST /tasr/subject/<subject name>/config
    Replaces the config map for the subject.  Each key:value pair of the
    updated map is returned as a line in the format "<key>=<value>\n" in the
    response body.  The method returns the updated config dict.
    '''
    url = 'http://%s:%s/tasr/subject/%s/config' % (host, port, subject_name)
    resp = requests.post(url, data=config_dict, timeout=timeout)
    if resp == None:
        raise TASRError('Timeout for register subject request.')
    if resp.status_code != 200:
        raise TASRError('Failed to update config for %s' % subject_name)
    # construct the dict from the response body
    buff = StringIO.StringIO(resp.content)
    config_dict = dict()
    for line in buff:
        if line and line.strip():
            # this ensures we don't try and split empty lines
            key, value = line.strip().split('=', 1)
            config_dict[key] = value
    buff.close()
    return config_dict


def is_subject_integral(subject_name, host=TASR_HOST,
                     port=TASR_PORT, timeout=TIMEOUT):
    ''' GET /tasr/subject/<subject name>/integral
    Returns 'True' or 'False' as plaintext in the response body, indicating
    whether the IDs used by the repository are guaranteed to be integers.  Note
    that TASR will always return 'False' in its current state as we accept both
    version numbers (which are integers) and multi-type IDs (which are base64-
    encoded byte arrays, not integers).
    '''
    url = 'http://%s:%s/tasr/subject/%s/integral' % (host, port, subject_name)
    resp = requests.get(url, timeout=timeout)
    if resp == None:
        raise TASRError('Timeout for get all subjects request.')
    if resp.status_code != 200:
        raise TASRError('No valid integral response for %s' % subject_name)
    if resp.content.strip().upper() == 'TRUE':
        return True
    return False


def get_active_subject_names(host=TASR_HOST, port=TASR_PORT, timeout=TIMEOUT):
    ''' GET /tasr/collection/subjects/active
    Retrieves all the active subject names (ones with schemas), both as X-TASR
    header fields and as plain text, one per line, in the response body.  This
    method returns a list of subject name strings.
    '''
    url = 'http://%s:%s/tasr/collection/subjects/active' % (host, port)
    resp = requests.get(url, timeout=timeout)
    if resp == None:
        raise TASRError('Timeout for get active subjects request.')
    if resp.status_code != 200:
        raise TASRError('Failed to get active subjects (status code: %s)' %
                        resp.status_code)
    subject_metas = SubjectHeaderBot.extract_metadata(resp)
    # check that subject_metas.keys() matches the body list
    buff = StringIO.StringIO(resp.content)
    name_list = []
    for line in buff:
        name_list.append(line.strip())
    buff.close()
    if len(subject_metas.keys()) != len(name_list):
        raise TASRError('Header-body mismatch for subject name lists.')
    if sorted(subject_metas.keys()) != sorted(name_list):
        raise TASRError('Header-body mismatch for subject name lists.')
    return subject_metas.keys()


def get_all_subject_names(host=TASR_HOST, port=TASR_PORT, timeout=TIMEOUT):
    ''' GET /tasr/collection/subjects/all
    Retrieves all the registered subject names, both as X-TASR header fields
    and as plain text, one per line, in the response body.  This method returns
    a list of subject name strings.
    '''
    url = 'http://%s:%s/tasr/collection/subjects/all' % (host, port)
    resp = requests.get(url, timeout=timeout)
    if resp == None:
        raise TASRError('Timeout for get all subjects request.')
    if resp.status_code != 200:
        raise TASRError('Failed to get all subjects (status code: %s)' %
                        resp.status_code)
    subject_metas = SubjectHeaderBot.extract_metadata(resp)
    # check that subject_metas.keys() matches the body list
    buff = StringIO.StringIO(resp.content)
    name_list = []
    for line in buff:
        name_list.append(line.strip())
    buff.close()
    if len(subject_metas.keys()) != len(name_list):
        raise TASRError('Header-body mismatch for subject name lists.')
    if sorted(subject_metas.keys()) != sorted(name_list):
        raise TASRError('Header-body mismatch for subject name lists.')
    return subject_metas.keys()


def get_all_subject_schema_ids(subject_name, host=TASR_HOST,
                               port=TASR_PORT, timeout=TIMEOUT):
    ''' GET /tasr/subject/<subject name>/all_ids
    Retrieves a list of the SHA256 multi-type IDs for all the schema versions
    registered for a subject, in version order.
    '''
    url = 'http://%s:%s/tasr/subject/%s/all_ids' % (host, port, subject_name)
    resp = requests.get(url, timeout=timeout)
    if resp == None:
        raise TASRError('Timeout for get all subject IDs request.')
    if resp.status_code != 200:
        raise TASRError('Failed to get all subject IDs (status code: %s)' %
                        resp.status_code)
    meta = SubjectHeaderBot.extract_metadata(resp)[subject_name]
    # check that the sha256_list matches the body list
    buff = StringIO.StringIO(resp.content)
    sha256_ids = []
    for line in buff:
        sha256_ids.append(line.strip())
    buff.close()
    if len(meta.sha256_id_list) != len(sha256_ids):
        raise TASRError('Header-body mismatch for sha256_id lists.')
    if meta.sha256_id_list != sha256_ids:
        raise TASRError('Header-body mismatch for sha256_id lists.')
    return meta.sha256_id_list


def get_all_subject_schemas(subject_name,
                            host=TASR_HOST, port=TASR_PORT, timeout=TIMEOUT):
    ''' GET /tasr/subject/<subject name>/all_schemas
    Retrieves all the (canonical) schema versions registered for a subject,
    in version order, one per line in the response body.  The multi-type IDs
    are included in the headers for confirmation.
    '''
    url = ('http://%s:%s/tasr/subject/%s/all_schemas' %
           (host, port, subject_name))
    resp = requests.get(url, timeout=timeout)
    if resp == None:
        raise TASRError('Timeout for get all subject schemas request.')
    if resp.status_code != 200:
        raise TASRError('Failed to get all subject schemas (status code: %s)' %
                        resp.status_code)
    meta = SubjectHeaderBot.extract_metadata(resp)[subject_name]
    buff = StringIO.StringIO(resp.content)
    schemas = []
    version = 1
    for schema_str in buff:
        ras = RegisteredAvroSchema()
        ras.schema_str = schema_str.strip()
        ras.gv_dict[subject_name] = version
        if ras.sha256_id != meta.sha256_id_list[version - 1]:
            raise TASRError('Generated SHA256 ID did not match passed ID.')
        schemas.append(ras)
        version += 1
    buff.close()
    return schemas


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


def register_schema_if_latest(subject_name, version, schema_str,
                              host=TASR_HOST, port=TASR_PORT, timeout=TIMEOUT):
    ''' PUT /tasr/subject/<subject name>/register_if_latest/<version>
    Register a schema string for a subject if the version specified is the
    latest version number at the time of the request.  If successful, it
    returns a RegisteredSchema object.
    '''
    url = ('http://%s:%s/tasr/subject/%s/register_if_latest/%s' %
           (host, port, subject_name, version))
    headers = {'content-type': 'application/json; charset=utf8', }
    return reg_schema_from_url(url, method='PUT', data=schema_str,
                               headers=headers, timeout=timeout)


def lookup_by_schema_str(subject_name, schema_str,
                         host=TASR_HOST, port=TASR_PORT, timeout=TIMEOUT):
    ''' POST /tasr/subject/<subject name>/schema
    Get a RegisteredAvroSchema back for a given subject and schema string.
    '''
    url = 'http://%s:%s/tasr/subject/%s/schema' % (host, port, subject_name)
    headers = {'content-type': 'application/json; charset=utf8', }
    return reg_schema_from_url(url, method='POST', data=schema_str,
                               headers=headers, timeout=timeout,
                               err_404='Schema not registered.')


def lookup_by_version(subject_name, version,
                      host=TASR_HOST, port=TASR_PORT, timeout=TIMEOUT):
    ''' GET /tasr/subject/<subject name>/version/<version>
    Get a RegisteredAvroSchema back for a given subject name and version
    number.  Note version numbers are integers greater than 0.
    '''
    try:
        iver = int(version)
        if iver < 1:
            raise TASRError('Bad version %s' % version)
    except:
        raise TASRError('Bad version %s' % version)
    url = ('http://%s:%s/tasr/subject/%s/version/%s' %
           (host, port, subject_name, iver))
    return reg_schema_from_url(url, timeout=timeout,
                               err_404='No such version.')


def lookup_by_id_str(subject_name, id_str,
                     host=TASR_HOST, port=TASR_PORT, timeout=TIMEOUT):
    ''' GET /tasr/subject/<subject name>/id/<version>
    Get a RegisteredAvroSchema back for a given subject name and a multi-type
    ID string.
    '''
    url = ('http://%s:%s/tasr/subject/%s/id/%s' %
           (host, port, subject_name, id_str))
    return reg_schema_from_url(url, timeout=timeout,
                               err_404='No schema registered with this ID.')


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
    def register_subject(self, subject_name, config_dict=None):
        '''Registers a subject name.  Returns a GroupMetadata object.'''
        return register_subject(subject_name, config_dict,
                                self.host, self.port, self.timeout)

    def lookup_subject(self, subject_name):
        '''Checks whether a subject has been registered.'''
        return lookup_subject(subject_name,
                              self.host, self.port, self.timeout)

    def subject_config(self, subject_name):
        '''Gets the config map for the subject.'''
        return get_subject_config(subject_name,
                                  self.host, self.port, self.timeout)

    def update_subject_config(self, subject_name, config_dict):
        '''Updates the config map for the subject.'''
        return update_subject_config(subject_name, config_dict,
                                     self.host, self.port, self.timeout)

    def is_subject_integral(self, subject_name):
        '''Indicates whether schema IDs are guaranteed to be integers.'''
        return is_subject_integral(subject_name,
                                   self.host, self.port, self.timeout)

    def active_subject_names(self):
        '''Returns a list of active subject names.'''
        return get_active_subject_names(self.host, self.port, self.timeout)

    def all_subject_names(self):
        '''Returns a list of registered subject names.'''
        return get_all_subject_names(self.host, self.port, self.timeout)

    def all_subject_schema_ids(self, subject_name):
        '''Returns a list of SHA256-based IDs for schema versions registered
        for the specified subject.'''
        return get_all_subject_schema_ids(subject_name,
                                          self.host, self.port, self.timeout)

    def all_subject_schemas(self, subject_name):
        '''Returns a version-ordered list of registered schemas for the
        specified subject.'''
        return get_all_subject_schemas(subject_name,
                                       self.host, self.port, self.timeout)

    # schema calls
    def register_schema(self, subject_name, schema_str):
        '''Register a schema for a subject.'''
        return register_schema(subject_name, schema_str)

    def register_schema_if_latest_version(self, subject_name, ver, schema_str):
        '''Register a schema for a subject if the version number is currently
        the latest for the subject.'''
        return register_schema_if_latest(subject_name, ver, schema_str)

    def lookup_by_schema_str(self, subject, schema_str):
        '''Get a registered schema for a specified schema str.'''
        return lookup_by_schema_str(subject, schema_str,
                                    self.host, self.port, self.timeout)

    def lookup_by_version(self, subject_name, version):
        '''Get a registered schema for the subject and version.'''
        return lookup_by_version(subject_name, version,
                                 self.host, self.port, self.timeout)

    def lookup_by_id_str(self, subject_name, id_str):
        '''Get a registered schema for the subject and multi-type ID string.'''
        return lookup_by_id_str(subject_name, id_str,
                                 self.host, self.port, self.timeout)

    def lookup_latest(self, subject_name):
        '''Get the latest registered schema for the subject.'''
        return lookup_latest(subject_name, self.host, self.port, self.timeout)
