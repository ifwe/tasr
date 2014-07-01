'''
Created on May 6, 2014

@author: cmills

The idea here is to provide client-side functions to interact with the TASR
repo.  We use the requests package here.  We provide both stand-alone functions
and a class with methods.  The class is easier if you are using non-default
values for the host or port.
'''

import requests
from tasr.registered_schema import RegisteredAvroSchema

TASR_HOST = 'localhost'
TASR_PORT = 8080
TASR_TIMEOUT = 2  # seconds


class TASRError(Exception):
    '''Something went wrong with a TASR interaction'''


def _response_to_reg_schema(schema_str, resp):
    '''Package the response data up in a RegisteredAvroSchema object.
    '''
    ras = RegisteredAvroSchema()
    ras.schema_str = schema_str
    # check that it came through as expected
    sha256_id = resp.headers["X-SCHEMA-SHA256-ID"]
    if not ras.sha256_id == sha256_id:
        raise TASRError('Schema was modified in transit.')
    # set the topic-version intersections from the headers
    tv_list = resp.headers["X-SCHEMA-TOPIC-VERSION"].split(',')
    for tvx in tv_list:
        (topic, ver) = tvx.split('=', 1)
        ver = int(ver)
        ras.tv_dict[topic.strip()] = ver
    # set the topic-version intersection timestamps from the headers
    ts_list = resp.headers["X-SCHEMA-TOPIC-VERSION-TIMESTAMP"].split(',')
    for tse in ts_list:
        (topic, timestamp) = tse.split('=', 1)
        timestamp = long(timestamp)
        ras.ts_dict[topic.strip()] = timestamp
    return ras


def _rs_from_url(url, method='GET', data=None, headers=None,
                 timeout=TASR_TIMEOUT, err_404='No such object.'):
    '''A generic method to call a URL and transform the reply into a
    RegisteredSchema object.  Most of the API calls can use this skeleton.
    '''
    resp = None
    try:
        if method.upper() == 'GET':
            resp = requests.get(url, timeout=timeout)
        elif method.upper() == 'POST':
            resp = requests.post(url, data=data, headers=headers,
                                 timeout=timeout)
        elif method.upper() == 'PUT':
            resp = requests.put(url, data=data, headers=headers,
                                timeout=timeout)
        if resp == None:
            raise TASRError('Timeout for request to %s' % url)
        if 404 == resp.status_code:
            raise TASRError(err_404)
        if not 200 == resp.status_code:
            raise TASRError('Failed request to %s (status code: %s)' %
                            (url, resp.status_code))
        return _response_to_reg_schema(resp.content, resp)
    except Exception as exc:
        raise TASRError(exc)

#############################################################################
# Bare TASR API methods
#############################################################################


def get_reg_schema_for_topic(topic, version=None, host=TASR_HOST,
                             port=TASR_PORT, timeout=TASR_TIMEOUT):
    '''Get a RegisteredAvroSchema back for a given topic (and version if
    provided, otherwise what's most recent).
    '''
    url = 'http://%s:%s/tasr/topic/%s' % (host, port, topic)
    if version:
        url = '%s/%s' % (url, version)
    return _rs_from_url(url, timeout=timeout, err_404='No such version.')


def get_reg_schema_for_id_str(id_str, host=TASR_HOST,
                              port=TASR_PORT, timeout=TASR_TIMEOUT):
    '''Get a RegisteredAvroSchema back for a given ID str.
    '''
    url = 'http://%s:%s/tasr/id/%s' % (host, port, id_str)
    return _rs_from_url(url, timeout=timeout, err_404='No schema for id.')


def get_reg_schema_for_schema_str(schema_str, host=TASR_HOST, port=TASR_PORT,
                                  timeout=TASR_TIMEOUT):
    '''Get a RegisteredAvroSchema back for a given schema string.
    '''
    url = 'http://%s:%s/tasr/schema' % (host, port)
    headers = {'content-type': 'application/json; charset=utf8', }
    return _rs_from_url(url, method='POST', data=schema_str, headers=headers,
                        timeout=timeout, err_404='Schema not registered.')


def register_schema_for_topic(schema_str, topic, host=TASR_HOST,
                              port=TASR_PORT, timeout=TASR_TIMEOUT):
    '''Register a schema string for a topic.
    '''
    url = 'http://%s:%s/tasr/topic/%s' % (host, port, topic)
    headers = {'content-type': 'application/json; charset=utf8', }
    return _rs_from_url(url, method='PUT', data=schema_str, headers=headers,
                        timeout=timeout)

#############################################################################
# Bare S+V API methods
#############################################################################


def get_reg_schema_for_subject(subject, version=None, host=TASR_HOST,
                               port=TASR_PORT, timeout=TASR_TIMEOUT):
    '''Get a RegisteredAvroSchema back for a given subject (and version if
    provided, otherwise what's most recent).
    '''
    url = 'http://%s:%s/tasr/subject/%s' % (host, port, subject)
    if version:
        url = '%s/id/%s' % (url, version)
    return _rs_from_url(url, timeout=timeout, err_404='No such version.')


def get_reg_schema_for_subject_and_schema_str(subject, schema_str,
                                              host=TASR_HOST, port=TASR_PORT,
                                              timeout=TASR_TIMEOUT):
    '''Get a RegisteredAvroSchema back for a given subject and schema string.
    '''
    url = 'http://%s:%s/tasr/subject/%s' % (host, port, subject)
    headers = {'content-type': 'application/json; charset=utf8', }
    return _rs_from_url(url, method='POST', data=schema_str, headers=headers,
                        timeout=timeout, err_404='Schema not registered.')


def register_schema_for_subject(schema_str, subject, host=TASR_HOST,
                                port=TASR_PORT, timeout=TASR_TIMEOUT):
    '''Register a schema string for a subject.
    '''
    url = 'http://%s:%s/tasr/subject/%s/register' % (host, port, subject)
    headers = {'content-type': 'application/json; charset=utf8', }
    return _rs_from_url(url, method='PUT', data=schema_str, headers=headers,
                        timeout=timeout)

#############################################################################
# Wrapped in a class
#############################################################################


class TASRClient(object):
    '''An object means you only need to specify the host settings once.
    '''
    def __init__(self, host=TASR_HOST, port=TASR_PORT, timeout=TASR_TIMEOUT):
        self.host = host
        self.port = port
        self.timeout = timeout

    def register(self, schema_str, topic):
        '''Register a schema'''
        return self.register_for_topic(schema_str, topic)

    def register_for_topic(self, schema_str, topic):
        '''Register a schema for a topic'''
        return register_schema_for_topic(schema_str, topic, self.host,
                                         self.port, self.timeout)

    def register_for_subject(self, schema_str, subject):
        '''Register a schema for a subject'''
        return register_schema_for_subject(schema_str, subject, self.host,
                                         self.port, self.timeout)

    def get_for_topic(self, topic, version=None):
        '''Get a reg schema for the topic'''
        return get_reg_schema_for_topic(topic, version, self.host,
                                        self.port, self.timeout)

    def get_for_subject(self, subject, version=None):
        '''Get a reg schema for the subject'''
        return get_reg_schema_for_subject(subject, version, self.host,
                                          self.port, self.timeout)

    def get_for_id(self, id_str):
        '''Get a reg schema for a specified ID str.'''
        return get_reg_schema_for_id_str(id_str, self.host, self.port,
                                         self.timeout)

    def get_for_schema(self, schema_str, subject=None):
        '''Get a reg schema for a specified schema str (subject optional).'''
        if subject:
            return get_reg_schema_for_subject_and_schema_str(schema_str,
                                                             subject,
                                                             self.host,
                                                             self.port,
                                                             self.timeout)
        else:
            return get_reg_schema_for_schema_str(schema_str, self.host,
                                                 self.port, self.timeout)
