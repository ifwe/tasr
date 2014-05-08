'''
Created on May 6, 2014

@author: cmills

The idea here is to provide client-side functions to interact with the TASR repo.
We use the requests package here.  We provide both stand-alone functions and a
class with methods.  The class is easier if you are using non-default values for
the host or port.
'''

import requests
from tasr.registered_schema import RegisteredAvroSchema

TASR_HOST='localhost'
TASR_PORT=8080
TASR_TIMEOUT=2 # seconds

class TASRError(Exception):
    '''Something went wrong with a TASR interaction'''

def _response_to_registered_schema(schema_str, resp):
    _ras = RegisteredAvroSchema()
    _ras.schema_str = schema_str
    # check that it came through as expected
    _sha256_id = resp.headers["X-SCHEMA-SHA256-ID"]
    if not _ras.sha256_id == _sha256_id:
        raise TASRError('Schema was modified in transit.') 
    # set the topic-version intersections from the headers
    _tv_list = resp.headers["X-SCHEMA-TOPIC-VERSION"].split(',')
    for _tv in _tv_list:
        (_t, _ver) = _tv.split('=', 1)
        _ver = int(_ver)
        _ras.tv_dict[_t] = _ver
    return _ras

def get_registered_schema_for_topic(topic, version=None, host=TASR_HOST,
                                    port=TASR_PORT, timeout=TASR_TIMEOUT):
    try:
        _url = 'http://%s:%s/tasr/topic/%s' % (host, port, topic)
        if version:
            _url = '%s/%s' % (_url, version)
        _resp = requests.get(_url, timeout=timeout)
        if _resp == None:
            raise TASRError('Timeout for request to %s' % _url)
        if 404 == _resp.status_code:
            raise TASRError('No such version.')
        if not 200 == _resp.status_code:
            raise TASRError('Failed request to %s (status code: %s)' % 
                            (_url, _resp.status_code))
        return _response_to_registered_schema(_resp.content, _resp)
    except Exception as e:
        raise TASRError(e)

def get_registered_schema_for_id_str(id_str, host=TASR_HOST, 
                                    port=TASR_PORT, timeout=TASR_TIMEOUT):
    try:
        _url = 'http://%s:%s/tasr/id/%s' % (host, port, id_str)
        _resp = requests.get(_url, timeout=timeout)
        if _resp == None:
            raise TASRError('Timeout for request to %s' % _url)
        if 404 == _resp.status_code:
            raise TASRError('No schema for id.')
        if not 200 == _resp.status_code:
            raise TASRError('Failed request to %s (status code: %s)' % 
                            (_url, _resp.status_code))
        return _response_to_registered_schema(_resp.content, _resp)
    except Exception as e:
        raise TASRError(e)

def register_schema_for_topic(schema_str, topic, host=TASR_HOST, port=TASR_PORT,
                              timeout=TASR_TIMEOUT):
    try:
        _url = 'http://%s:%s/tasr/topic/%s' % (host, port, topic)
        _headers = {'content-type': 'application/json; charset=utf8', }
        _resp = requests.put(_url, data=schema_str, timeout=timeout,
                             headers=_headers)
        if _resp == None:
            raise TASRError('Timeout for request to %s' % _url)
        if not 200 == _resp.status_code:
            raise TASRError('Failed request to %s (status code: %s)' % 
                            (_url, _resp.status_code))
        return _response_to_registered_schema(schema_str, _resp)
    except Exception as e:
        raise TASRError(e)

class TASRClient(object):
    def __init__(self, host=TASR_HOST, port=TASR_PORT, timeout=TASR_TIMEOUT):
        self.host = host
        self.port = port
        self.timeout = timeout
        
    def register(self, schema_str, topic):
        return register_schema_for_topic(schema_str, topic, self.host, 
                                         self.port, self.timeout)

    def get_for_topic(self, topic, version=None):
        return get_registered_schema_for_topic(topic, version, self.host, 
                                               self.port, self.timeout)
    
    def get_for_id(self, id_str):
        return get_registered_schema_for_id_str(id_str, self.host, self.port,
                                                self.timeout)
