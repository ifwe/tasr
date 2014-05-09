'''
Created on May 7, 2014

@author: cmills
'''
import sys, os
_test_dir = os.path.abspath(os.path.dirname(__file__))
_src_dir = os.path.abspath(os.path.dirname('%s/../../src/py/tagged' % _test_dir))
sys.path.insert(0, os.path.join(_test_dir, _src_dir))
_fix_dir = os.path.abspath(os.path.dirname("%s/../fixtures/" % _test_dir))

import unittest
import tasr.app
import tasr.client
import requests
from requests.packages.urllib3._collections import HTTPHeaderDict
from requests.packages.urllib3.response import HTTPResponse
import httmock
from webtest import TestApp, TestRequest

class TestTASRClient(unittest.TestCase):

    def setUp(self):
        self.event_type = "gold"
        self.avsc_file = "%s/schemas/%s.avsc" % (_fix_dir, self.event_type)
        self.schema_str = open(self.avsc_file, "r").read()
        self.tasr = TestApp(tasr.app.app)
        # client settings
        self.host = 'localhost' # should match netloc below
        self.port = 8080        # should match netloc below

    def tearDown(self):
        # this clears out redis after each test -- careful!
        for _k in tasr.app.ASR.redis.keys():
            tasr.app.ASR.redis.delete(_k)

    @httmock.urlmatch(netloc=r'localhost:8080')
    def route_to_testapp(self, url, requests_req):
        '''This is some tricky stuff.  To test the client methods, we need the 
        responses package calls to route to our webtest TestApp WSGI wrapper.
        We use httmock to intercept the requests call, then we handle processing 
        in this function instead -- calling the TestApp wrapper.
        '''
        # create a webtest TestRequest from the requests PreparedRequest
        _webtest_req = TestRequest.blank(requests_req.url, 
                                         method=requests_req.method, 
                                         body=requests_req.body, 
                                         headers=requests_req.headers)
        # have the TestApp wrapper process the TestRequest
        _webtest_resp = self.tasr.request(_webtest_req)
        # webtest responses support multiple headers with the same key, while 
        # the requests package holds them in a case-insensitive dict of lists of
        # (key,value) tuples.  We need to translate by hand here to keep cases
        # with multiple headers with the same key
        _headers = HTTPHeaderDict()
        for _k, _v in _webtest_resp.headers.iteritems():
            _headers.add(_k, _v)
        # use the webtest TestResponse to build a new requests HTTPResponse
        _requests_http_resp = HTTPResponse(body=_webtest_resp.body, 
                                           headers=_headers,
                                           status=_webtest_resp.status_code)
        # get an HTTPAdaptor, then use it to build the requests Response object
        _a = requests.adapters.HTTPAdapter()
        _requests_resp = _a.build_response(requests_req, _requests_http_resp)
        # For some reason, we need to explicitly set the _content attribute after
        # the response object is built -- it is already in there as raw.data, 
        # but it doesn't make it to _content, so it never hits content() without
        # this intervention.
        _requests_resp._content = _webtest_resp.body
        return _requests_resp

    def test_bare_register_schema(self):
        with httmock.HTTMock(self.route_to_testapp):
            _rs = tasr.client.register_schema_for_topic(self.schema_str, self.event_type,
                                                        self.host, self.port)
            self.assertEqual(self.schema_str, _rs.schema_str, 'Schema string modified!')
            self.assertIn(self.event_type, _rs.topics, 'Topic not in registered schema object.')

    def test_obj_register_schema(self):
        with httmock.HTTMock(self.route_to_testapp):
            _client = tasr.client.TASRClient(self.host, self.port)
            _rs = _client.register(self.schema_str, self.event_type)
            self.assertEqual(self.schema_str, _rs.schema_str, 'Schema string modified!')
            self.assertIn(self.event_type, _rs.topics, 'Topic not in registered schema object.')

    def test_bare_reg_fail_on_empty_schema(self):
        with httmock.HTTMock(self.route_to_testapp):
            try:
                _rs = tasr.client.register_schema_for_topic(None, self.event_type,
                                                            self.host, self.port)
                self.fail('should have thrown a TASRError')
            except tasr.client.TASRError as te:
                self.assertTrue(te, 'Missing TASRError')

    def test_obj_reg_fail_on_empty_schema(self):
        with httmock.HTTMock(self.route_to_testapp):
            try:
                _client = tasr.client.TASRClient(self.host, self.port)
                _rs = _client.register(None, self.event_type) 
                self.fail('should have thrown a TASRError')
            except tasr.client.TASRError as te:
                self.assertTrue(te, 'Missing TASRError')

    def test_bare_reg_fail_on_invalid_schema(self):
        with httmock.HTTMock(self.route_to_testapp):
            _bad_schema = '%s }' % self.schema_str
            try:
                _rs = tasr.client.register_schema_for_topic(_bad_schema, 
                                                            self.event_type,
                                                            self.host, 
                                                            self.port)
                self.fail('should have thrown a TASRError')
            except tasr.client.TASRError as te:
                self.assertTrue(te, 'Missing TASRError')
        
    def test_obj_reg_fail_on_invalid_schema(self):
        with httmock.HTTMock(self.route_to_testapp):
            _bad_schema = '%s }' % self.schema_str
            try:
                _client = tasr.client.TASRClient(self.host, self.port)
                _rs = _client.register(_bad_schema, self.event_type)
                self.fail('should have thrown a TASRError')
            except tasr.client.TASRError as te:
                self.assertTrue(te, 'Missing TASRError')
        
    def test_bare_reg_and_rereg(self):
        with httmock.HTTMock(self.route_to_testapp):
            _rs1 = tasr.client.register_schema_for_topic(self.schema_str, 
                                                         self.event_type,
                                                         self.host, self.port)
            _rs2 = tasr.client.register_schema_for_topic(self.schema_str, 
                                                         self.event_type,
                                                         self.host, self.port)
            self.assertEqual(_rs1, _rs2, 'reg and rereg schemas unequal!')

    def test_obj_reg_and_rereg(self):
        with httmock.HTTMock(self.route_to_testapp):
            _client = tasr.client.TASRClient(self.host, self.port)
            _rs1 = _client.register(self.schema_str, self.event_type)
            _rs2 = _client.register(self.schema_str, self.event_type)
            self.assertEqual(_rs1, _rs2, 'reg and rereg schemas unequal!')

    def test_bare_reg_and_get_by_md5_id(self):
        with httmock.HTTMock(self.route_to_testapp):
            _reg_rs = tasr.client.register_schema_for_topic(self.schema_str, 
                                                            self.event_type,
                                                            self.host, self.port)
            _get_rs = tasr.client.get_registered_schema_for_id_str(_reg_rs.md5_id,
                                                                   self.host, 
                                                                   self.port)
            self.assertEqual(_reg_rs, _get_rs, 'registered and retrieved schemas unequal')

    def test_obj_reg_and_get_by_md5_id(self):
        with httmock.HTTMock(self.route_to_testapp):
            _client = tasr.client.TASRClient(self.host, self.port)
            _reg_rs = _client.register(self.schema_str, self.event_type)
            _get_rs = _client.get_for_id(_reg_rs.md5_id)
            self.assertEqual(_reg_rs, _get_rs, 'registered and retrieved schemas unequal')

    def test_bare_reg_and_get_by_sha256_id(self):
        with httmock.HTTMock(self.route_to_testapp):
            _reg_rs = tasr.client.register_schema_for_topic(self.schema_str, 
                                                            self.event_type,
                                                            self.host, self.port)
            _get_rs = tasr.client.get_registered_schema_for_id_str(_reg_rs.sha256_id,
                                                                   self.host, 
                                                                   self.port)
            self.assertEqual(_reg_rs, _get_rs, 'registered and retrieved schemas unequal')

    def test_obj_reg_and_get_by_sha256_id(self):
        with httmock.HTTMock(self.route_to_testapp):
            _client = tasr.client.TASRClient(self.host, self.port)
            _reg_rs = _client.register(self.schema_str, self.event_type)
            _get_rs = _client.get_for_id(_reg_rs.sha256_id)
            self.assertEqual(_reg_rs, _get_rs, 'registered and retrieved schemas unequal')

    def test_bare_reg_and_get_non_existent_version(self):
        with httmock.HTTMock(self.route_to_testapp):
            _reg_rs = tasr.client.register_schema_for_topic(self.schema_str, 
                                                            self.event_type,
                                                            self.host, self.port)
            _bad_ver = _reg_rs.current_version(self.event_type) + 1
            try:
                _get_rs = tasr.client.get_registered_schema_for_topic(self.event_type,
                                                                      _bad_ver,
                                                                      self.host, 
                                                                      self.port)
                self.fail('Should have thrown an TASRError')
            except tasr.client.TASRError as te:
                self.assertTrue(te, 'Missing TASRError')
        
    def test_obj_reg_and_get_non_existent_version(self):
        with httmock.HTTMock(self.route_to_testapp):
            _client = tasr.client.TASRClient(self.host, self.port)
            _reg_rs = _client.register(self.schema_str, self.event_type)
            _bad_ver = _reg_rs.current_version(self.event_type) + 1
            try:
                _get_rs = _client.get_for_topic(self.event_type, _bad_ver)
                self.fail('Should have thrown an TASRError')
            except tasr.client.TASRError as te:
                self.assertTrue(te, 'Missing TASRError')

    def test_obj_reg_50_and_get_by_version(self):
        with httmock.HTTMock(self.route_to_testapp):
            _client = tasr.client.TASRClient(self.host, self.port)
            _schemas = []
            for _v in range(1, 50):
                _ver_schema_str = self.schema_str.replace('tagged.events', 
                                                          'tagged.events.%s' % _v, 1)
                _schemas.append(_ver_schema_str)
                _rs = _client.register(_ver_schema_str, self.event_type)
                self.assertEqual(_ver_schema_str, _rs.schema_str, 'Schema string modified!')
                self.assertIn(self.event_type, _rs.topics, 'Topic not in registered schema object.')
                
            for _v in range(1, 50):
                _rs = _client.get_for_topic(self.event_type, _v)
                self.assertEqual(_schemas[_v-1], _rs.canonical_schema_str, 'Unexpected version.')


    def test_obj_reg_regmod_reg_then_get_ver_1(self):
        with httmock.HTTMock(self.route_to_testapp):
            _client = tasr.client.TASRClient(self.host, self.port)
            _rs1 = _client.register(self.schema_str, self.event_type)
            _ssv2 = self.schema_str.replace('tagged.events', 'tagged.events.alt', 1)
            _rs2 = _client.register(_ssv2, self.event_type)
            _rs3 = _client.register(self.schema_str, self.event_type)
            self.assertEqual(3, _rs3.current_version(self.event_type), 'unexpected version')

            # now get version 1 -- should be same schema, and should list 
            # requested version as "current"
            _rs = _client.get_for_topic(self.event_type, 1)
            self.assertEqual(_rs1.canonical_schema_str, _rs.canonical_schema_str, 
                             'Unexpected schema string change between v1 and v3.')
            self.assertEqual(1, _rs.current_version(self.event_type), 
                            'Expected different current version value.')
    
    def test_obj_multi_topic_reg(self):
        with httmock.HTTMock(self.route_to_testapp):
            _client = tasr.client.TASRClient(self.host, self.port)
            _rs1 = _client.register(self.schema_str, self.event_type)
            _alt_topic = 'bob'
            _rs2 = _client.register(self.schema_str, _alt_topic)
            self.assertEqual(1, _rs2.current_version(self.event_type), 
                             'Expected version of 1.')
            self.assertEqual(1, _rs2.current_version(_alt_topic), 
                             'Expected version of 1.')

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTASRClient)
    unittest.TextTestRunner(verbosity=2).run(suite)
