'''
Created on May 7, 2014

@author: cmills
'''

import sys
import os
TEST_DIR = os.path.abspath(os.path.dirname(__file__))
SRC_DIR = os.path.abspath(os.path.dirname('%s/../../src/py/tagged' % TEST_DIR))
sys.path.insert(0, os.path.join(TEST_DIR, SRC_DIR))
FIX_DIR = os.path.abspath(os.path.dirname("%s/../fixtures/" % TEST_DIR))

import unittest
import tasr.app
import tasr.client
import requests
import copy
from requests.packages.urllib3._collections import HTTPHeaderDict
from requests.packages.urllib3.response import HTTPResponse
import httmock
from webtest import TestApp, TestRequest


class TestTASRClient(unittest.TestCase):

    def setUp(self):
        self.event_type = "gold"
        self.avsc_file = "%s/schemas/%s.avsc" % (FIX_DIR, self.event_type)
        self.schema_str = open(self.avsc_file, "r").read()
        self.tasr = TestApp(tasr.app.TASR_APP)
        # client settings
        self.host = 'localhost'  # should match netloc below
        self.port = 8080         # should match netloc below

    def tearDown(self):
        # this clears out redis after each test -- careful!
        for k in tasr.app.ASR.redis.keys():
            tasr.app.ASR.redis.delete(k)

    @httmock.urlmatch(netloc=r'localhost:8080')
    def route_to_testapp(self, url, requests_req):
        '''This is some tricky stuff.  To test the client methods, we need the
        responses package calls to route to our webtest TestApp WSGI wrapper.
        We use httmock to intercept the requests call, then we handle
        processing in this function instead -- calling the TestApp wrapper.
        '''

        # create a webtest TestRequest from the requests PreparedRequest
        webtest_req = TestRequest.blank(requests_req.url,
                                        method=requests_req.method,
                                        body=requests_req.body,
                                        headers=requests_req.headers)

        # have the TestApp wrapper process the TestRequest
        webtest_resp = self.tasr.request(webtest_req)

        '''webtest responses support multiple headers with the same key, while
        the requests package holds them in a case-insensitive dict of lists of
        (key,value) tuples.  We need to translate by hand here to keep cases
        with multiple headers with the same key
        '''
        headers = HTTPHeaderDict()
        for key, value in webtest_resp.headers.iteritems():
            headers.add(key, value)

        # use the webtest TestResponse to build a new requests HTTPResponse
        requests_http_resp = HTTPResponse(body=webtest_resp.body,
                                           headers=headers,
                                           status=webtest_resp.status_code)

        # get an HTTPAdaptor, then use it to build the requests Response object
        a = requests.adapters.HTTPAdapter()
        requests_resp = a.build_response(requests_req, requests_http_resp)

        '''For some reason, we need to explicitly set the _content attribute
        after the response object is built -- it is already in there as
        raw.data, but it doesn't make it to _content, so it never hits
        content() without this intervention.
        '''
        requests_resp._content = webtest_resp.body
        return requests_resp

    ########################################################################
    # registration tests
    ########################################################################
    def bare_register_schema_skeleton(self, schema_str):
        '''register_schema_for_topic() - skeleton test'''
        with httmock.HTTMock(self.route_to_testapp):
            func = tasr.client.register_schema_for_topic
            rs = func(schema_str, self.event_type, self.host, self.port)
            self.assertEqual(schema_str, rs.schema_str,
                             'Schema string modified!')
            self.assertIn(self.event_type, rs.topics,
                          'Topic not in registered schema object.')
            self.assertIn(self.event_type, rs.ts_dict.keys(),
                          'Topic not in registration timestamps.')
            return rs

    def obj_register_schema_skeleton(self, schema_str):
        '''TASRClient.register() - skeleton test'''
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client.TASRClient(self.host, self.port)
            rs = client.register(schema_str, self.event_type)
            self.assertEqual(schema_str, rs.schema_str,
                             'Schema string modified!')
            self.assertIn(self.event_type, rs.topics,
                          'Topic not in registered schema object.')
            self.assertIn(self.event_type, rs.ts_dict.keys(),
                          'Topic not in registration timestamps.')
            return rs

    def test_bare_register_schema(self):
        '''register_schema_for_topic() - as expected'''
        self.bare_register_schema_skeleton(self.schema_str)

    def test_obj_register_schema(self):
        '''TASRClient.register() - as expected'''
        self.obj_register_schema_skeleton(self.schema_str)

    def test_bare_reg_fail_on_empty_schema(self):
        '''register_schema_for_topic() - fail on empty schema'''
        try:
            self.bare_register_schema_skeleton(None)
            self.fail('should have thrown a TASRError')
        except tasr.client.TASRError as te:
            self.assertTrue(te, 'Missing TASRError')

    def test_obj_reg_fail_on_empty_schema(self):
        '''TASRClient.register() - fail on empty schema'''
        try:
            self.obj_register_schema_skeleton(None)
            self.fail('should have thrown a TASRError')
        except tasr.client.TASRError as te:
            self.assertTrue(te, 'Missing TASRError')

    def test_bare_reg_fail_on_invalid_schema(self):
        '''register_schema_for_topic() - fail on invalid schema'''
        try:
            bad_schema = '%s }' % self.schema_str
            self.bare_register_schema_skeleton(bad_schema)
            self.fail('should have thrown a TASRError')
        except tasr.client.TASRError as te:
            self.assertTrue(te, 'Missing TASRError')

    def test_obj_reg_fail_on_invalid_schema(self):
        '''TASRClient.register() - fail on invalid schema'''
        try:
            bad_schema = '%s }' % self.schema_str
            self.obj_register_schema_skeleton(bad_schema)
            self.fail('should have thrown a TASRError')
        except tasr.client.TASRError as te:
            self.assertTrue(te, 'Missing TASRError')

    def test_bare_reg_and_rereg(self):
        '''register_schema_for_topic() - multi calls, same schema'''
        rs1 = self.bare_register_schema_skeleton(self.schema_str)
        rs2 = self.bare_register_schema_skeleton(self.schema_str)
        self.assertEqual(rs1, rs2, 'reg and rereg schemas unequal!')

    def test_obj_reg_and_rereg(self):
        '''TASRClient.register() - multi calls, same schema'''
        rs1 = self.obj_register_schema_skeleton(self.schema_str)
        rs2 = self.obj_register_schema_skeleton(self.schema_str)
        self.assertEqual(rs1, rs2, 'reg and rereg schemas unequal!')

    ########################################################################
    # retrieval tests for TASR API
    ########################################################################
    def bare_get_for_id_str_skeleton(self, id_str):
        with httmock.HTTMock(self.route_to_testapp):
            func = tasr.client.get_reg_schema_for_id_str
            rs = func(id_str, self.host, self.port)
            self.assertIn(id_str, (rs.sha256_id, rs.md5_id), 'ID missing')
            return rs

    def bare_get_for_topic_skeleton(self, topic, version):
        with httmock.HTTMock(self.route_to_testapp):
            func = tasr.client.get_reg_schema_for_topic
            return func(topic, version, self.host, self.port)

    def obj_get_for_id_str_skeleton(self, id_str):
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client.TASRClient(self.host, self.port)
            rs = client.get_for_id_str(id_str)
            self.assertIn(id_str, (rs.sha256_id, rs.md5_id), 'ID missing')
            return rs

    def obj_get_for_topic_skeleton(self, topic, version):
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client.TASRClient(self.host, self.port)
            return client.get_for_topic(topic, version)

    def test_bare_reg_and_get_by_md5_id(self):
        '''get_reg_schema_for_id_str() - with md5 ID'''
        reg_rs = self.bare_register_schema_skeleton(self.schema_str)
        get_rs = self.bare_get_for_id_str_skeleton(reg_rs.md5_id)
        self.assertEqual(reg_rs, get_rs, 'got unexpected schema')

    def test_obj_reg_and_get_by_md5_id(self):
        '''TASRClient.get_for_id_str() - with md5 ID'''
        reg_rs = self.obj_register_schema_skeleton(self.schema_str)
        get_rs = self.obj_get_for_id_str_skeleton(reg_rs.md5_id)
        self.assertEqual(reg_rs, get_rs, 'got unexpected schema')

    def test_bare_reg_and_get_by_sha256_id(self):
        '''get_reg_schema_for_id_str() - with sha256 ID'''
        reg_rs = self.bare_register_schema_skeleton(self.schema_str)
        get_rs = self.bare_get_for_id_str_skeleton(reg_rs.sha256_id)
        self.assertEqual(reg_rs, get_rs, 'got unexpected schema')

    def test_obj_reg_and_get_by_sha256_id(self):
        '''TASRClient.get_for_id_str() - with sha256 ID'''
        reg_rs = self.obj_register_schema_skeleton(self.schema_str)
        get_rs = self.obj_get_for_id_str_skeleton(reg_rs.sha256_id)
        self.assertEqual(reg_rs, get_rs, 'got unexpected schema')

    def test_bare_reg_and_get_non_existent_version(self):
        '''get_reg_schema_for_topic() - bad version'''
        reg_rs = self.bare_register_schema_skeleton(self.schema_str)
        bad_ver = reg_rs.current_version(self.event_type) + 1
        try:
            self.bare_get_for_topic_skeleton(self.schema_str, bad_ver)
            self.fail('Should have thrown an TASRError')
        except tasr.client.TASRError as te:
            self.assertTrue(te, 'Missing TASRError')

    def test_obj_reg_and_get_non_existent_version(self):
        '''TASRClient.get_for_topic() - bad version'''
        reg_rs = self.obj_register_schema_skeleton(self.schema_str)
        bad_ver = reg_rs.current_version(self.event_type) + 1
        try:
            self.obj_get_for_topic_skeleton(self.schema_str, bad_ver)
            self.fail('Should have thrown an TASRError')
        except tasr.client.TASRError as te:
            self.assertTrue(te, 'Missing TASRError')

    def test_obj_reg_50_and_get_by_version(self):
        '''TASRClient.get_for_topic() - multiple versions'''
        schemas = []
        for v in range(1, 50):
            ver_schema_str = copy.copy(self.schema_str)
            ver_schema_str = ver_schema_str.replace('tagged.events',
                                                    'tagged.events.%s' % v, 1)
            schemas.append(ver_schema_str)
            rs = self.obj_register_schema_skeleton(ver_schema_str)
            self.assertEqual(ver_schema_str, rs.schema_str,
                             'Schema string modified!')
            self.assertIn(self.event_type, rs.topics,
                          'Topic not in registered schema object.')
        # now pull them by version and check they match what we sent originally
        for v in range(1, 50):
            rs = self.obj_get_for_topic_skeleton(self.event_type, v)
            self.assertEqual(schemas[v - 1], rs.canonical_schema_str,
                             'Unexpected version.')

    def test_obj_reg_regmod_reg_then_get_ver_1(self):
        '''TASRClient.get_for_topic() - non-sequential re-reg'''
        alt_schema_str = copy.copy(self.schema_str)
        alt_schema_str = alt_schema_str.replace('tagged.events',
                                                'tagged.events.alt', 1)
        rs1 = self.obj_register_schema_skeleton(self.schema_str)
        self.obj_register_schema_skeleton(alt_schema_str)
        rs3 = self.obj_register_schema_skeleton(self.schema_str)
        self.assertEqual(3, rs3.current_version(self.event_type),
                         'unexpected version')
        # now get version 1 -- should be same schema, and should list
        # requested version as "current"
        rs = self.obj_get_for_topic_skeleton(self.event_type, 1)
        self.assertEqual(rs1.canonical_schema_str, rs.canonical_schema_str,
                         'Unexpected schema string change between v1 and v3.')
        self.assertEqual(1, rs.current_version(self.event_type),
                        'Expected different current version value.')

    def test_obj_multi_topic_reg(self):
        '''TASRClient.get_for_topic() - one schema, multiple topics'''
        self.obj_register_schema_skeleton(self.schema_str)
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client.TASRClient(self.host, self.port)
            alt_topic = 'bob'
            rs = client.register(self.schema_str, alt_topic)
            self.assertEqual(1, rs.current_version(self.event_type),
                             'Expected version of 1.')
            self.assertEqual(1, rs.current_version(alt_topic),
                             'Expected version of 1.')

    def test_obj_get_by_schema_str(self):
        '''TASRClient.get_for_schema_str() - with and without a subject'''
        rs1 = self.obj_register_schema_skeleton(self.schema_str)
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client.TASRClient(self.host, self.port)
            # first try specifying the topic/subject explicitly (uses S+V API)
            rs2 = client.get_for_schema_str(self.schema_str, self.event_type)
            self.assertEqual(rs1, rs2, 'unexpected schema returned')
            # now try leaving the topic/subject out (uses TASR API)
            rs3 = client.get_for_schema_str(self.schema_str)
            self.assertEqual(rs1, rs3, 'unexpected schema returned')

    def test_bare_get_by_subject_and_schema_str(self):
        '''get_reg_schema_for_subject_and_schema_str()'''
        rs1 = self.obj_register_schema_skeleton(self.schema_str)
        with httmock.HTTMock(self.route_to_testapp):
            func = tasr.client.get_reg_schema_for_subject_and_schema_str
            rs2 = func(self.event_type, self.schema_str, self.host, self.port)
            self.assertEqual(rs1, rs2, 'unexpected schema returned')

    def test_get_get_all_subjects(self):
        '''TASRClient.get_all_subjects()'''
        self.obj_register_schema_skeleton(self.schema_str)
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client.TASRClient(self.host, self.port)
            subject_list = client.get_all_subjects()
            self.assertListEqual(subject_list, [self.event_type, ],
                                 'unexpected subject list: %s' % subject_list)

    def test_get_get_current_topic_versions(self):
        '''TASRClient.get_current_topic_versions()'''
        self.obj_register_schema_skeleton(self.schema_str)
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client.TASRClient(self.host, self.port)
            cur_ver_dict = client.get_current_topic_versions()
            self.assertDictEqual(cur_ver_dict, {self.event_type: 1},
                                 'unexpected cv dict: %s' % cur_ver_dict)


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTASRClient)
    unittest.TextTestRunner(verbosity=2).run(suite)
