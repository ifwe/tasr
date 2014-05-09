'''
Created on Apr 8, 2014

@author: cmills
'''
import sys, os
_test_dir = os.path.abspath(os.path.dirname(__file__))
_src_dir = os.path.abspath(os.path.dirname('%s/../../src/py/tagged' % _test_dir))
sys.path.insert(0, os.path.join(_test_dir, _src_dir))
_fix_dir = os.path.abspath(os.path.dirname("%s/../fixtures/" % _test_dir))

import unittest
from webtest import TestApp

import tasr.app

def extract_hdict(hlist, prefix=None):
    _hdict = dict()
    for _h in hlist:
        (_k, _v) = _h
        _k = _k.upper()
        if prefix:
            prefix = prefix.upper()
            if _k[0:len(prefix)] == prefix:
                if not _k in _hdict:
                    _hdict[_k] = []
                _hdict[_k].append(_v)
        else:
            if not _k in _hdict:
                _hdict[_k] = []
            _hdict[_k].append(_v)
    return _hdict

class TestTASRService(unittest.TestCase):

    def setUp(self):
        self.event_type = "gold"
        self.avsc_file = "%s/schemas/%s.avsc" % (_fix_dir, self.event_type)
        self.schema_str = open(self.avsc_file, "r").read()
        self.tasr_service = TestApp(tasr.app.app)
        self.topic_url = 'http://localhost:8080/tasr/topic/%s' % self.event_type
        self.id_url_prefix = 'http://localhost:8080/tasr/id'
        self.content_type = 'application/json; charset=utf8'
    
    def tearDown(self):
        # this clears out redis after each test -- careful!
        for _k in tasr.app.ASR.redis.keys():
            tasr.app.ASR.redis.delete(_k)
    
    
    def test_register_schema(self):
        _resp = self.tasr_service.request(self.topic_url, method='PUT', 
                                          content_type=self.content_type, 
                                          body=self.schema_str)

        self.assertEqual(200, _resp.status_code, 
                         u'Non-200 status code: %s' % _resp.status_code)

        _expected_x_headers = ['X-SCHEMA-TOPIC-VERSION','X-SCHEMA-MD5-ID', 
                               'X-SCHEMA-SHA256-ID']
        
        _hdict = extract_hdict(_resp.headerlist, 'X-SCHEMA-')
        
        for _xk in _expected_x_headers:
            self.assertIn(_xk, _hdict.keys(), u'%s header missing.' % _xk)

        for _tv in _hdict['X-SCHEMA-TOPIC-VERSION']:
            _t, _v = _tv.split('=')
            self.assertEqual(self.event_type, _t, u'Unexpected topic.')

    def test_reg_fail_on_empty_schema(self):
        _resp = self.tasr_service.request(self.topic_url, method='PUT', 
                                          content_type=self.content_type,
                                          expect_errors=True,
                                          body=None)
        self.assertEqual(400, _resp.status_int, u'Expected a 400 status code.')
        

    def test_reg_fail_on_invalid_schema(self):
        _resp = self.tasr_service.request(self.topic_url, method='PUT', 
                                          content_type=self.content_type,
                                          expect_errors=True,
                                          body="%s }" % self.schema_str)
        self.assertEqual(400, _resp.status_int, u'Expected a 400 status code.')

    def test_reg_fail_on_bad_content_type(self):
        _resp = self.tasr_service.request(self.topic_url, method='PUT', 
                                          content_type='text/plain; charset=utf8',
                                          expect_errors=True,
                                          body=self.schema_str)
        self.assertEqual(406, _resp.status_int, u'Expected a 406 status code.')


    def test_reg_and_rereg(self):
        _resp = self.tasr_service.request(self.topic_url, method='PUT', 
                                          content_type=self.content_type, 
                                          body=self.schema_str)
        self.assertEqual(200, _resp.status_code, 
                         u'Non-200 status code: %s' % _resp.status_code)

        _hdict = extract_hdict(_resp.headerlist, 'X-SCHEMA-')
        _t0, _v0 = _hdict['X-SCHEMA-TOPIC-VERSION'][0].split('=')
        self.assertNotEqual(None, _v0, u'Invalid initial version: %s' % _v0)

        # on the reregistration, we should get the same version back
        _resp1 = self.tasr_service.request(self.topic_url, method='PUT', 
                                           content_type=self.content_type, 
                                           body=self.schema_str)
        _hdict1 = extract_hdict(_resp.headerlist, 'X-SCHEMA-')
        _t1, _v1 = _hdict1['X-SCHEMA-TOPIC-VERSION'][0].split('=')
        self.assertEqual(_v0, _v1, u'Reregistration produced a different version.')
        
    def test_reg_and_get_by_md5_id(self):
        _put_resp = self.tasr_service.request(self.topic_url, method='PUT', 
                                              content_type=self.content_type, 
                                              body=self.schema_str)
        _hdict = extract_hdict(_put_resp.headerlist, 'X-SCHEMA-')
        _id = _hdict['X-SCHEMA-MD5-ID'][0]
        _get_resp = self.tasr_service.request("%s/%s" % (self.id_url_prefix, _id), 
                                              method='GET')
        self.assertEqual(200, _get_resp.status_code, 
                         u'Non-200 status code: %s' % _get_resp.status_code)
        self.assertEqual(self.schema_str, _get_resp.body, 
                         u'Unexpected body: %s' % _get_resp.body)

    def test_reg_and_get_by_sha256_id(self):
        _put_resp = self.tasr_service.request(self.topic_url, method='PUT', 
                                              content_type=self.content_type, 
                                              body=self.schema_str)
        _hdict = extract_hdict(_put_resp.headerlist, 'X-SCHEMA-')
        _id = _hdict['X-SCHEMA-SHA256-ID'][0]
        _get_resp = self.tasr_service.request("%s/%s" % (self.id_url_prefix, _id), 
                                              method='GET')
        self.assertEqual(200, _get_resp.status_code, 
                         u'Non-200 status code: %s' % _get_resp.status_code)
        self.assertEqual(self.schema_str, _get_resp.body, 
                         u'Unexpected body: %s' % _get_resp.body)

    def test_reg_and_get_non_existent_version(self):
        _put_resp = self.tasr_service.request(self.topic_url, method='PUT', 
                                              content_type=self.content_type, 
                                              body=self.schema_str)
        _hdict = extract_hdict(_put_resp.headerlist, 'X-SCHEMA-')
        _t, _v = _hdict['X-SCHEMA-TOPIC-VERSION'][0].split('=')
        _query = "%s/%s" % (self.topic_url, (int(_v) + 1))
        _get_resp = self.tasr_service.request(_query, method='GET', expect_errors=True)
        self.assertEqual(404, _get_resp.status_int, u'Expected a 404 status code.')

    def test_reg_50_and_get_by_version(self):
        _schemas = []
        for _v in range(1, 50):
            _ver_schema_str = self.schema_str.replace('tagged.events', 'tagged.events.%s' % _v, 1)
            _schemas.append(_ver_schema_str)
            _put_resp = self.tasr_service.request(self.topic_url, method='PUT', 
                                                  content_type=self.content_type, 
                                                  body=_ver_schema_str)
            self.assertEqual(200, _put_resp.status_code, 
                             u'Non-200 status code: %s' % _put_resp.status_code)
            
        for _v in range(1, 50):
            _query = "%s/%s" % (self.topic_url, _v)
            _get_resp = self.tasr_service.request(_query, method='GET')
            self.assertEqual(200, _get_resp.status_code, 
                             u'Non-200 status code: %s' % _get_resp.status_code)
            self.assertEqual(_schemas[_v - 1], _get_resp.body, 
                             u'Unexpected body: %s' % _get_resp.body)
    
    def test_reg_regmod_reg_then_get_ver_1(self):
        _put_resp = self.tasr_service.request(self.topic_url, method='PUT', 
                                              content_type=self.content_type, 
                                              body=self.schema_str)
        _schema_str_2 = self.schema_str.replace('tagged.events', 'tagged.events.alt', 1)
        _put_resp2 = self.tasr_service.request(self.topic_url, method='PUT', 
                                               content_type=self.content_type, 
                                               body=_schema_str_2)
        _put_resp3 = self.tasr_service.request(self.topic_url, method='PUT', 
                                               content_type=self.content_type, 
                                               body=self.schema_str)
        _hdict = extract_hdict(_put_resp3.headerlist, 'X-SCHEMA-')
        _t, _v = _hdict['X-SCHEMA-TOPIC-VERSION'][0].split('=')
        self.assertEqual(3, int(_v), u'Expected third PUT to return version of 3.')

        # now get version 1 -- should be same schema, but diff ver in headers        
        _query = "%s/%s" % (self.topic_url, 1)
        _get_resp = self.tasr_service.request(_query, method='GET', expect_errors=True)
        self.assertEqual(200, _get_resp.status_code, 
                         u'Non-200 status code: %s' % _get_resp.status_code)
        self.assertEqual(self.schema_str, _get_resp.body, 
                         u'Unexpected body: %s' % _get_resp.body)
        _hdict = extract_hdict(_get_resp.headerlist, 'X-SCHEMA-')
        _t, _v = _hdict['X-SCHEMA-TOPIC-VERSION'][0].split('=')
        self.assertEqual(1, int(_v), u'Expected GET to return version of 1.')
    
    def test_multi_topic_reg(self):
        _put_resp = self.tasr_service.request(self.topic_url, method='PUT', 
                                              content_type=self.content_type, 
                                              body=self.schema_str)
        _alt_topic = 'bob'
        _alt_topic_url = 'http://localhost:8080/tasr/topic/%s' % _alt_topic
        _put_resp2 = self.tasr_service.request(_alt_topic_url, method='PUT', 
                                               content_type=self.content_type, 
                                               body=self.schema_str)
        _tv_dict = dict()
        _hdict = extract_hdict(_put_resp2.headerlist, 'X-SCHEMA-')
        for _tv in _hdict['X-SCHEMA-TOPIC-VERSION']:
            _t, _v = _tv.split('=')
            _tv_dict[_t] = int(_v)
        self.assertEqual(1, _tv_dict[self.event_type], u'Expected version of 1.')            
        self.assertEqual(1, _tv_dict[_alt_topic], u'Expected version of 1.')            

    
if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTASRService)
    unittest.TextTestRunner(verbosity=2).run(suite)
