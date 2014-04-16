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

import tasr.service

def extract_hdict(hlist, prefix=None):
    _hdict = dict()
    for _h in hlist:
        (_k, _v) = _h
        _k = _k.upper()
        if prefix:
            prefix = prefix.upper()
            if _k[0:len(prefix)] == prefix:
                _hdict[_k] = _v
        else:
            _hdict[_k] = _v
    return _hdict

class TestTASRService(unittest.TestCase):

    def setUp(self):
        self.event_type = "gold"
        self.avsc_file = "%s/schemas/%s.avsc" % (_fix_dir, self.event_type)
        self.schema_str = open(self.avsc_file, "r").read()
        self.tasr_service = TestApp(tasr.service.app)
        self.url = 'http://localhost:8080/tasr/topic/%s' % self.event_type
    
    def tearDown(self):
        # this clears out redis after each test -- careful!
        for _k in tasr.service.ASR.redis.keys():
            tasr.service.ASR.redis.delete(_k)
    
    
    def test_register_schema(self):
        _resp = self.tasr_service.request(self.url, method='PUT', 
                                          content_type='application/json', 
                                          body=self.schema_str)

        self.assertEqual(200, _resp.status_code, 
                         u'Non-200 status code: %s' % _resp.status_code)

        _expected_x_headers = ['X-SCHEMA-TOPIC', 'X-SCHEMA-VERSION',
                               'X-SCHEMA-MD5-ID', 'X-SCHEMA-SHA256-ID']
        
        _hdict = extract_hdict(_resp.headerlist, 'X-SCHEMA-')
        
        for _xk in _expected_x_headers:
            self.assertIn(_xk, _hdict.keys(), u'%s header missing.' % _xk)

        self.assertEqual(self.event_type, _hdict['X-SCHEMA-TOPIC'], 
                         u'X-Schema-Topic header bad or missing.')

    def test_reg_fail_on_empty_schema(self):
        _resp = self.tasr_service.request(self.url, method='PUT', 
                                          content_type='application/json',
                                          expect_errors=True,
                                          body=None)
        self.assertEqual(400, _resp.status_int, u'Expected a 400 status code.')
        

    def test_reg_fail_on_invalid_schema(self):
        _resp = self.tasr_service.request(self.url, method='PUT', 
                                          content_type='application/json',
                                          expect_errors=True,
                                          body="%s }" % self.schema_str)
        self.assertEqual(400, _resp.status_int, u'Expected a 400 status code.')

    def test_reg_fail_on_bad_content_type(self):
        _resp = self.tasr_service.request(self.url, method='PUT', 
                                          content_type='text',
                                          expect_errors=True,
                                          body=self.schema_str)
        self.assertEqual(406, _resp.status_int, u'Expected a 406 status code.')


    def test_reregister_schema(self):
        _resp = self.tasr_service.request(self.url, method='PUT', 
                                          content_type='application/json', 
                                          body=self.schema_str)
        self.assertEqual(200, _resp.status_code, 
                         u'Non-200 status code: %s' % _resp.status_code)

        _hdict = extract_hdict(_resp.headerlist, 'X-SCHEMA-')
        _ver_0 = _hdict['X-SCHEMA-VERSION']
        self.assertNotEqual(None, _ver_0, u'Invalid initial version: %s' % _ver_0)

        # on the reregistration, we should get the same version (timestamp) back
        _resp1 = self.tasr_service.request(self.url, method='PUT', 
                                           content_type='application/json', 
                                           body=self.schema_str)
        _hdict1 = extract_hdict(_resp1.headerlist, 'X-SCHEMA-')
        _ver_1 = _hdict1['X-SCHEMA-VERSION']
        self.assertEqual(_ver_0, _ver_1, u'Reregistration produced a different version.')
        
    






















    
if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTASRService)
    unittest.TextTestRunner(verbosity=2).run(suite)
