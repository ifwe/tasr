'''
Created on Apr 8, 2014

@author: cmills
'''
import sys, os
test_dir = os.path.abspath(os.path.dirname(__file__))
src_dir = os.path.abspath(os.path.dirname('%s/../../src/py/tagged' % test_dir))
sys.path.insert(0, os.path.join(test_dir, src_dir))
fix_dir = os.path.abspath(os.path.dirname("%s/../fixtures/" % test_dir))

import unittest
from webtest import TestApp
import tasr.app

def extract_hdict(hlist, prefix=None):
    hdict = dict()
    for h in hlist:
        (k, v) = h
        k = k.upper()
        if prefix:
            prefix = prefix.upper()
            if k[0:len(prefix)] == prefix:
                if not k in hdict:
                    hdict[k] = []
                hdict[k].append(v)
        else:
            if not k in hdict:
                hdict[k] = []
            hdict[k].append(v)
    return hdict

class TestTASRApp(unittest.TestCase):

    def setUp(self):
        self.event_type = "gold"
        self.avsc_file = "%s/schemas/%s.avsc" % (fix_dir, self.event_type)
        self.schema_str = open(self.avsc_file, "r").read()
        self.tasr_app = TestApp(tasr.app.TASR_APP)
        self.topic_url = 'http://localhost:8080/tasr/topic/%s' % self.event_type
        self.id_url_prefix = 'http://localhost:8080/tasr/id'
        self.content_type = 'application/json; charset=utf8'
    
    def tearDown(self):
        # this clears out redis after each test -- careful!
        for k in tasr.app.ASR.redis.keys():
            tasr.app.ASR.redis.delete(k)
    
    
    def test_register_schema(self):
        resp = self.tasr_app.request(self.topic_url, method='PUT', 
                                     content_type=self.content_type, 
                                     body=self.schema_str)

        self.assertEqual(200, resp.status_code, 
                         u'Non-200 status code: %s' % resp.status_code)

        expected_x_headers = ['X-SCHEMA-TOPIC-VERSION','X-SCHEMA-SHA256-ID',
                              'X-SCHEMA-TOPIC-VERSION-TIMESTAMP','X-SCHEMA-MD5-ID']
        
        hdict = extract_hdict(resp.headerlist, 'X-SCHEMA-')
        
        for xk in expected_x_headers:
            self.assertIn(xk, hdict.keys(), u'%s header missing.' % xk)

        for tv in hdict['X-SCHEMA-TOPIC-VERSION']:
            t = tv.split('=')[0]
            self.assertEqual(self.event_type, t, u'Unexpected topic.')

        for ts in hdict['X-SCHEMA-TOPIC-VERSION-TIMESTAMP']:
            t = ts.split('=')[0]
            self.assertEqual(self.event_type, t, u'Unexpected topic.')


    def test_reg_fail_on_empty_schema(self):
        resp = self.tasr_app.request(self.topic_url, method='PUT', 
                                     content_type=self.content_type,
                                     expect_errors=True,
                                     body=None)
        self.assertEqual(400, resp.status_int, u'Expected a 400 status code.')
        

    def test_reg_fail_on_invalid_schema(self):
        resp = self.tasr_app.request(self.topic_url, method='PUT', 
                                     content_type=self.content_type,
                                     expect_errors=True,
                                     body="%s }" % self.schema_str)
        self.assertEqual(400, resp.status_int, u'Expected a 400 status code.')

    def test_reg_fail_on_bad_content_type(self):
        resp = self.tasr_app.request(self.topic_url, method='PUT', 
                                     content_type='text/plain; charset=utf8',
                                     expect_errors=True,
                                     body=self.schema_str)
        self.assertEqual(406, resp.status_int, u'Expected a 406 status code.')


    def test_reg_and_rereg(self):
        resp = self.tasr_app.request(self.topic_url, method='PUT', 
                                     content_type=self.content_type, 
                                     body=self.schema_str)
        self.assertEqual(200, resp.status_code, 
                         u'Non-200 status code: %s' % resp.status_code)

        hdict = extract_hdict(resp.headerlist, 'X-SCHEMA-')
        t0, v0 = hdict['X-SCHEMA-TOPIC-VERSION'][0].split('=')
        self.assertNotEqual(None, v0, u'Invalid initial version: %s' % v0)

        # on the reregistration, we should get the same version back
        resp1 = self.tasr_app.request(self.topic_url, method='PUT', 
                                      content_type=self.content_type, 
                                      body=self.schema_str)
        hdict1 = extract_hdict(resp1.headerlist, 'X-SCHEMA-')
        t1, v1 = hdict1['X-SCHEMA-TOPIC-VERSION'][0].split('=')
        self.assertEqual(t0, t1, u'Reregistration produced a different topic.')
        self.assertEqual(v0, v1, u'Reregistration produced a different version.')
        
    def test_reg_and_get_by_md5_id(self):
        put_resp = self.tasr_app.request(self.topic_url, method='PUT', 
                                         content_type=self.content_type, 
                                         body=self.schema_str)
        hdict = extract_hdict(put_resp.headerlist, 'X-SCHEMA-')
        id_str = hdict['X-SCHEMA-MD5-ID'][0]
        get_resp = self.tasr_app.request("%s/%s" % (self.id_url_prefix, id_str), 
                                              method='GET')
        self.assertEqual(200, get_resp.status_code, 
                         u'Non-200 status code: %s' % get_resp.status_code)
        self.assertEqual(self.schema_str, get_resp.body, 
                         u'Unexpected body: %s' % get_resp.body)

    def test_reg_and_get_by_sha256_id(self):
        put_resp = self.tasr_app.request(self.topic_url, method='PUT', 
                                         content_type=self.content_type, 
                                         body=self.schema_str)
        hdict = extract_hdict(put_resp.headerlist, 'X-SCHEMA-')
        id_str = hdict['X-SCHEMA-SHA256-ID'][0]
        get_resp = self.tasr_app.request("%s/%s" % (self.id_url_prefix, id_str), 
                                              method='GET')
        self.assertEqual(200, get_resp.status_code, 
                         u'Non-200 status code: %s' % get_resp.status_code)
        self.assertEqual(self.schema_str, get_resp.body, 
                         u'Unexpected body: %s' % get_resp.body)

    def test_reg_and_get_non_existent_version(self):
        put_resp = self.tasr_app.request(self.topic_url, method='PUT', 
                                         content_type=self.content_type, 
                                         body=self.schema_str)
        hdict = extract_hdict(put_resp.headerlist, 'X-SCHEMA-')
        v = hdict['X-SCHEMA-TOPIC-VERSION'][0].split('=')[1]
        query = "%s/%s" % (self.topic_url, (int(v) + 1))
        get_resp = self.tasr_app.request(query, method='GET', expect_errors=True)
        self.assertEqual(404, get_resp.status_int, u'Expected a 404 status code.')

    def test_reg_50_and_get_by_version(self):
        schemas = []
        for v in range(1, 50):
            ver_schema_str = self.schema_str.replace('tagged.events', 'tagged.events.%s' % v, 1)
            schemas.append(ver_schema_str)
            put_resp = self.tasr_app.request(self.topic_url, method='PUT', 
                                             content_type=self.content_type, 
                                             body=ver_schema_str)
            self.assertEqual(200, put_resp.status_code, 
                             u'Non-200 status code: %s' % put_resp.status_code)
            
        for v in range(1, 50):
            query = "%s/%s" % (self.topic_url, v)
            get_resp = self.tasr_app.request(query, method='GET')
            self.assertEqual(200, get_resp.status_code, 
                             u'Non-200 status code: %s' % get_resp.status_code)
            self.assertEqual(schemas[v - 1], get_resp.body, 
                             u'Unexpected body: %s' % get_resp.body)
    
    def test_reg_regmod_reg_then_get_ver_1(self):
        put_resp = self.tasr_app.request(self.topic_url, method='PUT', 
                                         content_type=self.content_type, 
                                         body=self.schema_str)
        self.assertEqual(200, put_resp.status_code, 
                         u'Non-200 status code: %s' % put_resp.status_code)
        schema_str_2 = self.schema_str.replace('tagged.events', 'tagged.events.alt', 1)
        put_resp2 = self.tasr_app.request(self.topic_url, method='PUT', 
                                          content_type=self.content_type, 
                                          body=schema_str_2)
        self.assertEqual(200, put_resp2.status_code, 
                         u'Non-200 status code: %s' % put_resp2.status_code)
        put_resp3 = self.tasr_app.request(self.topic_url, method='PUT', 
                                          content_type=self.content_type, 
                                          body=self.schema_str)
        hdict = extract_hdict(put_resp3.headerlist, 'X-SCHEMA-')
        v = hdict['X-SCHEMA-TOPIC-VERSION'][0].split('=')[1]
        self.assertEqual(3, int(v), u'Expected third PUT to return version of 3.')

        # now get version 1 -- should be same schema, but diff ver in headers        
        query = "%s/%s" % (self.topic_url, 1)
        get_resp = self.tasr_app.request(query, method='GET', expect_errors=True)
        self.assertEqual(200, get_resp.status_code, 
                         u'Non-200 status code: %s' % get_resp.status_code)
        self.assertEqual(self.schema_str, get_resp.body, 
                         u'Unexpected body: %s' % get_resp.body)
        hdict = extract_hdict(get_resp.headerlist, 'X-SCHEMA-')
        v = hdict['X-SCHEMA-TOPIC-VERSION'][0].split('=')[1]
        self.assertEqual(1, int(v), u'Expected GET to return version of 1.')
    
    def test_multi_topic_reg(self):
        put_resp = self.tasr_app.request(self.topic_url, method='PUT', 
                                         content_type=self.content_type, 
                                         body=self.schema_str)
        self.assertEqual(200, put_resp.status_code, 
                         u'Non-200 status code: %s' % put_resp.status_code)
        alt_topic = 'bob'
        alt_topic_url = 'http://localhost:8080/tasr/topic/%s' % alt_topic
        put_resp2 = self.tasr_app.request(alt_topic_url, method='PUT', 
                                          content_type=self.content_type, 
                                          body=self.schema_str)
        tv_dict = dict()
        hdict = extract_hdict(put_resp2.headerlist, 'X-SCHEMA-')
        for tv in hdict['X-SCHEMA-TOPIC-VERSION']:
            t, v = tv.split('=')
            tv_dict[t] = int(v)
        self.assertEqual(1, tv_dict[self.event_type], u'Expected version of 1.')            
        self.assertEqual(1, tv_dict[alt_topic], u'Expected version of 1.')            

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTASRApp)
    unittest.TextTestRunner(verbosity=2).run(suite)
