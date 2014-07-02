'''
Created on July 1, 2014

@author: cmills
'''

import sys
import os
TEST_DIR = os.path.abspath(os.path.dirname(__file__))
SRC_DIR = os.path.abspath(os.path.dirname('%s/../../src/py/tagged' % TEST_DIR))
sys.path.insert(0, os.path.join(TEST_DIR, SRC_DIR))
FIX_DIR = os.path.abspath(os.path.dirname("%s/../fixtures/" % TEST_DIR))

import unittest
from webtest import TestApp
import tasr.app
import StringIO


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


class TestTASRAppSVAPI(unittest.TestCase):
    '''These tests check that the TASR S+V REST API, expected by the Avro-1124
    repo code.  This does not check the TASR native API calls.
    '''

    def setUp(self):
        self.event_type = "gold"
        self.avsc_file = "%s/schemas/%s.avsc" % (FIX_DIR, self.event_type)
        self.schema_str = open(self.avsc_file, "r").read()
        self.tasr_app = TestApp(tasr.app.TASR_APP)
        self.url_prefix = 'http://localhost:8080/tasr/subject'
        self.subject_url = '%s/%s' % (self.url_prefix, self.event_type)
        self.content_type = 'application/json; charset=utf8'

    def tearDown(self):
        # this clears out redis after each test -- careful!
        for k in tasr.app.ASR.redis.keys():
            tasr.app.ASR.redis.delete(k)

    # registration
    def test_register_schema(self):
        '''PUT /tasr/subject/<subject>/register - as expected'''
        reg_url = '%s/register' % self.subject_url
        resp = self.tasr_app.request(reg_url, method='PUT',
                                     content_type=self.content_type,
                                     body=self.schema_str)
        self.assertEqual(200, resp.status_code,
                         u'Non-200 status code: %s' % resp.status_code)
        expected_x_headers = ['X-SCHEMA-TOPIC-VERSION',
                              'X-SCHEMA-SHA256-ID',
                              'X-SCHEMA-TOPIC-VERSION-TIMESTAMP',
                              'X-SCHEMA-MD5-ID']

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
        '''PUT /tasr/subject/<subject>/register - empty schema'''
        reg_url = '%s/register' % self.subject_url
        resp = self.tasr_app.request(reg_url, method='PUT',
                                     content_type=self.content_type,
                                     expect_errors=True,
                                     body=None)
        self.assertEqual(400, resp.status_int, u'Expected a 400 status code.')

    def test_reg_fail_on_invalid_schema(self):
        '''PUT /tasr/subject/<subject>/register - bad schema'''
        reg_url = '%s/register' % self.subject_url
        resp = self.tasr_app.request(reg_url, method='PUT',
                                     content_type=self.content_type,
                                     expect_errors=True,
                                     body="%s }" % self.schema_str)
        self.assertEqual(400, resp.status_int, u'Expected a 400 status code.')

    def test_reg_fail_on_bad_content_type(self):
        '''PUT /tasr/subject/<subject>/register - bad content type'''
        reg_url = '%s/register' % self.subject_url
        resp = self.tasr_app.request(reg_url, method='PUT',
                                     content_type='text/plain; charset=utf8',
                                     expect_errors=True,
                                     body=self.schema_str)
        self.assertEqual(406, resp.status_int, u'Expected a 406 status code.')

    def test_reg_and_rereg(self):
        '''PUT /tasr/subject/<subject>/register - multiple calls, one schema'''
        reg_url = '%s/register' % self.subject_url
        resp = self.tasr_app.request(reg_url, method='PUT',
                                     content_type=self.content_type,
                                     body=self.schema_str)
        self.assertEqual(200, resp.status_code,
                         u'Non-200 status code: %s' % resp.status_code)

        hdict = extract_hdict(resp.headerlist, 'X-SCHEMA-')
        t0, v0 = hdict['X-SCHEMA-TOPIC-VERSION'][0].split('=')
        self.assertNotEqual(None, v0, u'Invalid initial version: %s' % v0)

        # on the re-registration, we should get the same version back
        resp1 = self.tasr_app.request(reg_url, method='PUT',
                                      content_type=self.content_type,
                                      body=self.schema_str)
        hdict1 = extract_hdict(resp1.headerlist, 'X-SCHEMA-')
        t1, v1 = hdict1['X-SCHEMA-TOPIC-VERSION'][0].split('=')
        self.assertEqual(t0, t1, u'Re-registration produced a diff subject.')
        self.assertEqual(v0, v1, u'Re-registration produced a diff version.')

    def test_multi_subject_reg(self):
        '''PUT /tasr/subject/<subject>/register - multi subjects, one schema'''
        reg_url = '%s/register' % self.subject_url
        put_resp = self.tasr_app.request(reg_url, method='PUT',
                                         content_type=self.content_type,
                                         body=self.schema_str)
        self.assertEqual(200, put_resp.status_code,
                         u'Non-200 status code: %s' % put_resp.status_code)
        alt_subject = 'bob'
        alt_reg_url = '%s/%s/register' % (self.url_prefix, alt_subject)
        put_resp2 = self.tasr_app.request(alt_reg_url, method='PUT',
                                          content_type=self.content_type,
                                          body=self.schema_str)
        tv_dict = dict()
        hdict = extract_hdict(put_resp2.headerlist, 'X-SCHEMA-')
        for tv in hdict['X-SCHEMA-TOPIC-VERSION']:
            t, v = tv.split('=')
            tv_dict[t] = int(v)
        self.assertEqual(1, tv_dict[self.event_type], u'Expected version 1.')
        self.assertEqual(1, tv_dict[alt_subject], u'Expected version 1.')

    # retrieval
    def test_get_latest(self):
        '''GET /tasr/subject/<subject>/latest - as expected'''
        reg_url = '%s/register' % self.subject_url
        self.tasr_app.request(reg_url, method='PUT',
                              content_type=self.content_type,
                              body=self.schema_str)
        latest_url = '%s/latest' % self.subject_url
        get_resp = self.tasr_app.request(latest_url, method='GET')
        hdict = extract_hdict(get_resp.headerlist, 'X-SCHEMA-')
        t, v = hdict['X-SCHEMA-TOPIC-VERSION'][0].split('=')
        self.assertEqual(200, get_resp.status_code,
                         u'Non-200 status code: %s' % get_resp.status_code)
        self.assertEqual(self.event_type, t, u'Unexpected topic.')
        self.assertEqual('1', v, u'Unexpected version.')
        self.assertEqual(self.schema_str, get_resp.body,
                         u'Unexpected body: %s' % get_resp.body)

    def test_reg_50_and_get_by_version(self):
        '''GET /tasr/subject/<subject>/id/<version> - as expected'''
        schemas = []
        # add a bunch of versions for our subject
        reg_url = '%s/register' % self.subject_url
        for v in range(1, 50):
            ver_schema_str = self.schema_str.replace('tagged.events',
                                                     'tagged.events.%s' % v, 1)
            schemas.append(ver_schema_str)
            put_resp = self.tasr_app.request(reg_url, method='PUT',
                                             content_type=self.content_type,
                                             body=ver_schema_str)
            self.assertEqual(200, put_resp.status_code,
                             u'Non-200 status code: %s' % put_resp.status_code)

        # step through and request each version by version number
        for v in range(1, 50):
            get_url = '%s/id/%s' % (self.subject_url, v)
            get_resp = self.tasr_app.request(get_url, method='GET')
            self.assertEqual(200, get_resp.status_code,
                             u'Non-200 status code: %s' % get_resp.status_code)
            self.assertEqual(schemas[v - 1], get_resp.body,
                             u'Unexpected body: %s' % get_resp.body)

    def test_get_for_subject_and_version_fail_on_bad_version(self):
        '''GET /tasr/subject/<subject>/id/<version> - fail on bad version'''
        reg_url = '%s/register' % self.subject_url
        put_resp = self.tasr_app.request(reg_url, method='PUT',
                                         content_type=self.content_type,
                                         body=self.schema_str)
        hdict = extract_hdict(put_resp.headerlist, 'X-SCHEMA-')
        cur_ver = hdict['X-SCHEMA-TOPIC-VERSION'][0].split('=')[1]
        get_url = "%s/id/%s" % (self.subject_url, (int(cur_ver) + 1))
        get_resp = self.tasr_app.request(get_url, method='GET',
                                         expect_errors=True)
        self.assertEqual(404, get_resp.status_int, u'Expected a 404 status.')

    def test_get_for_stale_version(self):
        '''GET /tasr/subject/<subject>/id/<version> - 1 schema as 2 versions'''
        reg_url = '%s/register' % self.subject_url
        put_resp = self.tasr_app.request(reg_url, method='PUT',
                                         content_type=self.content_type,
                                         body=self.schema_str)
        self.assertEqual(200, put_resp.status_code,
                         u'Non-200 status code: %s' % put_resp.status_code)
        schema_str_2 = self.schema_str.replace('tagged.events',
                                               'tagged.events.alt', 1)
        put_resp2 = self.tasr_app.request(reg_url, method='PUT',
                                          content_type=self.content_type,
                                          body=schema_str_2)
        self.assertEqual(200, put_resp2.status_code,
                         u'Non-200 status code: %s' % put_resp2.status_code)
        put_resp3 = self.tasr_app.request(reg_url, method='PUT',
                                          content_type=self.content_type,
                                          body=self.schema_str)
        hdict = extract_hdict(put_resp3.headerlist, 'X-SCHEMA-')
        v = hdict['X-SCHEMA-TOPIC-VERSION'][0].split('=')[1]
        self.assertEqual(3, int(v), u'Expected third PUT to return version 3.')

        # now get version 1 -- should be same schema, but diff ver in headers
        get_url = "%s/id/%s" % (self.subject_url, 1)
        get_resp = self.tasr_app.request(get_url, method='GET',
                                         expect_errors=True)
        self.assertEqual(200, get_resp.status_code,
                         u'Non-200 status code: %s' % get_resp.status_code)
        self.assertEqual(self.schema_str, get_resp.body,
                         u'Unexpected body: %s' % get_resp.body)
        hdict = extract_hdict(get_resp.headerlist, 'X-SCHEMA-')
        v = hdict['X-SCHEMA-TOPIC-VERSION'][0].split('=')[1]
        self.assertEqual(1, int(v), u'Expected GET to return version of 1.')

    def test_get_for_schema(self):
        '''POST /tasr/subject/<subject>/schema - as expected'''
        reg_url = '%s/register' % self.subject_url
        put_resp = self.tasr_app.request(reg_url, method='PUT',
                                         content_type=self.content_type,
                                         body=self.schema_str)
        put_hdict = extract_hdict(put_resp.headerlist, 'X-SCHEMA-')
        post_url = "%s/schema" % self.subject_url
        post_resp = self.tasr_app.request(post_url, method='POST',
                                          content_type=self.content_type,
                                          body=self.schema_str)
        post_hdict = extract_hdict(post_resp.headerlist, 'X-SCHEMA-')
        self.assertEqual(200, post_resp.status_code,
                         u'Non-200 status code: %s' % post_resp.status_code)
        self.assertDictEqual(put_hdict, post_hdict, 'Headers unequal.')
        self.assertEqual(self.schema_str, post_resp.body,
                         u'Unexpected body: %s' % post_resp.body)

    def test_get_for_schema_fail_empty_schema_str(self):
        '''POST /tasr/subject/<subject>/schema - fail on empty schema string'''
        reg_url = '%s/register' % self.subject_url
        self.tasr_app.request(reg_url, method='PUT',
                              content_type=self.content_type,
                              body=self.schema_str)
        post_url = "%s/schema" % self.subject_url
        resp = self.tasr_app.request(post_url, method='POST',
                                     content_type=self.content_type,
                                     expect_errors=True,
                                     body=None)
        self.assertEqual(400, resp.status_int,
                         u'Unexpected status: %s' % resp.status_int)

    def test_get_for_schema_fail_invalid_schema_str(self):
        '''POST /tasr/subject/<subject>/schema - fail on bad schema string'''
        reg_url = '%s/register' % self.subject_url
        self.tasr_app.request(reg_url, method='PUT',
                              content_type=self.content_type,
                              body=self.schema_str)
        post_url = "%s/schema" % self.subject_url
        resp = self.tasr_app.request(post_url, method='POST',
                                     content_type=self.content_type,
                                     expect_errors=True,
                                     body="%s }" % self.schema_str)
        self.assertEqual(400, resp.status_int,
                         u'Unexpected status: %s' % resp.status_int)

    def test_get_for_schema_fail_unregistered_schema_str(self):
        '''POST /tasr/subject/<subject>/schema - fail on new schema string'''
        reg_url = '%s/register' % self.subject_url
        self.tasr_app.request(reg_url, method='PUT',
                              content_type=self.content_type,
                              body=self.schema_str)
        post_url = "%s/schema" % self.subject_url
        new_schema_str = self.schema_str.replace('tagged.events',
                                                 'tagged.events.new', 1)
        resp = self.tasr_app.request(post_url, method='POST',
                                     content_type=self.content_type,
                                     expect_errors=True,
                                     body=new_schema_str)
        self.assertEqual(404, resp.status_int,
                         u'Unexpected status: %s' % resp.status_int)

    def test_get_all_subjects(self):
        '''GET /tasr/subject - as expected'''
        # reg two vers for target subject and one for an alt subject
        reg_url = '%s/register' % self.subject_url
        self.tasr_app.request(reg_url, method='PUT',
                              content_type=self.content_type,
                              body=self.schema_str)
        schema_str_2 = self.schema_str.replace('tagged.events',
                                               'tagged.events.alt', 1)
        self.tasr_app.request(reg_url, method='PUT',
                              content_type=self.content_type,
                              body=schema_str_2)
        alt_topic = 'bob'
        alt_url = '%s/%s/register' % (self.url_prefix, alt_topic)
        self.tasr_app.request(alt_url, method='PUT',
                              content_type=self.content_type,
                              body=self.schema_str)
        # now get all with versions and check the headers
        resp = self.tasr_app.request(self.url_prefix, method='GET')
        hdict = extract_hdict(resp.headerlist, 'X-SCHEMA-')
        self.assertEqual(200, resp.status_code,
                         u'Non-200 status code: %s' % resp.status_code)
        tv_dict = {}
        for tv_hval in hdict['X-SCHEMA-TOPIC-VERSION']:
            t, v = tv_hval.split('=')
            tv_dict[t] = v
        self.assertEqual('2', tv_dict[self.event_type], 'Expected 2 versions.')
        self.assertEqual('1', tv_dict[alt_topic], 'Expected 1 version.')
        # lastly check the body
        buff = StringIO.StringIO(resp.body)
        topics = []
        for topic_line in buff:
            topics.append(topic_line.strip())
        buff.close()
        self.assertListEqual(sorted(topics), sorted(tv_dict.keys()),
                             'Expected topics in body to match headers.')

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTASRAppSVAPI)
    unittest.TextTestRunner(verbosity=2).run(suite)
