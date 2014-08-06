'''
Created on July 1, 2014

@author: cmills
'''

from tasr_test import TASRTestCase
from tasr.headers import SchemaHeaderBot, SubjectHeaderBot

import unittest
from webtest import TestApp
import tasr.app
import StringIO


class TestTASRAppSVAPI(TASRTestCase):
    '''These tests check that the TASR S+V REST API, expected by the Avro-1124
    repo code.  This does not check the TASR native API calls.
    '''

    def setUp(self):
        self.event_type = "gold"
        fix_rel_path = "schemas/%s.avsc" % (self.event_type)
        self.avsc_file = TASRTestCase.get_fixture_file(fix_rel_path, "r")
        self.schema_str = self.avsc_file.read()
        self.tasr_app = TestApp(tasr.app.TASR_APP)
        self.url_prefix = 'http://localhost:8080/tasr/subject'
        self.subject_url = '%s/%s' % (self.url_prefix, self.event_type)
        self.content_type = 'application/json; charset=utf8'
        # clear out all the keys before beginning -- careful!
        tasr.app.ASR.redis.flushdb()

    def tearDown(self):
        # this clears out redis after each test -- careful!
        tasr.app.ASR.redis.flushdb()

    def abort_diff_status(self, resp, code):
        self.assertEqual(code, resp.status_code,
                         u'Non-%s status code: %s' % (code, resp.status_code))

    def register_subject(self, subject_name):
        url = '%s/%s' % (self.url_prefix, subject_name)
        return self.tasr_app.put(url, {'subject_name': subject_name})

    def register_schema(self, subject_name, schema_str, expect_errors=False):
        reg_url = '%s/%s/register' % (self.url_prefix, subject_name)
        return self.tasr_app.request(reg_url, method='PUT',
                                     content_type=self.content_type,
                                     expect_errors=expect_errors,
                                     body=schema_str)

    # subject tests
    def test_register_subject(self):
        '''PUT /tasr/subject - registers the subject (not the schema)'''
        resp = self.register_subject(self.event_type)
        self.abort_diff_status(resp, 200)
        metas = SubjectHeaderBot.extract_metadata(resp)
        self.assertEqual(self.event_type, metas[self.event_type].name,
                         'unexpected subject name')

    def test_register_subject_with_no_config(self):
        '''PUT /tasr/subject/<subject> - missing body should be OK'''
        resp = self.tasr_app.request(self.subject_url, method='PUT',
                                     body=None,
                                     expect_errors=False)
        self.abort_diff_status(resp, 200)

    def test_register_subject_with_config_with_empty_field_val(self):
        '''PUT /tasr/subject - empty subject name should be OK'''
        resp = self.tasr_app.put(self.subject_url,
                                 {'subject_name': ''},
                                 expect_errors=False)
        self.abort_diff_status(resp, 200)

    def test_register_subject_with_config_with_colliding_fields(self):
        '''PUT /tasr/subject - empty subject name should return a 400'''
        resp = self.tasr_app.put(self.subject_url,
                                 {'subject_name': ['alice', 'bob']},
                                 expect_errors=True)
        self.abort_diff_status(resp, 400)

    def test_lookup_subject(self):
        '''GET /tasr/subject/<subject> - lookup the subject by name'''
        self.register_subject(self.event_type)
        resp = self.tasr_app.request(self.subject_url, method='GET')
        self.abort_diff_status(resp, 200)
        metas = SubjectHeaderBot.extract_metadata(resp)
        self.assertEqual(self.event_type, metas[self.event_type].name,
                         'unexpected subject name')

    def test_lookup_missing_subject(self):
        '''GET /tasr/subject/<subject> - lookup the subject by name'''
        missing_subject_name = 'bob'
        url = '%s/%s' % (self.url_prefix, missing_subject_name)
        resp = self.tasr_app.request(url, method='GET', expect_errors=True)
        self.abort_diff_status(resp, 404)

    def test_get_all_subjects(self):
        '''GET /tasr/subject - gets _all_ current subjects, as expected'''
        # reg two vers for target subject and one for an alt subject
        self.register_subject(self.event_type)
        alt_subject_name = 'bob'
        self.register_subject(alt_subject_name)
        # now get all and check the headers
        resp = self.tasr_app.request(self.url_prefix, method='GET')
        self.abort_diff_status(resp, 200)
        meta_dict = SubjectHeaderBot.extract_metadata(resp)

        self.assertIn(self.event_type, meta_dict.keys(), 'missing subject')
        subj = meta_dict[self.event_type]
        self.assertEqual(self.event_type, subj.name, 'bad subject name')

        self.assertIn(alt_subject_name, meta_dict.keys(), 'missing subject')
        alt_subj = meta_dict[alt_subject_name]
        self.assertEqual(alt_subject_name, alt_subj.name, 'bad subject name')

        # lastly check the body
        buff = StringIO.StringIO(resp.body)
        group_names = []
        for topic_line in buff:
            group_names.append(topic_line.strip())
        buff.close()
        self.assertListEqual(sorted(group_names), sorted(meta_dict.keys()),
                             'Expected group_names in body to match headers.')

    # schema tests
    def test_lookup(self):
        '''GET  '''
        # should be nothing there to start with
        resp = self.tasr_app.request('%s/latest' % self.subject_url,
                                     method='GET', expect_errors=True)
        self.abort_diff_status(resp, 404)

        # reg a schema so we'll have something to lookup
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 200)

        # check that lookup gets the schema
        resp = self.tasr_app.get('%s/latest' % self.subject_url)
        self.abort_diff_status(resp, 200)
        meta = SchemaHeaderBot.extract_metadata(resp)
        self.assertEqual(1, meta.group_version(self.event_type), 'bad ver')

    def test_register_schema(self):
        '''PUT /tasr/subject/<subject>/register - as expected'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 200)

    def test_reg_fail_on_empty_schema(self):
        '''PUT /tasr/subject/<subject>/register - empty schema'''
        resp = self.register_schema(self.event_type, None, True)
        self.abort_diff_status(resp, 400)

    def test_reg_fail_on_invalid_schema(self):
        '''PUT /tasr/subject/<subject>/register - bad schema'''
        bad_schema_str = "%s }" % self.schema_str
        resp = self.register_schema(self.event_type, bad_schema_str, True)
        self.abort_diff_status(resp, 400)

    def test_reg_fail_on_bad_content_type(self):
        '''PUT /tasr/subject/<subject>/register - bad content type'''
        reg_url = '%s/register' % self.subject_url
        resp = self.tasr_app.request(reg_url, method='PUT',
                                     content_type='text/plain; charset=utf8',
                                     expect_errors=True,
                                     body=self.schema_str)
        self.abort_diff_status(resp, 406)

    def test_reg_and_rereg(self):
        '''PUT /tasr/subject/<subject>/register - multiple calls, one schema'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 200)
        meta = SchemaHeaderBot.extract_metadata(resp)
        self.assertEqual(1, meta.group_version(self.event_type), 'bad ver')
        resp1 = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp1, 200)
        meta1 = SchemaHeaderBot.extract_metadata(resp1)
        self.assertEqual(1, meta1.group_version(self.event_type), 'bad ver')

    def test_multi_subject_reg(self):
        '''PUT /tasr/subject/<subject>/register - multi subjects, one schema'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 200)
        meta = SchemaHeaderBot.extract_metadata(resp)
        self.assertEqual(1, meta.group_version(self.event_type), 'bad ver')

        alt_subject = 'bob'
        resp2 = self.register_schema(alt_subject, self.schema_str)
        self.abort_diff_status(resp2, 200)
        meta2 = SchemaHeaderBot.extract_metadata(resp2)
        self.assertEqual(1, meta2.group_version(alt_subject), 'bad ver')

        # check that first association still holds
        resp3 = self.tasr_app.get('%s/latest' % self.subject_url)
        meta3 = SchemaHeaderBot.extract_metadata(resp3)
        self.assertEqual(1, meta3.group_version(self.event_type), 'lost reg')

    # retrieval
    def test_get_latest(self):
        '''GET /tasr/subject/<subject>/latest - as expected'''
        # reg a schema so we'll have something to lookup
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 200)
        schema_str_2 = self.schema_str.replace('tagged.events',
                                               'tagged.events.alt', 1)
        resp = self.register_schema(self.event_type, schema_str_2)
        self.abort_diff_status(resp, 200)

        # check that lookup gets the _latest_ schema
        resp = self.tasr_app.get('%s/latest' % self.subject_url)
        self.abort_diff_status(resp, 200)
        meta = SchemaHeaderBot.extract_metadata(resp)
        self.assertEqual(2, meta.group_version(self.event_type), 'bad ver')

    def test_reg_50_and_get_by_version(self):
        '''GET /tasr/subject/<subject>/id/<version> - as expected'''
        schemas = []
        # add a bunch of versions for our subject
        for v in range(1, 50):
            ver_schema_str = self.schema_str.replace('tagged.events',
                                                     'tagged.events.%s' % v, 1)
            resp = self.register_schema(self.event_type, ver_schema_str)
            self.abort_diff_status(resp, 200)
            # schema str with canonicalized whitespace returned
            canonicalized_schema_str = resp.body
            schemas.append(canonicalized_schema_str)

        # step through and request each version by version number
        for v in range(1, 50):
            get_url = '%s/id/%s' % (self.subject_url, v)
            get_resp = self.tasr_app.request(get_url, method='GET')
            self.abort_diff_status(get_resp, 200)
            meta = SchemaHeaderBot.extract_metadata(get_resp)
            self.assertEqual(v, meta.group_version(self.event_type), 'bad ver')
            self.assertEqual(schemas[v - 1], get_resp.body,
                             u'Unexpected body: %s' % get_resp.body)

    def test_get_for_subject_and_version_fail_on_bad_version(self):
        '''GET /tasr/subject/<subject>/id/<version> - fail on bad version'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 200)
        meta = SchemaHeaderBot.extract_metadata(resp)
        bad_version = meta.group_version(self.event_type) + 1
        get_url = '%s/id/%s' % (self.subject_url, bad_version)
        get_resp = self.tasr_app.request(get_url, method='GET',
                                         expect_errors=True)
        self.abort_diff_status(get_resp, 404)

    def test_get_for_stale_version(self):
        '''GET /tasr/subject/<subject>/id/<version> - 1 schema as 2 versions'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 200)
        schema_str_2 = self.schema_str.replace('tagged.events',
                                               'tagged.events.alt', 1)
        resp = self.register_schema(self.event_type, schema_str_2)
        self.abort_diff_status(resp, 200)
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 200)
        # get the latest version, which should be 3
        resp = self.tasr_app.get('%s/latest' % self.subject_url)
        self.abort_diff_status(resp, 200)
        meta_v3 = SchemaHeaderBot.extract_metadata(resp)
        self.assertEqual(3, meta_v3.group_version(self.event_type), 'bad ver')
        # now get ver 1, which should have the same body as ver 3
        get_url = '%s/id/%s' % (self.subject_url, 1)
        get_resp = self.tasr_app.request(get_url, method='GET')
        self.abort_diff_status(get_resp, 200)
        meta_v1 = SchemaHeaderBot.extract_metadata(get_resp)
        self.assertEqual(1, meta_v1.group_version(self.event_type), 'bad ver')
        self.assertEqual(resp.body, get_resp.body, 'schema body mismatch')

    def test_get_for_schema(self):
        '''POST /tasr/subject/<subject>/schema - as expected'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 200)
        # canonicalized schema string is passed back on registration
        canonicalized_schema_str = resp.body
        meta_1 = SchemaHeaderBot.extract_metadata(resp)
        self.assertEqual(1, meta_1.group_version(self.event_type), 'bad ver')

        schema_str_2 = self.schema_str.replace('tagged.events',
                                               'tagged.events.alt', 1)
        resp = self.register_schema(self.event_type, schema_str_2)
        self.abort_diff_status(resp, 200)
        # get by POSTed schema
        post_url = "%s/schema" % self.subject_url
        post_resp = self.tasr_app.request(post_url, method='POST',
                                          content_type=self.content_type,
                                          body=self.schema_str)
        meta_2 = SchemaHeaderBot.extract_metadata(post_resp)
        self.assertEqual(1, meta_2.group_version(self.event_type), 'bad ver')
        self.assertEqual(meta_1.sha256_id, meta_2.sha256_id, 'SHA mismatch')
        self.assertEqual(meta_1.md5_id, meta_2.md5_id, 'MD5 mismatch')
        self.assertEqual(canonicalized_schema_str, post_resp.body,
                         u'Unexpected body: %s' % post_resp.body)

    def test_get_for_schema_fail_empty_schema_str(self):
        '''POST /tasr/subject/<subject>/schema - fail on empty schema string'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 200)
        post_url = "%s/schema" % self.subject_url
        resp = self.tasr_app.request(post_url, method='POST',
                                     content_type=self.content_type,
                                     expect_errors=True,
                                     body=None)
        self.abort_diff_status(resp, 400)

    def test_get_for_schema_fail_invalid_schema_str(self):
        '''POST /tasr/subject/<subject>/schema - fail on bad schema string'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 200)
        post_url = "%s/schema" % self.subject_url
        resp = self.tasr_app.request(post_url, method='POST',
                                     content_type=self.content_type,
                                     expect_errors=True,
                                     body="%s }" % self.schema_str)
        self.abort_diff_status(resp, 400)

    def test_get_for_schema_fail_unregistered_schema_str(self):
        '''POST /tasr/subject/<subject>/schema - fail on new schema string'''
        post_url = "%s/schema" % self.subject_url
        resp = self.tasr_app.request(post_url, method='POST',
                                     content_type=self.content_type,
                                     expect_errors=True,
                                     body=self.schema_str)
        self.assertEqual(404, resp.status_int,
                         u'Unexpected status: %s' % resp.status_int)
        meta = SchemaHeaderBot.extract_metadata(resp)
        self.assertTrue(meta.sha256_id, 'SHA missing')
        self.assertTrue(meta.md5_id, 'MD5 missing')


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTASRAppSVAPI)
    unittest.TextTestRunner(verbosity=2).run(suite)
