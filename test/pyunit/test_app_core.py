'''
Created on December 3, 2014

@author: cmills
'''

from tasr_test import TASRTestCase
from tasr.headers import SchemaHeaderBot, SubjectHeaderBot

import unittest
from webtest import TestApp
import tasr.app
import StringIO
import json
import tasr.registered_schema

APP = tasr.app.TASR_APP
APP.set_config_mode('local')


class TestTASRCoreApp(TASRTestCase):
    '''These tests check that the TASR core app endpoints work.'''

    def setUp(self):
        self.event_type = "gold"
        fix_rel_path = "schemas/%s.avsc" % (self.event_type)
        self.avsc_file = TASRTestCase.get_fixture_file(fix_rel_path, "r")
        self.schema_str = self.avsc_file.read()
        self.tasr_app = TestApp(APP)
        self.url_prefix = 'http://%s:%s/tasr' % (APP.config.host,
                                                 APP.config.port)
        self.subject_url = '%s/subject/%s' % (self.url_prefix, self.event_type)
        self.content_type = 'application/json; charset=utf8'
        # clear out all the keys before beginning -- careful!
        APP.ASR.redis.flushdb()

    def tearDown(self):
        # this clears out redis after each test -- careful!
        APP.ASR.redis.flushdb()

    def abort_diff_status(self, resp, code):
        self.assertEqual(code, resp.status_code,
                         u'Non-%s status code: %s' % (code, resp.status_code))

    def register_subject(self, subject_name):
        url = '%s/subject/%s' % (self.url_prefix, subject_name)
        return self.tasr_app.put(url, {'subject_name': subject_name})

    def register_schema(self, subject_name, schema_str, expect_errors=False):
        reg_url = '%s/subject/%s/register' % (self.url_prefix, subject_name)
        return self.tasr_app.request(reg_url, method='PUT',
                                     content_type=self.content_type,
                                     expect_errors=expect_errors,
                                     body=schema_str)

    ###########################################################################
    # /id app
    ###########################################################################
    def test_lookup_by_md5_id(self):
        '''GET /tasr/id/<MD5 ID> - as expected'''
        put_resp = self.register_schema(self.event_type, self.schema_str)
        # the canonicalized form returned has normalized whitespace
        canonicalized_schema_str = put_resp.body
        smeta = SchemaHeaderBot.extract_metadata(put_resp)
        self.assertEqual(1, smeta.group_version(self.event_type), 'bad ver')
        url = "%s/id/%s" % (self.url_prefix, smeta.md5_id)
        get_resp = self.tasr_app.request(url, method='GET')
        self.abort_diff_status(get_resp, 200)
        self.assertEqual(canonicalized_schema_str, get_resp.body,
                         u'Unexpected body: %s' % get_resp.body)

    def test_lookup_by_md5_id__accept_json(self):
        '''GET /tasr/id/<MD5 ID> - "Accept: text/json" as expected'''
        put_resp = self.register_schema(self.event_type, self.schema_str)
        # the canonicalized form returned has normalized whitespace
        canonicalized_schema_str = put_resp.body
        smeta = SchemaHeaderBot.extract_metadata(put_resp)
        self.assertEqual(1, smeta.group_version(self.event_type), 'bad ver')
        url = "%s/id/%s" % (self.url_prefix, smeta.md5_id)
        get_resp = self.tasr_app.request(url, method='GET', accept='text/json')
        self.abort_diff_status(get_resp, 200)
        self.assertEqual(canonicalized_schema_str, get_resp.body,
                         u'Unexpected body: %s' % get_resp.body)

    def test_lookup_by_sha256_id(self):
        '''GET /tasr/id/<SHA256 ID> - as expected'''
        put_resp = self.register_schema(self.event_type, self.schema_str)
        # the canonicalized form returned has normalized whitespace
        canonicalized_schema_str = put_resp.body
        smeta = SchemaHeaderBot.extract_metadata(put_resp)
        self.assertEqual(1, smeta.group_version(self.event_type), 'bad ver')
        url = "%s/id/%s" % (self.url_prefix, smeta.sha256_id)
        get_resp = self.tasr_app.request(url, method='GET')
        self.abort_diff_status(get_resp, 200)
        self.assertEqual(canonicalized_schema_str, get_resp.body,
                         u'Unexpected body: %s' % get_resp.body)

    def test_lookup_by_sha256_id__accept_json(self):
        '''GET /tasr/id/<SHA256 ID> - "Accept: text/json" as expected'''
        put_resp = self.register_schema(self.event_type, self.schema_str)
        # the canonicalized form returned has normalized whitespace
        canonicalized_schema_str = put_resp.body
        smeta = SchemaHeaderBot.extract_metadata(put_resp)
        self.assertEqual(1, smeta.group_version(self.event_type), 'bad ver')
        url = "%s/id/%s" % (self.url_prefix, smeta.sha256_id)
        get_resp = self.tasr_app.request(url, method='GET', accept='text/json')
        self.abort_diff_status(get_resp, 200)
        self.assertEqual(canonicalized_schema_str, get_resp.body,
                         u'Unexpected body: %s' % get_resp.body)

    def test_lookup_by_sha256_id_str__bad_id(self):
        '''GET /tasr/id/<id str> - fail on bad ID'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        ver_meta = SchemaHeaderBot.extract_metadata(resp)
        sha256_id = ver_meta.sha256_id
        # get a "bad" ID from a different schema string
        rs = tasr.registered_schema.RegisteredSchema()
        rs.schema_str = self.schema_str.replace('tagged.events', 'bob')
        bad_sha256_id = rs.sha256_id
        self.assertNotEqual(sha256_id, bad_sha256_id, 'IDs should differ')
        # try getting the schema for the "bad" ID
        get_url = '%s/id/%s' % (self.url_prefix, bad_sha256_id)
        get_resp = self.tasr_app.request(get_url, method='GET',
                                         expect_errors=True)
        self.abort_diff_status(get_resp, 404)

    def test_lookup_by_sha256_id_str__accept_json__bad_id(self):
        '''GET /tasr/id/<id str> - "Accept: text/json" fail on bad ID'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        ver_meta = SchemaHeaderBot.extract_metadata(resp)
        sha256_id = ver_meta.sha256_id
        # get a "bad" ID from a different schema string
        rs = tasr.registered_schema.RegisteredSchema()
        rs.schema_str = self.schema_str.replace('tagged.events', 'bob')
        bad_sha256_id = rs.sha256_id
        self.assertNotEqual(sha256_id, bad_sha256_id, 'IDs should differ')
        # try getting the schema for the "bad" ID
        get_url = '%s/id/%s' % (self.url_prefix, bad_sha256_id)
        get_resp = self.tasr_app.request(get_url, method='GET',
                                         accept='text/json',
                                         expect_errors=True)
        self.abort_diff_status(get_resp, 404)
        # we expect a JSON error back, so check that we got it
        json_error = json.loads(get_resp.body)  # body is parseable JSON
        self.assertEqual(404, json_error["status_code"], "expected a 404")

    def test_lookup_by_sha256_id_str(self):
        '''GET /tasr/id/<id_str> - as expected'''
        sha256_ids = []
        schemas = []
        # add a bunch of versions for our subject
        for v in range(1, 50):
            ver_schema_str = self.get_schema_permutation(self.schema_str,
                                                         "fn_%s" % v)
            resp = self.register_schema(self.event_type, ver_schema_str)
            self.abort_diff_status(resp, 201)
            ver_meta = SchemaHeaderBot.extract_metadata(resp)
            sha256_ids.append(ver_meta.sha256_id)
            # schema str with canonicalized whitespace returned
            canonicalized_schema_str = resp.body
            schemas.append(canonicalized_schema_str)

        # step through and request each version by version number
        for v in range(1, 50):
            get_url = '%s/id/%s' % (self.url_prefix, sha256_ids[v - 1])
            get_resp = self.tasr_app.request(get_url, method='GET')
            self.abort_diff_status(get_resp, 200)
            meta = SchemaHeaderBot.extract_metadata(get_resp)
            self.assertEqual(sha256_ids[v - 1], meta.sha256_id, 'bad ID')
            self.assertEqual(schemas[v - 1], get_resp.body,
                             u'Unexpected body: %s' % get_resp.body)

    def test_lookup_by_md5_id_str(self):
        '''GET /tasr/id/<id_str> - as expected'''
        md5_ids = []
        schemas = []
        # add a bunch of versions for our subject
        for v in range(1, 50):
            ver_schema_str = self.get_schema_permutation(self.schema_str,
                                                         "fn_%s" % v)
            resp = self.register_schema(self.event_type, ver_schema_str)
            self.abort_diff_status(resp, 201)
            ver_meta = SchemaHeaderBot.extract_metadata(resp)
            md5_ids.append(ver_meta.md5_id)
            # schema str with canonicalized whitespace returned
            canonicalized_schema_str = resp.body
            schemas.append(canonicalized_schema_str)

        # step through and request each version by version number
        for v in range(1, 50):
            get_url = '%s/id/%s' % (self.url_prefix, md5_ids[v - 1])
            get_resp = self.tasr_app.request(get_url, method='GET')
            self.abort_diff_status(get_resp, 200)
            meta = SchemaHeaderBot.extract_metadata(get_resp)
            self.assertEqual(md5_ids[v - 1], meta.md5_id, 'bad ID')
            self.assertEqual(schemas[v - 1], get_resp.body,
                             u'Unexpected body: %s' % get_resp.body)

    ###########################################################################
    # /schema app
    ###########################################################################
    def test_lookup_by_schema_str(self):
        '''POST /tasr/schema - as expected'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        # canonicalized schema string is passed back on registration
        canonicalized_schema_str = resp.body
        meta_1 = SchemaHeaderBot.extract_metadata(resp)
        self.assertEqual(1, meta_1.group_version(self.event_type), 'bad ver')

        schema_str_2 = self.get_schema_permutation(self.schema_str)
        resp = self.register_schema(self.event_type, schema_str_2)
        self.abort_diff_status(resp, 201)
        # get by POSTed schema
        post_url = "%s/schema" % self.url_prefix
        post_resp = self.tasr_app.request(post_url, method='POST',
                                          content_type=self.content_type,
                                          body=self.schema_str)
        meta_2 = SchemaHeaderBot.extract_metadata(post_resp)
        self.assertEqual(1, meta_2.group_version(self.event_type), 'bad ver')
        self.assertEqual(meta_1.sha256_id, meta_2.sha256_id, 'SHA mismatch')
        self.assertEqual(meta_1.md5_id, meta_2.md5_id, 'MD5 mismatch')
        self.assertEqual(canonicalized_schema_str, post_resp.body,
                         u'Unexpected body: %s' % post_resp.body)

    def test_lookup_by_schema_str__accept_json(self):
        '''POST /tasr/schema - "Accept: text/json" as expected'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        # canonicalized schema string is passed back on registration
        canonicalized_schema_str = resp.body
        meta_1 = SchemaHeaderBot.extract_metadata(resp)
        self.assertEqual(1, meta_1.group_version(self.event_type), 'bad ver')

        schema_str_2 = self.get_schema_permutation(self.schema_str)
        resp = self.register_schema(self.event_type, schema_str_2)
        self.abort_diff_status(resp, 201)
        # get by POSTed schema
        post_url = "%s/schema" % self.url_prefix
        post_resp = self.tasr_app.request(post_url, method='POST',
                                          content_type=self.content_type,
                                          accept='text/json',
                                          body=self.schema_str)
        meta_2 = SchemaHeaderBot.extract_metadata(post_resp)
        self.assertEqual(1, meta_2.group_version(self.event_type), 'bad ver')
        self.assertEqual(meta_1.sha256_id, meta_2.sha256_id, 'SHA mismatch')
        self.assertEqual(meta_1.md5_id, meta_2.md5_id, 'MD5 mismatch')
        self.assertEqual(canonicalized_schema_str, post_resp.body,
                         u'Unexpected body: %s' % post_resp.body)

    def test_fail_lookup_by_schema_str_on_empty_schema_str(self):
        '''POST /tasr/schema - fail on empty schema string'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        post_url = "%s/schema" % self.url_prefix
        resp = self.tasr_app.request(post_url, method='POST',
                                     content_type=self.content_type,
                                     expect_errors=True,
                                     body=None)
        self.abort_diff_status(resp, 400)

    def test_fail_lookup_by_schema_str_on_empty_schema_str__accept_json(self):
        '''POST /tasr/schema - "Accept: text/json" fail on empty schema str'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        post_url = "%s/schema" % self.url_prefix
        resp = self.tasr_app.request(post_url, method='POST',
                                     content_type=self.content_type,
                                     accept='text/json',
                                     expect_errors=True,
                                     body=None)
        self.abort_diff_status(resp, 400)
        # we expect a JSON error back, so check that we got it
        json_error = json.loads(resp.body)  # body is parseable JSON
        self.assertEqual(400, json_error["status_code"], "expected a 404")

    def test_fail_lookup_by_schema_str_on_invalid_schema_str(self):
        '''POST /tasr/schema - fail on bad schema string'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        post_url = "%s/schema" % self.url_prefix
        resp = self.tasr_app.request(post_url, method='POST',
                                     content_type=self.content_type,
                                     expect_errors=True,
                                     body="%s }" % self.schema_str)
        self.abort_diff_status(resp, 400)

    def test_fail_lookup_by_schema_str_on_invalid_schema_str__accept_json(self):
        '''POST /tasr/schema - "Accept: text/json" fail on bad schema string'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        post_url = "%s/schema" % self.url_prefix
        resp = self.tasr_app.request(post_url, method='POST',
                                     content_type=self.content_type,
                                     accept='text/json',
                                     expect_errors=True,
                                     body="%s }" % self.schema_str)
        self.abort_diff_status(resp, 400)
        # we expect a JSON error back, so check that we got it
        json_error = json.loads(resp.body)  # body is parseable JSON
        self.assertEqual(400, json_error["status_code"], "expected a 404")

    def test_fail_lookup_by_schema_str_on_unreg_schema_str(self):
        '''POST /tasr/schema - fail on new schema string'''
        post_url = "%s/schema" % self.url_prefix
        resp = self.tasr_app.request(post_url, method='POST',
                                     content_type=self.content_type,
                                     expect_errors=True,
                                     body=self.schema_str)
        self.assertEqual(404, resp.status_int,
                         u'Unexpected status: %s' % resp.status_int)
        meta = SchemaHeaderBot.extract_metadata(resp)
        self.assertTrue(meta.sha256_id, 'SHA missing')
        self.assertTrue(meta.md5_id, 'MD5 missing')

    def test_fail_lookup_by_schema_str_on_unreg_schema_str__accept_json(self):
        '''POST /tasr/schema - fail on new schema string'''
        post_url = "%s/schema" % self.url_prefix
        resp = self.tasr_app.request(post_url, method='POST',
                                     content_type=self.content_type,
                                     accept='text/json',
                                     expect_errors=True,
                                     body=self.schema_str)
        self.assertEqual(404, resp.status_int,
                         u'Unexpected status: %s' % resp.status_int)
        meta = SchemaHeaderBot.extract_metadata(resp)
        self.assertTrue(meta.sha256_id, 'SHA missing')
        self.assertTrue(meta.md5_id, 'MD5 missing')
        # we expect a JSON error back, so check that we got it
        json_error = json.loads(resp.body)  # body is parseable JSON
        self.assertEqual(404, json_error["status_code"], "expected a 404")

    ###########################################################################
    # /collection app
    ###########################################################################
    def test_all_subject_names(self):
        '''GET /tasr/collection/subjects/all - get _all_ registered subjects'''
        # reg two vers for target subject and one for an alt subject
        self.register_subject(self.event_type)
        alt_subject_name = 'bob'
        self.register_subject(alt_subject_name)
        # now get all and check the headers
        get_url = '%s/collection/subjects/all' % self.url_prefix
        resp = self.tasr_app.request(get_url, method='GET')
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

    def test_all_subject_names__accept_json(self):
        '''GET /tasr/collection/subjects/all - get _all_ registered subjects'''
        # reg two vers for target subject and one for an alt subject
        self.register_subject(self.event_type)
        alt_subject_name = 'bob'
        self.register_subject(alt_subject_name)
        # now get all and check the headers
        get_url = '%s/collection/subjects/all' % self.url_prefix
        resp = self.tasr_app.request(get_url, method='GET', accept='text/json')
        self.abort_diff_status(resp, 200)
        meta_dict = SubjectHeaderBot.extract_metadata(resp)

        self.assertIn(self.event_type, meta_dict.keys(), 'missing subject')
        subj = meta_dict[self.event_type]
        self.assertEqual(self.event_type, subj.name, 'bad subject name')

        self.assertIn(alt_subject_name, meta_dict.keys(), 'missing subject')
        alt_subj = meta_dict[alt_subject_name]
        self.assertEqual(alt_subject_name, alt_subj.name, 'bad subject name')

        # the body should be a JSON dict of subject objects keyed by name
        sub_dict = json.loads(resp.body)
        self.assertListEqual(sorted(sub_dict.keys()), sorted(meta_dict.keys()),
                             'Expected group_names in body to match headers.')

    def test_active_subjects(self):
        '''GET /tasr/collection/subjects/active - gets _active_ subjects (that
        is, ones with at least one schema), as expected'''
        # reg two vers for target subject and one for an alt subject
        self.register_subject(self.event_type)
        alt_subject_name = 'bob'
        self.register_subject(alt_subject_name)
        # now get all and check the headers
        all_url = "%s/collection/subjects/all" % self.url_prefix
        resp = self.tasr_app.request(all_url, method='GET')
        self.abort_diff_status(resp, 200)
        meta_dict = SubjectHeaderBot.extract_metadata(resp)
        # we should have a GroupMetadata object for each group in the headers
        for sub_name in [self.event_type, alt_subject_name]:
            self.assertIn(sub_name, meta_dict.keys(), 'missing subject')
            subj = meta_dict[sub_name]
            self.assertEqual(sub_name, subj.name, 'bad subject name')

        # now get the ACTIVE subjects, which should be empty so far
        active_url = "%s/collection/subjects/active" % self.url_prefix
        resp = self.tasr_app.request(active_url, method='GET')
        self.abort_diff_status(resp, 200)
        meta_dict = SubjectHeaderBot.extract_metadata(resp)
        # we should have no GroupMetadata objects
        for sub_name in [self.event_type, alt_subject_name]:
            self.assertNotIn(sub_name, meta_dict.keys(), 'unexpected subject')

        # now register a schema for the base subject and recheck
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)

        # the get_all should be unchanged, the get_active should have one
        resp = self.tasr_app.request(all_url, method='GET')
        self.abort_diff_status(resp, 200)
        meta_dict = SubjectHeaderBot.extract_metadata(resp)
        # we should have a GroupMetadata object for each group in the headers
        for sub_name in [self.event_type, alt_subject_name]:
            self.assertIn(sub_name, meta_dict.keys(), 'missing subject')
            subj = meta_dict[sub_name]
            self.assertEqual(sub_name, subj.name, 'bad subject name')

        # now get the ACTIVE subjects, which should be empty so far
        resp = self.tasr_app.request(active_url, method='GET')
        self.abort_diff_status(resp, 200)
        meta_dict = SubjectHeaderBot.extract_metadata(resp)
        # we should have a GroupMetadata object for one group in the headers
        self.assertNotIn(alt_subject_name, meta_dict.keys(), 'unexpected obj')
        # the event_type should be there
        self.assertIn(self.event_type, meta_dict.keys(), 'missing subject')
        subj = meta_dict[self.event_type]
        self.assertEqual(self.event_type, subj.name, 'bad subject name')

        # lastly check the body
        buff = StringIO.StringIO(resp.body)
        group_names = []
        for topic_line in buff:
            group_names.append(topic_line.strip())
        buff.close()
        self.assertListEqual(sorted(group_names), sorted(meta_dict.keys()),
                             'Expected group_names in body to match headers.')

    def test_active_subjects__accept_json(self):
        '''GET /tasr/collection/subjects/active - gets _active_ subjects (that
        is, ones with at least one schema), as expected'''
        # reg two vers for target subject and one for an alt subject
        self.register_subject(self.event_type)
        alt_subject_name = 'bob'
        self.register_subject(alt_subject_name)

        # now get the ACTIVE subjects, which should be empty so far
        active_url = "%s/collection/subjects/active" % self.url_prefix
        resp = self.tasr_app.request(active_url, method='GET',
                                     accept='text/json')
        self.abort_diff_status(resp, 200)
        meta_dict = SubjectHeaderBot.extract_metadata(resp)
        # we should have no GroupMetadata objects
        for sub_name in [self.event_type, alt_subject_name]:
            self.assertNotIn(sub_name, meta_dict.keys(), 'unexpected subject')
        # we should have received an empty dict as the content body
        active_dict = json.loads(resp.body)
        self.assertDictEqual({}, active_dict, 'expected empty dict')

        # now register a schema for the base subject and recheck
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)

        # now get the ACTIVE subjects, which should be empty so far
        resp = self.tasr_app.request(active_url, method='GET',
                                     accept='text/json')
        self.abort_diff_status(resp, 200)
        meta_dict = SubjectHeaderBot.extract_metadata(resp)
        # we should have a GroupMetadata object for one group in the headers
        self.assertNotIn(alt_subject_name, meta_dict.keys(), 'unexpected obj')
        # the event_type should be there
        self.assertIn(self.event_type, meta_dict.keys(), 'missing subject')
        subj = meta_dict[self.event_type]
        self.assertEqual(self.event_type, subj.name, 'bad subject name')

        # and check the expected content body JSON
        active_dict = json.loads(resp.body)
        self.assertListEqual(sorted(active_dict.keys()),
                             sorted(meta_dict.keys()),
                             'Expected group_names in body to match headers.')

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(TestTASRCoreApp)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
