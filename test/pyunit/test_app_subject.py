'''
Created on July 1, 2014

@author: cmills
'''

from tasr_test import TASRTestCase
from tasr.headers import SchemaHeaderBot, SubjectHeaderBot

import unittest
from webtest import TestApp
import tasr.app
import json
import StringIO


APP = tasr.app.TASR_APP
APP.set_config_mode('local')


class TestTASRSubjectApp(TASRTestCase):
    '''These tests check that the TASR S+V REST API, expected by the Avro-1124
    repo code.  This does not check the TASR native API calls.
    '''

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
    # subject tests
    ###########################################################################
    def test_all_subject_names(self):
        '''GET /tasr/subject - gets _all_ current subjects, as expected'''
        # reg two vers for target subject and one for an alt subject
        self.register_subject(self.event_type)
        alt_subject_name = 'bob'
        self.register_subject(alt_subject_name)
        # now get all and check the headers
        resp = self.tasr_app.request('%s/subject' % self.url_prefix,
                                     method='GET')
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

    def test_lookup_subject(self):
        '''GET /tasr/subject/<subject> - lookup the subject by name'''
        self.register_subject(self.event_type)
        resp = self.tasr_app.request(self.subject_url, method='GET')
        self.abort_diff_status(resp, 200)
        metas = SubjectHeaderBot.extract_metadata(resp)
        self.assertEqual(self.event_type, metas[self.event_type].name,
                         'unexpected subject name')

    def test_lookup_subject__accept_json(self):
        '''GET /tasr/subject/<subject> - lookup the subject by name'''
        self.register_subject(self.event_type)
        resp = self.tasr_app.request(self.subject_url, method='GET',
                                     accept='text/json')
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

    def test_lookup_missing_subject__accept_json(self):
        '''GET /tasr/subject/<subject> - lookup the subject by name'''
        missing_subject_name = 'bob'
        url = '%s/%s' % (self.url_prefix, missing_subject_name)
        resp = self.tasr_app.request(url, method='GET',
                                     accept='text/json',
                                     expect_errors=True)
        self.abort_diff_status(resp, 404)
        # we expect a JSON error back, so check that we got it
        json_error = json.loads(resp.body)  # body is parseable JSON
        self.assertEqual(404, json_error["status_code"], "expected a 404")

    def test_register_subject(self):
        '''PUT /tasr/subject - registers the subject (not the schema)'''
        resp = self.register_subject(self.event_type)
        self.abort_diff_status(resp, 201)
        metas = SubjectHeaderBot.extract_metadata(resp)
        self.assertEqual(self.event_type, metas[self.event_type].name,
                         'unexpected subject name')

    def test_register_subject__accept_json(self):
        '''PUT /tasr/subject - registers the subject (not the schema)'''
        url = '%s/subject/%s' % (self.url_prefix, self.event_type)
        dummy_config = {'dummy_config_key': 'dummy_config_val'}
        resp = self.tasr_app.put(url, dummy_config, {'Accept': 'text/json'})
        self.abort_diff_status(resp, 201)
        metas = SubjectHeaderBot.extract_metadata(resp)
        self.assertEqual(self.event_type, metas[self.event_type].name,
                         'unexpected subject name')
        # check the returned JSON to ensure it worked
        json_sub = json.loads(resp.body)
        self.assertEqual(self.event_type, json_sub["subject_name"],
                         "bad subject name")
        self.assertEqual(dummy_config, json_sub["config"], "bad config")

    def test_register_subject_with_no_config(self):
        '''PUT /tasr/subject/<subject> - missing body should be OK'''
        resp = self.tasr_app.request(self.subject_url, method='PUT',
                                     body=None,
                                     expect_errors=False)
        self.abort_diff_status(resp, 201)

    def test_register_subject_with_config_with_empty_field_val(self):
        '''PUT /tasr/subject - empty subject name should be OK'''
        resp = self.tasr_app.put(self.subject_url,
                                 {'subject_name': ''},
                                 expect_errors=False)
        self.abort_diff_status(resp, 201)

    def test_register_subject_with_config_with_colliding_fields(self):
        '''PUT /tasr/subject - empty subject name should return a 400'''
        resp = self.tasr_app.put(self.subject_url,
                                 {'subject_name': ['alice', 'bob']},
                                 expect_errors=True)
        self.abort_diff_status(resp, 400)

    def test_rereg_subject_with_non_conflicting_config(self):
        '''PUT /tasr/subject -  configs match, so no problem'''
        resp = self.tasr_app.put(self.subject_url,
                                 {'subject_name': 'bob'},
                                 expect_errors=False)
        self.abort_diff_status(resp, 201)
        resp = self.tasr_app.put(self.subject_url,
                                 {'subject_name': 'bob'},
                                 expect_errors=False)
        self.abort_diff_status(resp, 200)

    def test_rereg_subject_with_conflicting_config(self):
        '''PUT /tasr/subject -  conflict with preexisting config should 409'''
        resp = self.tasr_app.put(self.subject_url,
                                 {'subject_name': 'bob'},
                                 expect_errors=False)
        self.abort_diff_status(resp, 201)
        resp = self.tasr_app.put(self.subject_url,
                                 {'subject_name': 'alice'},
                                 expect_errors=True)
        self.abort_diff_status(resp, 409)

    def test_reg_and_rereg_subject(self):
        '''PUT /tasr/subject - registers the subject (not the schema), then
        re-registers the same subject.  The second reg should return a 200.'''
        resp = self.register_subject(self.event_type)
        self.abort_diff_status(resp, 201)
        resp = self.register_subject(self.event_type)
        self.abort_diff_status(resp, 200)
        metas = SubjectHeaderBot.extract_metadata(resp)
        self.assertEqual(self.event_type, metas[self.event_type].name,
                         'unexpected subject name')

    def test_subject_config(self):
        '''GET /tasr/subject/<subject>/config - get the config map'''
        resp = self.tasr_app.put(self.subject_url,
                                 {'subject_name': self.event_type},
                                 expect_errors=False)
        self.abort_diff_status(resp, 201)
        url = '%s/config' % self.subject_url
        resp = self.tasr_app.request(url, method='GET')
        self.assertEqual('subject_name=%s' % self.event_type,
                         resp.body.strip(), 'Bad response body.')

    def test_update_subject_config(self):
        '''GET /tasr/subject/<subject>/config - get the config map'''
        resp = self.tasr_app.put(self.subject_url,
                                 {'subject_name': self.event_type})
        self.abort_diff_status(resp, 201)
        url = '%s/config' % self.subject_url
        resp = self.tasr_app.request(url, method='GET')
        self.abort_diff_status(resp, 200)
        self.assertEqual('subject_name=%s' % self.event_type,
                         resp.body.strip(), 'Bad response body.')
        resp = self.tasr_app.post(url, {'subject_name': 'alice'})
        self.abort_diff_status(resp, 200)
        resp = self.tasr_app.request(url, method='GET')
        self.abort_diff_status(resp, 200)
        self.assertEqual('subject_name=alice', resp.body.strip(),
                         'Bad response body.')

    def test_subject_integral(self):
        '''GET /tasr/subject/<subject_name>/integral - should always return
        False as our IDs are not integers (though our version numbers are).
        '''
        self.register_subject(self.event_type)
        url = '%s/integral' % self.subject_url
        resp = self.tasr_app.request(url, method='GET')
        self.abort_diff_status(resp, 200)
        self.assertEqual(u'False', resp.body.strip(), 'Bad response body.')

    def test_missing_subject_integral(self):
        '''GET /tasr/subject/<subject_name>/integral - for a bad subject'''
        url = '%s/integral' % self.subject_url
        resp = self.tasr_app.request(url, method='GET', expect_errors=True)
        self.abort_diff_status(resp, 404)

    def test_all_subject_ids(self):
        '''GET /tasr/subject/<subject>/all_ids - gets schema IDs for all
        versions of the subject, in order, one per line in the response body.
        '''
        sha256_ids = []
        # add a bunch of versions for our subject
        for v in range(1, 50):
            ver_schema_str = self.get_schema_permutation(self.schema_str,
                                                         "fn_%s" % v)
            resp = self.register_schema(self.event_type, ver_schema_str)
            self.abort_diff_status(resp, 201)
            meta = SchemaHeaderBot.extract_metadata(resp)
            sha256_ids.append(meta.sha256_id)

        url = '%s/all_ids' % self.subject_url
        resp = self.tasr_app.get(url)
        buff = StringIO.StringIO(resp.body)
        all_ids = []
        for topic_line in buff:
            all_ids.append(topic_line.strip())
        buff.close()
        self.assertListEqual(sha256_ids, all_ids, 'Bad ID list.')

    def test_all_subject_schemas(self):
        '''GET /tasr/subject/<subject>/all_schemas - gets schemas for all
        versions of the subject, in order, one per line in the response body.
        '''
        versions = []
        # add a bunch of versions for our subject
        for v in range(1, 50):
            ver_schema_str = self.get_schema_permutation(self.schema_str,
                                                         "fn_%s" % v)
            resp = self.register_schema(self.event_type, ver_schema_str)
            self.abort_diff_status(resp, 201)
            versions.append(resp.body.strip())

        url = '%s/all_schemas' % self.subject_url
        resp = self.tasr_app.get(url)
        buff = StringIO.StringIO(resp.body)
        all_vers = []
        for topic_line in buff:
            all_vers.append(topic_line.strip())
        buff.close()
        self.assertListEqual(versions, all_vers, 'Bad versions list.')

    ###########################################################################
    # schema tests
    ###########################################################################
    def test_register_schema(self):
        '''PUT /tasr/subject/<subject>/register - as expected'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)

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
        self.abort_diff_status(resp, 201)
        meta = SchemaHeaderBot.extract_metadata(resp)
        self.assertEqual(1, meta.group_version(self.event_type), 'bad ver')
        resp1 = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp1, 200)
        meta1 = SchemaHeaderBot.extract_metadata(resp1)
        self.assertEqual(1, meta1.group_version(self.event_type), 'bad ver')

    def test_multi_subject_reg(self):
        '''PUT /tasr/subject/<subject>/register - multi subjects, one schema'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        meta = SchemaHeaderBot.extract_metadata(resp)
        self.assertEqual(1, meta.group_version(self.event_type), 'bad ver')

        alt_subject = 'bob'
        resp2 = self.register_schema(alt_subject, self.schema_str)
        self.abort_diff_status(resp2, 201)
        meta2 = SchemaHeaderBot.extract_metadata(resp2)
        self.assertEqual(1, meta2.group_version(alt_subject), 'bad ver')

        # check that first association still holds
        resp3 = self.tasr_app.get('%s/latest' % self.subject_url)
        meta3 = SchemaHeaderBot.extract_metadata(resp3)
        self.assertEqual(1, meta3.group_version(self.event_type), 'lost reg')

    def test_reg_if_latest(self):
        '''PUT /tasr/subject/<subject name>/register_if_latest/<version>
        As expected, we reference the version number of the latest version.
        '''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        meta = SchemaHeaderBot.extract_metadata(resp)
        cur_ver = meta.group_version(self.event_type)
        schema_str_2 = self.get_schema_permutation(self.schema_str)
        url = '%s/register_if_latest/%s' % (self.subject_url, cur_ver)
        resp = self.tasr_app.request(url, method='PUT',
                                     content_type=self.content_type,
                                     body=schema_str_2)
        self.abort_diff_status(resp, 201)

    def test_fail_reg_if_latest_bad_ver(self):
        '''PUT /tasr/subject/<subject name>/register_if_latest/<version>
        Should fail as version number is non-existent.
        '''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        meta = SchemaHeaderBot.extract_metadata(resp)
        bad_ver = meta.group_version(self.event_type) + 1
        schema_str_2 = self.get_schema_permutation(self.schema_str)
        url = '%s/register_if_latest/%s' % (self.subject_url, bad_ver)
        resp = self.tasr_app.request(url, method='PUT',
                                     content_type=self.content_type,
                                     expect_errors=True,
                                     body=schema_str_2)
        self.abort_diff_status(resp, 409)

    def test_fail_reg_if_latest_old_ver(self):
        '''PUT /tasr/subject/<subject name>/register_if_latest/<version>
        Should fail as version number exists but is not the latest.
        '''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        schema_str_2 = self.get_schema_permutation(self.schema_str)
        resp = self.register_schema(self.event_type, schema_str_2)
        self.abort_diff_status(resp, 201)
        meta = SchemaHeaderBot.extract_metadata(resp)
        old_ver = meta.group_version(self.event_type) - 1
        url = '%s/register_if_latest/%s' % (self.subject_url, old_ver)
        resp = self.tasr_app.request(url, method='PUT',
                                     content_type=self.content_type,
                                     expect_errors=True,
                                     body=self.schema_str)
        self.abort_diff_status(resp, 409)

    # retrieval
    def test_lookup_by_schema_str(self):
        '''POST /tasr/subject/<subject>/schema - as expected'''
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

    def test_fail_lookup_by_schema_str_on_empty_schema_str(self):
        '''POST /tasr/subject/<subject>/schema - fail on empty schema string'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        post_url = "%s/schema" % self.subject_url
        resp = self.tasr_app.request(post_url, method='POST',
                                     content_type=self.content_type,
                                     expect_errors=True,
                                     body=None)
        self.abort_diff_status(resp, 400)

    def test_fail_lookup_by_schema_str_on_invalid_schema_str(self):
        '''POST /tasr/subject/<subject>/schema - fail on bad schema string'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        post_url = "%s/schema" % self.subject_url
        resp = self.tasr_app.request(post_url, method='POST',
                                     content_type=self.content_type,
                                     expect_errors=True,
                                     body="%s }" % self.schema_str)
        self.abort_diff_status(resp, 400)

    def test_fail_lookup_by_schema_str_on_unregistered_schema_str(self):
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

    def test_lookup_by_subject_and_version(self):
        '''GET /tasr/subject/<subject>/version/<version> - as expected'''
        schemas = []
        # add a bunch of versions for our subject
        for v in range(1, 50):
            ver_schema_str = self.get_schema_permutation(self.schema_str,
                                                         "fn_%s" % v)
            resp = self.register_schema(self.event_type, ver_schema_str)
            self.abort_diff_status(resp, 201)
            # schema str with canonicalized whitespace returned
            canonicalized_schema_str = resp.body
            schemas.append(canonicalized_schema_str)

        # step through and request each version by version number
        for v in range(1, 50):
            get_url = '%s/version/%s' % (self.subject_url, v)
            get_resp = self.tasr_app.request(get_url, method='GET')
            self.abort_diff_status(get_resp, 200)
            meta = SchemaHeaderBot.extract_metadata(get_resp)
            self.assertEqual(v, meta.group_version(self.event_type), 'bad ver')
            self.assertEqual(schemas[v - 1], get_resp.body,
                             u'Unexpected body: %s' % get_resp.body)

    def test_fail_lookup_for_subject_and_version_on_bad_version(self):
        '''GET /tasr/subject/<subject>/version/<version> - fail on bad ver'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        meta = SchemaHeaderBot.extract_metadata(resp)
        bad_version = meta.group_version(self.event_type) + 1
        get_url = '%s/version/%s' % (self.subject_url, bad_version)
        get_resp = self.tasr_app.request(get_url, method='GET',
                                         expect_errors=True)
        self.abort_diff_status(get_resp, 404)

    def test_lookup_for_subject_and_version_on_stale_version(self):
        '''GET /tasr/subject/<subject>/version/<version> - 1 schema, 2 vers'''
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        schema_str_2 = self.get_schema_permutation(self.schema_str)
        resp = self.register_schema(self.event_type, schema_str_2)
        self.abort_diff_status(resp, 201)
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        # get the latest version, which should be 3
        resp = self.tasr_app.get('%s/latest' % self.subject_url)
        self.abort_diff_status(resp, 200)
        meta_v3 = SchemaHeaderBot.extract_metadata(resp)
        self.assertEqual(3, meta_v3.group_version(self.event_type), 'bad ver')
        # now get ver 1, which should have the same body as ver 3
        get_url = '%s/version/%s' % (self.subject_url, 1)
        get_resp = self.tasr_app.request(get_url, method='GET')
        self.abort_diff_status(get_resp, 200)
        meta_v1 = SchemaHeaderBot.extract_metadata(get_resp)
        self.assertEqual(1, meta_v1.group_version(self.event_type), 'bad ver')
        self.assertEqual(resp.body, get_resp.body, 'schema body mismatch')

    def test_lookup_by_subject_and_sha256_id_str(self):
        '''GET /tasr/subject/<subject>/id/<id_str> - as expected'''
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
            get_url = '%s/id/%s' % (self.subject_url, sha256_ids[v - 1])
            get_resp = self.tasr_app.request(get_url, method='GET')
            self.abort_diff_status(get_resp, 200)
            meta = SchemaHeaderBot.extract_metadata(get_resp)
            self.assertEqual(sha256_ids[v - 1], meta.sha256_id, 'bad ID')
            self.assertEqual(schemas[v - 1], get_resp.body,
                             u'Unexpected body: %s' % get_resp.body)

    def test_lookup_by_subject_and_md5_id_str(self):
        '''GET /tasr/subject/<subject>/id/<id_str> - as expected'''
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
            get_url = '%s/id/%s' % (self.subject_url, md5_ids[v - 1])
            get_resp = self.tasr_app.request(get_url, method='GET')
            self.abort_diff_status(get_resp, 200)
            meta = SchemaHeaderBot.extract_metadata(get_resp)
            self.assertEqual(md5_ids[v - 1], meta.md5_id, 'bad ID')
            self.assertEqual(schemas[v - 1], get_resp.body,
                             u'Unexpected body: %s' % get_resp.body)

    def test_lookup_latest(self):
        '''GET  /tasr/subject/<subject name>/latest'''
        # should be nothing there to start with
        resp = self.tasr_app.request('%s/latest' % self.subject_url,
                                     method='GET', expect_errors=True)
        self.abort_diff_status(resp, 404)

        # reg a schema so we'll have something to lookup
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)

        # reg a second schema so we could get a stale version
        schema_str_2 = self.get_schema_permutation(self.schema_str)
        resp = self.register_schema(self.event_type, schema_str_2)
        self.abort_diff_status(resp, 201)

        # check that lookup gets the _latest_ schema
        resp = self.tasr_app.get('%s/latest' % self.subject_url)
        self.abort_diff_status(resp, 200)
        meta = SchemaHeaderBot.extract_metadata(resp)
        self.assertEqual(2, meta.group_version(self.event_type), 'bad ver')

    def test_master_schema_for_subject(self):
        '''GET /tasr/subject/<subject>/master - as expected'''
        schemas = []
        # add a bunch of versions for our subject
        for v in range(1, 10):
            ver_schema_str = self.get_schema_permutation(self.schema_str,
                                                         "fn_%s" % v)
            resp = self.register_schema(self.event_type, ver_schema_str)
            self.abort_diff_status(resp, 201)
            # schema str with canonicalized whitespace returned
            canonicalized_schema_str = resp.body
            schemas.append(canonicalized_schema_str)

        # grab the master and check that all the expected fields are there
        resp = self.tasr_app.get('%s/master' % self.subject_url)
        self.abort_diff_status(resp, 200)
        master_fnames = []
        for mfield in json.loads(resp.body)['fields']:
            master_fnames.append(mfield['name'])
        # check the original fields
        for ofield in json.loads(self.schema_str)['fields']:
            if not ofield['name'] in master_fnames:
                self.fail('missing original field %s' % ofield['name'])
        # now check all of the extra fields from the version permutations
        for v in range(1, 10):
            fname = "fn_%s" % v
            if not fname in master_fnames:
                self.fail('missing field %s' % fname)


if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(TestTASRSubjectApp)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
