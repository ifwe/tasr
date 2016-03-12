'''
Created on March 3, 2016

@author: cmills
'''

from tasr_test import TASRTestCase

import unittest
from webtest import TestApp
import tasr.app
import json

APP = tasr.app.TASR_APP
APP.set_config_mode('local')


class TestTASRRedshift(TASRTestCase):
    '''These tests check that the TASR RedShift-specific calls.'''

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

    def enable_redshift_for_subject(self, subject_name):
        url = '%s/subject/%s/config/redshift.enabled' % (self.url_prefix,
                                                         subject_name)
        return self.tasr_app.post(url, 'true')

    def register_schema(self, subject_name, schema_str, expect_errors=False):
        reg_url = '%s/subject/%s/register' % (self.url_prefix, subject_name)
        return self.tasr_app.request(reg_url, method='PUT',
                                     content_type=self.content_type,
                                     expect_errors=expect_errors,
                                     body=schema_str)

    def test_redshift_master_schema_for_subject(self):
        '''GET /tasr/subject/<subject>/redshift/master - as expected'''
        self.register_subject(self.event_type)
        self.enable_redshift_for_subject(self.event_type)
        schemas = []
        # add a bunch of versions for our subject
        for v in range(1, 4):
            ver_schema_str = self.get_schema_permutation(self.schema_str,
                                                         "fn_%s" % v)
            resp = self.register_schema(self.event_type, ver_schema_str)
            self.abort_diff_status(resp, 201)
            # schema str with canonicalized whitespace returned
            canonicalized_schema_str = resp.body
            schemas.append(canonicalized_schema_str)

        # grab the master and check that all the expected fields are there
        m_resp = self.tasr_app.get('%s/master' % self.subject_url)
        self.abort_diff_status(m_resp, 200)
        master_json = json.loads(m_resp.body)

        # now grab the RedShift master to compare
        rs_resp = self.tasr_app.get('%s/redshift/master' % self.subject_url)
        self.abort_diff_status(rs_resp, 200)
        print rs_resp.body
        rsm_json = json.loads(rs_resp.body)

        # field order should be (mostly) the same
        master_fnames = []
        for mfield in master_json['fields']:
            master_fnames.append(mfield['name'])
        rsm_fnames = []
        for mfield in rsm_json['fields']:
            rsm_fnames.append(mfield['name'])

        prefix = '%s__' % self.event_type
        master_only = ['meta__handlers', 'meta__kvpairs', 'meta__topic_name']
        for mfn in master_fnames:
            if mfn in master_only:
                continue
            if prefix in mfn:
                rs_name = mfn[len(prefix):]
                if rs_name not in rsm_fnames:
                    self.fail('master field "%s" not in RS master' % rs_name)
            elif not mfn in rsm_fnames:
                self.fail('master field "%s" missing in RS master' % mfn)

        rs_only = ['meta__kvpairs_json', 'dt', 'redshift__event_md5_hash']
        for rsmfn in rsm_fnames:
            if rsmfn in rs_only:
                continue
            if not rsmfn in master_fnames:
                prefixed_name = '%s%s' % (prefix, rsmfn)
                if not prefixed_name in master_fnames:
                    self.fail('RS field "%s" missing in master' % rsmfn)

    def test_redshift_create_dml_for_subject(self):
        '''GET /tasr/subject/<subject>/redshift/dml_create - as expected'''
        self.register_subject(self.event_type)
        self.enable_redshift_for_subject(self.event_type)
        schemas = []
        # add a bunch of versions for our subject
        for v in range(1, 4):
            ver_schema_str = self.get_schema_permutation(self.schema_str,
                                                         "fn_%s" % v)
            resp = self.register_schema(self.event_type, ver_schema_str)
            self.abort_diff_status(resp, 201)
            # schema str with canonicalized whitespace returned
            canonicalized_schema_str = resp.body
            schemas.append(canonicalized_schema_str)

        # grab the master and check that all the expected fields are there
        m_resp = self.tasr_app.get('%s/redshift/dml_create' % self.subject_url)
        self.abort_diff_status(m_resp, 200)
        rs_dml = m_resp.body
        print rs_dml

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(TestTASRRedshift)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
