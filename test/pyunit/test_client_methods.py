'''
Created on May 7, 2014

@author: cmills
'''

from tasr.registered_schema import RegisteredAvroSchema
from client_test import TestTASRAppClient

import unittest
import httmock

from tasr.utils.client import (get_active_subject_names,
                               get_all_subject_names,
                               get_all_subject_schema_ids,
                               get_all_subject_schemas,
                               get_subject_config,
                               is_subject_integral,
                               lookup_by_id_str,
                               lookup_latest,
                               lookup_by_schema_str,
                               lookup_by_version,
                               lookup_subject,
                               register_schema,
                               register_schema_if_latest,
                               register_subject,
                               update_subject_config,
                               TASRError
                               )

class TestTASRClientMethods(TestTASRAppClient):

    def setUp(self):
        super(TestTASRClientMethods, self).setUp()
        self.event_type = "gold"
        fix_rel_path = "schemas/%s.avsc" % (self.event_type)
        self.avsc_file = self.get_fixture_file(fix_rel_path, "r")
        self.schema_str = self.avsc_file.read()
        # client settings
        self.host = self.app.config.host  # should match netloc below
        self.port = self.app.config.port  # should match netloc below
        # clear out all the keys before beginning -- careful!
        self.app.ASR.redis.flushdb()

    def tearDown(self):
        # this clears out redis after each test -- careful!
        self.app.ASR.redis.flushdb()

    def bare_register_subject_skeleton(self, config_dict=None):
        '''register_subject() - skeleton test'''
        with httmock.HTTMock(self.route_to_testapp):
            meta = register_subject(self.event_type,
                                    config_dict,
                                    self.host, self.port)
            self.assertIn(self.event_type, meta.name, 'Bad subject name.')
            return meta

    def bare_register_schema_skeleton(self, schema_str):
        '''register_schema_for_topic() - skeleton test'''
        # whitespace gets normalized, so do that locally to the submitted
        # schema string so we have an accurate target for comparison
        ras = RegisteredAvroSchema()
        ras.schema_str = schema_str
        canonical_schema_str = ras.json
        with httmock.HTTMock(self.route_to_testapp):
            rs = register_schema(self.event_type, schema_str, self.host, self.port)
            self.assertEqual(canonical_schema_str, rs.schema_str,
                             'Schema string modified!')
            self.assertIn(self.event_type, rs.group_names,
                          'Topic not in registered schema object.')
            self.assertIn(self.event_type, rs.ts_dict.keys(),
                          'Topic not in registration timestamps.')
            return rs

    ########################################################################
    # subject tests for S+V API
    ########################################################################
    def test_bare_register_subject(self):
        '''register_group() - as expected'''
        self.bare_register_subject_skeleton()

    def test_bare_lookup_subject(self):
        '''lookup_subject() - as expected, should return True'''
        self.bare_register_subject_skeleton()
        with httmock.HTTMock(self.route_to_testapp):
            self.assertTrue(lookup_subject(self.event_type,
                                           self.host,
                                           self.port))

    def test_bare_lookup_missing_subject(self):
        '''lookup_subject() - no such subject, should return False'''
        with httmock.HTTMock(self.route_to_testapp):
            self.assertFalse(lookup_subject(self.event_type,
                                            self.host,
                                            self.port))

    def test_bare_get_subject_config(self):
        '''get_subject_config() - as expected'''
        test_config = {'bob': 'alice'}
        self.bare_register_subject_skeleton(test_config)
        with httmock.HTTMock(self.route_to_testapp):
            config_dict = get_subject_config(self.event_type,
                                             self.host,
                                             self.port)
            self.assertDictEqual(test_config, config_dict, 'bad config dict')

    def test_bare_update_subject_config(self):
        '''update_subject_config() - as expected'''
        test_config = {'bob': 'alice'}
        self.bare_register_subject_skeleton(test_config)
        with httmock.HTTMock(self.route_to_testapp):
            update_config = {'bob': 'cynthia', 'doris': 'eve'}
            config_dict = update_subject_config(self.event_type,
                                                update_config,
                                                self.host,
                                                self.port)
            self.assertDictEqual(update_config, config_dict, 'bad config dict')

    def test_bare_is_subject_integral(self):
        '''is_subject_integral() - as expected'''
        self.bare_register_subject_skeleton()
        with httmock.HTTMock(self.route_to_testapp):
            is_int = is_subject_integral(self.event_type,
                                         self.host,
                                         self.port)
            self.assertFalse(is_int)

    def test_bare_get_get_active_subject_names_with_none_and_one_present(self):
        '''get_active_subject_names() - as expected'''
        self.bare_register_subject_skeleton()
        # without a schema, the subject is not active
        with httmock.HTTMock(self.route_to_testapp):
            subject_names = get_active_subject_names(self.host, self.port)
            self.assertEqual(0, len(subject_names), 'expected no subjects')

        # now reg a schema and try again
        self.bare_register_schema_skeleton(self.schema_str)
        with httmock.HTTMock(self.route_to_testapp):
            subject_names = get_active_subject_names(self.host, self.port)
            self.assertListEqual(subject_names, [self.event_type, ],
                                 'unexpected groups: %s' % subject_names)

    def test_bare_get_get_all_subject_names_with_one_present(self):
        '''get_all_subject_names() - as expected'''
        self.bare_register_subject_skeleton()
        with httmock.HTTMock(self.route_to_testapp):
            subject_names = get_all_subject_names(self.host, self.port)
            self.assertListEqual(subject_names, [self.event_type, ],
                                 'unexpected groups: %s' % subject_names)

    def test_bare_get_get_all_subject_names_with_none_present(self):
        '''get_all_subject_names() - checking an empty list doesn't blow up'''
        with httmock.HTTMock(self.route_to_testapp):
            subject_names = get_all_subject_names(self.host, self.port)
            self.assertEqual(0, len(subject_names), 'expected no subjects')

    def test_bare_get_all_subject_schema_ids(self):
        '''get_all_subject_schema_ids() - as expected'''
        with httmock.HTTMock(self.route_to_testapp):
            schemas = []
            sha256_ids = []
            for v in range(1, 50):
                ver_schema_str = self.get_schema_permutation(self.schema_str,
                                                             "fn_%s" % v)
                # whitespace gets normalized, so do that locally to the
                # submitted schema string so we have an accurate target for
                # comparison
                ras = RegisteredAvroSchema()
                ras.schema_str = ver_schema_str
                canonical_ver_schema_str = ras.json
                schemas.append(canonical_ver_schema_str)
                # reg with the non-canonicalized schema string
                rs = self.bare_register_schema_skeleton(ver_schema_str)
                self.assertEqual(canonical_ver_schema_str, rs.schema_str,
                                 'Schema string modified!')
                self.assertIn(self.event_type, rs.group_names,
                              'Subject not in registered schema object.')
                self.assertEqual(rs.sha256_id, ras.sha256_id,
                                 'SHA256 ID mismatch')
                sha256_ids.append(rs.sha256_id)

            # now pull the ID list and check it matches
            ids = get_all_subject_schema_ids(self.event_type,
                                             self.host,
                                             self.port)
            self.assertListEqual(sha256_ids, ids, 'ID list mismatch')

    def test_bare_get_all_subject_schemas(self):
        '''get_all_subject_schemas() - as expected'''
        with httmock.HTTMock(self.route_to_testapp):
            test_schema_strs = []
            for v in range(1, 50):
                ver_schema_str = self.get_schema_permutation(self.schema_str,
                                                             "fn_%s" % v)
                # whitespace gets normalized, so do that locally to the
                # submitted schema string so we have an accurate target for
                # comparison
                ras = RegisteredAvroSchema()
                ras.schema_str = ver_schema_str
                canonical_ver_schema_str = ras.json
                test_schema_strs.append(canonical_ver_schema_str)
                # reg with the non-canonicalized schema string
                rs = self.bare_register_schema_skeleton(ver_schema_str)
                self.assertEqual(canonical_ver_schema_str, rs.schema_str,
                                 'Schema string modified!')
                self.assertIn(self.event_type, rs.group_names,
                              'Subject not in registered schema object.')
                self.assertEqual(rs.sha256_id, ras.sha256_id,
                                 'SHA256 ID mismatch')

            # now pull the schema list and check it matches
            schemas = get_all_subject_schemas(self.event_type,
                                              self.host,
                                              self.port)
            for v in range(1, 50):
                reg_schema = schemas[v - 1]
                test_schema_str = test_schema_strs[v - 1]
                self.assertEqual(reg_schema.json, test_schema_str,
                                 'schema string mismatch')

    ########################################################################
    # subject schema registration tests
    ########################################################################
    def test_bare_register_schema(self):
        '''register_schema_for_topic() - as expected'''
        self.bare_register_schema_skeleton(self.schema_str)

    def test_bare_reg_fail_on_empty_schema(self):
        '''register_schema_for_topic() - fail on empty schema'''
        try:
            self.bare_register_schema_skeleton(None)
            self.fail('should have thrown a TASRError')
        except TASRError as te:
            self.assertTrue(te, 'Missing TASRError')

    def test_bare_reg_fail_on_invalid_schema(self):
        '''register_schema_for_topic() - fail on invalid schema'''
        try:
            bad_schema = '%s }' % self.schema_str
            self.bare_register_schema_skeleton(bad_schema)
            self.fail('should have thrown a ValueError')
        except TASRError as te:
            self.fail('should have thrown a ValueError')
        except ValueError:
            pass

    def test_bare_reg_and_rereg(self):
        '''register_schema_for_topic() - multi calls, same schema'''
        rs1 = self.bare_register_schema_skeleton(self.schema_str)
        rs2 = self.bare_register_schema_skeleton(self.schema_str)
        self.assertEqual(rs1, rs2, 'reg and rereg schemas unequal!')

    def test_bare_register_schema_if_latest(self):
        '''register_schema_if_latest() - as expected'''
        self.bare_register_schema_skeleton(self.schema_str)
        with httmock.HTTMock(self.route_to_testapp):
            alt_schema_str = self.get_schema_permutation(self.schema_str)
            cur_latest_ver = 1
            rs = register_schema_if_latest(self.event_type,
                                           cur_latest_ver,
                                           alt_schema_str,
                                           self.host,
                                           self.port)
            self.assertEqual(rs.current_version(self.event_type), 2, 'bad ver')

    def test_bare_fail_register_schema_if_latest_stale_version(self):
        '''register_schema_if_latest() - as expected'''
        self.bare_register_schema_skeleton(self.schema_str)
        alt_schema_str = self.get_schema_permutation(self.schema_str)
        self.bare_register_schema_skeleton(alt_schema_str)
        # so cur ver is now 2
        with httmock.HTTMock(self.route_to_testapp):
            old_ver = 1
            try:
                register_schema_if_latest(self.event_type,
                                          old_ver,
                                          self.schema_str,
                                          self.host,
                                          self.port)
                self.fail('expected a TASRError')
            except TASRError as te:
                self.assertTrue(te, 'Missing TASRError')

    def test_bare_fail_register_schema_if_latest_bad_version(self):
        '''register_schema_if_latest() - as expected'''
        self.bare_register_schema_skeleton(self.schema_str)
        # so cur ver is now 1
        with httmock.HTTMock(self.route_to_testapp):
            alt_schema_str = self.get_schema_permutation(self.schema_str)
            bad_ver = 2
            try:
                register_schema_if_latest(self.event_type,
                                          bad_ver,
                                          alt_schema_str,
                                          self.host,
                                          self.port)
                self.fail('expected a TASRError')
            except TASRError as te:
                self.assertTrue(te, 'Missing TASRError')

    ########################################################################
    # schema retrieval tests for TASR S+V API
    ########################################################################
    def test_bare_lookup_by_schema_str(self):
        '''lookup_by_schema_str() - as expected'''
        reg_rs = self.bare_register_schema_skeleton(self.schema_str)
        with httmock.HTTMock(self.route_to_testapp):
            ret_rs = lookup_by_schema_str(self.event_type, reg_rs.json,
                                          self.host, self.port)
            self.assertEqual(reg_rs.sha256_id, ret_rs.sha256_id, 'ID mismatch')

    def bare_get_for_subject_skeleton(self, subject_name, version):
        '''lookup_by_version() - util method'''
        with httmock.HTTMock(self.route_to_testapp):
            return lookup_by_version(subject_name, version,
                                     self.host, self.port)

    def test_bare_fail_lookup_by_version_bad_version(self):
        '''lookup_by_version() - bad version'''
        reg_rs = self.bare_register_schema_skeleton(self.schema_str)
        bad_ver = reg_rs.current_version(self.event_type) + 1
        try:
            self.bare_get_for_subject_skeleton(self.schema_str, bad_ver)
            self.fail('Should have thrown an TASRError')
        except TASRError as te:
            self.assertTrue(te, 'Missing TASRError')

    def test_bare_lookup_by_version(self):
        '''lookup_by_version() - multiple versions, as expected'''
        schemas = []
        for v in range(1, 50):
            ver_schema_str = self.get_schema_permutation(self.schema_str,
                                                         "f_%s" % v)
            # whitespace gets normalized, so do that locally to the submitted
            # schema string so we have an accurate target for comparison
            ras = RegisteredAvroSchema()
            ras.schema_str = ver_schema_str
            canonical_ver_schema_str = ras.json
            schemas.append(canonical_ver_schema_str)
            # reg with the non-canonicalized schema string
            rs = self.bare_register_schema_skeleton(ver_schema_str)
            self.assertEqual(canonical_ver_schema_str, rs.schema_str,
                             'Schema string modified!')
            self.assertIn(self.event_type, rs.group_names,
                          'Topic not in registered schema object.')
        # now pull them by version and check they match what we sent originally
        for v in range(1, 50):
            rs = self.bare_get_for_subject_skeleton(self.event_type, v)
            self.assertEqual(schemas[v - 1], rs.json, 'Unexpected version.')

    def test_bare_lookup_by_version_old_version(self):
        '''get_schema_for_topic() - non-sequential re-reg'''
        alt_schema_str = self.get_schema_permutation(self.schema_str)
        rs1 = self.bare_register_schema_skeleton(self.schema_str)
        self.bare_register_schema_skeleton(alt_schema_str)
        rs3 = self.bare_register_schema_skeleton(self.schema_str)
        self.assertEqual(3, rs3.current_version(self.event_type),
                         'unexpected version')
        # now get version 1 -- should be same schema, and should list
        # requested version as "current"
        rs = self.bare_get_for_subject_skeleton(self.event_type, 1)
        self.assertEqual(rs1.json, rs.json,
                         'Unexpected schema string change between v1 and v3.')
        self.assertEqual(1, rs.current_version(self.event_type),
                        'Expected different current version value.')

    def test_bare_lookup_by_sha256_id_str(self):
        '''lookup_by_id_str() - multiple versions, as expected'''
        sha256_ids = []
        schemas = []
        for v in range(1, 50):
            ver_schema_str = self.get_schema_permutation(self.schema_str,
                                                         "fn_%s" % v)
            # whitespace gets normalized, so do that locally to the submitted
            # schema string so we have an accurate target for comparison
            ras = RegisteredAvroSchema()
            ras.schema_str = ver_schema_str
            canonical_ver_schema_str = ras.json
            schemas.append(canonical_ver_schema_str)
            # reg with the non-canonicalized schema string
            rs = self.bare_register_schema_skeleton(ver_schema_str)
            self.assertEqual(canonical_ver_schema_str, rs.schema_str,
                             'Schema string modified!')
            self.assertIn(self.event_type, rs.group_names,
                          'Topic not in registered schema object.')
            self.assertEqual(ras.sha256_id, rs.sha256_id, 'ID mismatch')
            sha256_ids.append(rs.sha256_id)
        # now pull them by sha256_id and check they match
        with httmock.HTTMock(self.route_to_testapp):
            for v in range(1, 50):
                sha256_id = sha256_ids[v - 1]
                schema_str = schemas[v - 1]
                try:
                    rs = lookup_by_id_str(self.event_type,
                                          sha256_id,
                                          self.host,
                                          self.port)
                    self.assertEqual(schema_str, rs.json,
                                     'schema string mismatch')
                except TASRError as terr:
                    print terr

    def test_bare_lookup_latest(self):
        self.bare_register_schema_skeleton(self.schema_str)
        alt_schema_str = self.get_schema_permutation(self.schema_str)
        self.bare_register_schema_skeleton(alt_schema_str)
        # so cur ver is now 2
        with httmock.HTTMock(self.route_to_testapp):
            rs = lookup_latest(self.event_type,
                               self.host,
                               self.port)
            self.assertEqual(2, rs.current_version(self.event_type), 'bad ver')

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(TestTASRClientMethods)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
