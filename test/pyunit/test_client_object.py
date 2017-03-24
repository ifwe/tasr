'''
Created on May 7, 2014

@author: cmills
'''

from client_test import TestTASRAppClient

import unittest
import tasr.utils.client
import httmock


class TestTASRClientObject(TestTASRAppClient):

    def setUp(self):
        super(TestTASRClientObject, self).setUp()
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

    def obj_register_subject_skeleton(self, config_dict=None):
        '''TASRClientSV.register_subject() - skeleton test'''
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            meta = client.register_subject(self.event_type, config_dict)
            self.assertIn(self.event_type, meta.name, 'Bad subject name.')
            return meta

    def obj_register_schema_skeleton(self, schema_str):
        '''TASRClientSV.register_schema() - skeleton test'''
        # whitespace gets normalized, so do that locally to the submitted
        # schema string so we have an accurate target for comparison
        ras = tasr.registered_schema.RegisteredAvroSchema()
        ras.schema_str = schema_str
        canonical_schema_str = ras.json
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            rs = client.register_schema(self.event_type, schema_str)
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
    def test_obj_register_subject(self):
        '''TASRClientSV.register_subject()'''
        self.obj_register_subject_skeleton()

    def test_obj_lookup_subject(self):
        '''TASRClientSV.lookup_subject()'''
        self.obj_register_subject_skeleton()
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            self.assertTrue(client.lookup_subject(self.event_type))

    def test_obj_lookup_missing_subject(self):
        '''TASRClientSV.lookup_subject()'''
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            self.assertFalse(client.lookup_subject(self.event_type))

    def test_obj_subject_config(self):
        '''TASRClientSV.subject_config() - as expected'''
        test_config = {'bob': 'alice'}
        self.obj_register_subject_skeleton(test_config)
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            config_dict = client.subject_config(self.event_type)
            self.assertDictEqual(test_config, config_dict, 'bad config dict')

    def test_obj_update_subject_config(self):
        '''TASRClientSV.update_subject_config() - as expected'''
        test_config = {'bob': 'alice'}
        self.obj_register_subject_skeleton(test_config)
        with httmock.HTTMock(self.route_to_testapp):
            update_config = {'bob': 'cynthia', 'doris': 'eve'}
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            config_dict = client.update_subject_config(self.event_type,
                                                       update_config)
            self.assertDictEqual(update_config, config_dict, 'bad config dict')

    def test_obj_is_subject_integral(self):
        '''TASRClientSV.is_subject_integral() - as expected'''
        self.obj_register_subject_skeleton()
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            self.assertFalse(client.is_subject_integral(self.event_type))

    def test_obj_get_get_active_subject_names_with_none_and_one_present(self):
        '''TASRClientSV.active_subject_names() - as expected'''
        self.obj_register_subject_skeleton()
        # without a schema, the subject is not active
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            subject_names = client.active_subject_names()
            self.assertEqual(0, len(subject_names), 'expected no groups')

        # now reg a schema and try again
        self.obj_register_schema_skeleton(self.schema_str)
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            subject_names = client.active_subject_names()
            self.assertListEqual(subject_names, [self.event_type, ],
                                 'unexpected groups: %s' % subject_names)

    def test_obj_all_subject_names_with_one_present(self):
        '''TASRClientSV.all_subject_names() - as expected'''
        self.obj_register_subject_skeleton()
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            subject_names = client.all_subject_names()
            self.assertListEqual(subject_names, [self.event_type, ],
                                 'unexpected groups: %s' % subject_names)

    def test_obj_all_subject_names_with_none_present(self):
        '''TASRClientSV.all_subject_names() - check an empty list works'''
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            subject_names = client.all_subject_names()
            self.assertEqual(0, len(subject_names), 'expected no groups')

    def test_obj_all_subject_schema_ids(self):
        '''TASRClientSV.all_subject_schema_ids() - as expected'''
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            schemas = []
            sha256_ids = []
            for v in range(1, 50):
                ver_schema_str = self.get_schema_permutation(self.schema_str,
                                                             "fn_%s" % v)
                # whitespace gets normalized, so do that locally to the
                # submitted schema string so we have an accurate target for
                # comparison
                ras = tasr.registered_schema.RegisteredAvroSchema()
                ras.schema_str = ver_schema_str
                canonical_ver_schema_str = ras.json
                schemas.append(canonical_ver_schema_str)
                # reg with the non-canonicalized schema string
                rs = self.obj_register_schema_skeleton(ver_schema_str)
                self.assertEqual(canonical_ver_schema_str, rs.schema_str,
                                 'Schema string modified!')
                self.assertIn(self.event_type, rs.group_names,
                              'Subject not in registered schema object.')
                self.assertEqual(rs.sha256_id, ras.sha256_id,
                                 'SHA256 ID mismatch')
                sha256_ids.append(rs.sha256_id)

            # now pull the ID list and check it matches
            ids = client.all_subject_schema_ids(self.event_type)
            self.assertListEqual(sha256_ids, ids, 'ID list mismatch')

    def test_obj_all_subject_schemas(self):
        '''TASRClientSV.all_subject_schemas() - as expected'''
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            test_schema_strs = []
            for v in range(1, 50):
                ver_schema_str = self.get_schema_permutation(self.schema_str,
                                                             "fn_%s" % v)
                # whitespace gets normalized, so do that locally to the
                # submitted schema string so we have an accurate target for
                # comparison
                ras = tasr.registered_schema.RegisteredAvroSchema()
                ras.schema_str = ver_schema_str
                canonical_ver_schema_str = ras.json
                test_schema_strs.append(canonical_ver_schema_str)
                # reg with the non-canonicalized schema string
                rs = self.obj_register_schema_skeleton(ver_schema_str)
                self.assertEqual(canonical_ver_schema_str, rs.schema_str,
                                 'Schema string modified!')
                self.assertIn(self.event_type, rs.group_names,
                              'Subject not in registered schema object.')
                self.assertEqual(rs.sha256_id, ras.sha256_id,
                                 'SHA256 ID mismatch')

            # now pull the schema list and check it matches
            schemas = client.all_subject_schemas(self.event_type)
            for v in range(1, 50):
                reg_schema = schemas[v - 1]
                test_schema_str = test_schema_strs[v - 1]
                self.assertEqual(reg_schema.json, test_schema_str,
                                 'schema string mismatch')

    ########################################################################
    # subject schema registration tests
    ########################################################################
    def test_obj_register_schema(self):
        '''TASRClientSV.register_schema() - as expected'''
        self.obj_register_schema_skeleton(self.schema_str)

    def test_obj_reg_fail_on_empty_schema(self):
        '''TASRClientSV.register_schema() - fail on empty schema'''
        try:
            self.obj_register_schema_skeleton(None)
            self.fail('should have thrown a TASRError')
        except tasr.utils.client.TASRError as te:
            self.assertTrue(te, 'Missing TASRError')

    def test_obj_reg_fail_on_invalid_schema(self):
        '''TASRClientSV.register_schema() - fail on invalid schema'''
        try:
            bad_schema = '%s }' % self.schema_str
            self.obj_register_schema_skeleton(bad_schema)
            self.fail('should have thrown a ValueError')
        except tasr.utils.client.TASRError:
            self.fail('should have thrown a ValueError')
        except ValueError:
            pass

    def test_obj_reg_and_rereg(self):
        '''TASRClientSV.register_schema() - multi calls, same schema'''
        rs1 = self.obj_register_schema_skeleton(self.schema_str)
        rs2 = self.obj_register_schema_skeleton(self.schema_str)
        self.assertEqual(rs1, rs2, 'reg and rereg schemas unequal!')

    def test_obj_register_schema_if_latest_version(self):
        '''TASRClientSV.register_schema_if_latest_version() - as expected'''
        self.obj_register_schema_skeleton(self.schema_str)
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            alt_schema_str = self.get_schema_permutation(self.schema_str)
            cur_latest_ver = 1
            rs = client.register_schema_if_latest_version(self.event_type,
                                                          cur_latest_ver,
                                                          alt_schema_str)
            self.assertEqual(rs.current_version(self.event_type), 2, 'bad ver')

    def test_obj_fail_register_schema_if_latest_stale_version(self):
        '''TASRClientSV.register_schema_if_latest_version() - as expected'''
        self.obj_register_schema_skeleton(self.schema_str)
        alt_schema_str = self.get_schema_permutation(self.schema_str)
        self.obj_register_schema_skeleton(alt_schema_str)
        # so cur ver is now 2
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            old_ver = 1
            try:
                client.register_schema_if_latest_version(self.event_type,
                                                         old_ver,
                                                         self.schema_str)
                self.fail('expected a TASRError')
            except tasr.utils.client.TASRError as te:
                self.assertTrue(te, 'Missing TASRError')

    def test_bare_fail_register_schema_if_latest_bad_version(self):
        '''TASRClientSV.register_schema_if_latest_version() - as expected'''
        self.obj_register_schema_skeleton(self.schema_str)
        # so cur ver is now 1
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            alt_schema_str = self.get_schema_permutation(self.schema_str)
            bad_ver = 2
            try:
                client.register_schema_if_latest_version(self.event_type,
                                                         bad_ver,
                                                         alt_schema_str)
                self.fail('expected a TASRError')
            except tasr.utils.client.TASRError as te:
                self.assertTrue(te, 'Missing TASRError')

    ########################################################################
    # schema retrieval tests for TASR S+V API
    ########################################################################
    def test_obj_lookup_by_schema_str(self):
        '''TASRClientSV.lookup_by_schema_str() - as expected'''
        reg_rs = self.obj_register_schema_skeleton(self.schema_str)
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            ret_rs = client.lookup_by_schema_str(self.event_type, reg_rs.json)
            self.assertEqual(reg_rs.sha256_id, ret_rs.sha256_id, 'ID mismatch')

    def obj_lookup_by_version_skeleton(self, topic, version):
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            return client.lookup_by_version(topic, version)

    def test_obj_fail_lookup_by_version_bad_version(self):
        '''TASRClientSV.lookup_by_version() - bad version'''
        reg_rs = self.obj_register_schema_skeleton(self.schema_str)
        bad_ver = reg_rs.current_version(self.event_type) + 1
        try:
            self.obj_lookup_by_version_skeleton(self.schema_str, bad_ver)
            self.fail('Should have thrown an TASRError')
        except tasr.utils.client.TASRError as te:
            self.assertTrue(te, 'Missing TASRError')

    def test_obj_lookup_by_version(self):
        '''TASRClientSV.get_schema_for_topic() - multiple versions'''
        schemas = []
        for v in range(1, 50):
            ver_schema_str = self.get_schema_permutation(self.schema_str,
                                                         "fn_%s" % v)
            # whitespace gets normalized, so do that locally to the submitted
            # schema string so we have an accurate target for comparison
            ras = tasr.registered_schema.RegisteredAvroSchema()
            ras.schema_str = ver_schema_str
            canonical_ver_schema_str = ras.json
            schemas.append(canonical_ver_schema_str)
            # reg with the non-canonicalized schema string
            rs = self.obj_register_schema_skeleton(ver_schema_str)
            self.assertEqual(canonical_ver_schema_str, rs.schema_str,
                             'Schema string modified!')
            self.assertIn(self.event_type, rs.group_names,
                          'Topic not in registered schema object.')
        # now pull them by version and check they match what we sent originally
        for v in range(1, 50):
            rs = self.obj_lookup_by_version_skeleton(self.event_type, v)
            self.assertEqual(schemas[v - 1], rs.json, 'Unexpected version.')

    def test_obj_lookup_by_version_old_version(self):
        '''TASRClientSV.get_schema_for_topic() - non-sequential re-reg'''
        alt_schema_str = self.get_schema_permutation(self.schema_str)
        rs1 = self.obj_register_schema_skeleton(self.schema_str)
        self.obj_register_schema_skeleton(alt_schema_str)
        rs3 = self.obj_register_schema_skeleton(self.schema_str)
        self.assertEqual(3, rs3.current_version(self.event_type),
                         'unexpected version')
        # now get version 1 -- should be same schema, and should list
        # requested version as "current"
        rs = self.obj_lookup_by_version_skeleton(self.event_type, 1)
        self.assertEqual(rs1.json, rs.json,
                         'Unexpected schema string change between v1 and v3.')
        self.assertEqual(1, rs.current_version(self.event_type),
                        'Expected different current version value.')

    def test_obj_lookup_by_sha256_id_str(self):
        '''TASRClientSV.lookup_by_id_str() - multiple versions, as expected'''
        sha256_ids = []
        schemas = []
        for v in range(1, 50):
            ver_schema_str = self.get_schema_permutation(self.schema_str,
                                                         "fn_%s" % v)
            # whitespace gets normalized, so do that locally to the submitted
            # schema string so we have an accurate target for comparison
            ras = tasr.registered_schema.RegisteredAvroSchema()
            ras.schema_str = ver_schema_str
            canonical_ver_schema_str = ras.json
            schemas.append(canonical_ver_schema_str)
            # reg with the non-canonicalized schema string
            rs = self.obj_register_schema_skeleton(ver_schema_str)
            self.assertEqual(canonical_ver_schema_str, rs.schema_str,
                             'Schema string modified!')
            self.assertIn(self.event_type, rs.group_names,
                          'Topic not in registered schema object.')
            self.assertEqual(ras.sha256_id, rs.sha256_id, 'ID mismatch')
            sha256_ids.append(rs.sha256_id)
        # now pull them by sha256_id and check they match
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            for v in range(1, 50):
                sha256_id = sha256_ids[v - 1]
                schema_str = schemas[v - 1]
                rs = client.lookup_by_id_str(self.event_type, sha256_id)
                self.assertEqual(schema_str, rs.json, 'schema string mismatch')

    def test_bare_lookup_latest(self):
        self.obj_register_schema_skeleton(self.schema_str)
        alt_schema_str = self.get_schema_permutation(self.schema_str)
        self.obj_register_schema_skeleton(alt_schema_str)
        # so cur ver is now 2
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.utils.client.TASRClientSV(self.host, self.port)
            rs = client.lookup_latest(self.event_type)
            self.assertEqual(2, rs.current_version(self.event_type), 'bad ver')


if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(TestTASRClientObject)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
