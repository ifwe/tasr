'''
Created on May 7, 2014

@author: cmills
'''

from client_test import TestTASRAppClient

import unittest
import tasr.registered_schema
import tasr.app
import tasr.client
import copy
import httmock


class TestTASRClientMethods(TestTASRAppClient):

    def setUp(self):
        super(TestTASRClientMethods, self).setUp()
        self.event_type = "gold"
        fix_rel_path = "schemas/%s.avsc" % (self.event_type)
        self.avsc_file = self.get_fixture_file(fix_rel_path, "r")
        self.schema_str = self.avsc_file.read()
        # client settings
        self.host = 'localhost'  # should match netloc below
        self.port = 8080         # should match netloc below
        # clear out all the keys before beginning -- careful!
        tasr.app.ASR.redis.flushdb()

    def tearDown(self):
        # this clears out redis after each test -- careful!
        tasr.app.ASR.redis.flushdb()

    ########################################################################
    # registration tests
    ########################################################################
    def bare_register_schema_skeleton(self, schema_str):
        '''register_schema_for_topic() - skeleton test'''
        # whitespace gets normalized, so do that locally to the submitted
        # schema string so we have an accurate target for comparison
        ras = tasr.registered_schema.RegisteredAvroSchema()
        ras.schema_str = schema_str
        canonical_schema_str = ras.canonical_schema_str
        with httmock.HTTMock(self.route_to_testapp):
            func = tasr.client.register_schema
            rs = func(self.event_type, schema_str, self.host, self.port)
            self.assertEqual(canonical_schema_str, rs.schema_str,
                             'Schema string modified!')
            self.assertIn(self.event_type, rs.group_names,
                          'Topic not in registered schema object.')
            self.assertIn(self.event_type, rs.ts_dict.keys(),
                          'Topic not in registration timestamps.')
            return rs

    def test_bare_register_schema(self):
        '''register_schema_for_topic() - as expected'''
        self.bare_register_schema_skeleton(self.schema_str)

    def test_bare_reg_fail_on_empty_schema(self):
        '''register_schema_for_topic() - fail on empty schema'''
        try:
            self.bare_register_schema_skeleton(None)
            self.fail('should have thrown a TASRError')
        except tasr.client.TASRError as te:
            self.assertTrue(te, 'Missing TASRError')

    def test_bare_reg_fail_on_invalid_schema(self):
        '''register_schema_for_topic() - fail on invalid schema'''
        try:
            bad_schema = '%s }' % self.schema_str
            self.bare_register_schema_skeleton(bad_schema)
            self.fail('should have thrown a TASRError')
        except tasr.client.TASRError as te:
            self.assertTrue(te, 'Missing TASRError')

    def test_bare_reg_and_rereg(self):
        '''register_schema_for_topic() - multi calls, same schema'''
        rs1 = self.bare_register_schema_skeleton(self.schema_str)
        rs2 = self.bare_register_schema_skeleton(self.schema_str)
        self.assertEqual(rs1, rs2, 'reg and rereg schemas unequal!')

    ########################################################################
    # topic retrieval tests for TASR API
    ########################################################################
    def test_bare_get_get_all_topics_with_none_present(self):
        '''get_all_topics()'''
        with httmock.HTTMock(self.route_to_testapp):
            group_metas = tasr.client.get_all_topics(self.host, self.port)
            self.assertEqual(0, len(group_metas), 'expected no groups')

    def test_bare_get_get_all_topics_with_one_present(self):
        '''get_all_topics()'''
        self.bare_register_schema_skeleton(self.schema_str)
        with httmock.HTTMock(self.route_to_testapp):
            group_metas = tasr.client.get_all_topics(self.host, self.port)
            self.assertListEqual(group_metas.keys(), [self.event_type, ],
                                 'unexpected groups: %s' % group_metas.keys())

    ########################################################################
    # schema retrieval tests for TASR API
    ########################################################################
    def bare_get_for_id_str_skeleton(self, id_str):
        with httmock.HTTMock(self.route_to_testapp):
            func = tasr.client.get_schema_for_id_str
            rs = func(id_str, self.host, self.port)
            self.assertIn(id_str, (rs.sha256_id, rs.md5_id), 'ID missing')
            return rs

    def bare_get_for_topic_skeleton(self, topic, version):
        with httmock.HTTMock(self.route_to_testapp):
            func = tasr.client.get_schema_version
            return func(topic, version, self.host, self.port)

    def test_bare_reg_and_get_by_md5_id(self):
        '''get_reg_schema_for_id_str() - with md5 ID'''
        reg_rs = self.bare_register_schema_skeleton(self.schema_str)
        get_rs = self.bare_get_for_id_str_skeleton(reg_rs.md5_id)
        self.assertEqual(reg_rs, get_rs, 'got unexpected schema')

    def test_bare_reg_and_get_by_sha256_id(self):
        '''get_reg_schema_for_id_str() - with sha256 ID'''
        reg_rs = self.bare_register_schema_skeleton(self.schema_str)
        get_rs = self.bare_get_for_id_str_skeleton(reg_rs.sha256_id)
        self.assertEqual(reg_rs, get_rs, 'got unexpected schema')

    def test_bare_reg_and_get_non_existent_version(self):
        '''get_reg_schema_for_topic() - bad version'''
        reg_rs = self.bare_register_schema_skeleton(self.schema_str)
        bad_ver = reg_rs.current_version(self.event_type) + 1
        try:
            self.bare_get_for_topic_skeleton(self.schema_str, bad_ver)
            self.fail('Should have thrown an TASRError')
        except tasr.client.TASRError as te:
            self.assertTrue(te, 'Missing TASRError')

    def test_bare_reg_50_and_get_by_version(self):
        '''get_schema_for_topic() - multiple versions'''
        schemas = []
        for v in range(1, 50):
            ver_schema_str = copy.copy(self.schema_str)
            ver_schema_str = ver_schema_str.replace('tagged.events',
                                                    'tagged.events.%s' % v, 1)
            # whitespace gets normalized, so do that locally to the submitted
            # schema string so we have an accurate target for comparison
            ras = tasr.registered_schema.RegisteredAvroSchema()
            ras.schema_str = ver_schema_str
            canonical_ver_schema_str = ras.canonical_schema_str
            schemas.append(canonical_ver_schema_str)
            # reg with the non-canonicalized schema string
            rs = self.bare_register_schema_skeleton(ver_schema_str)
            self.assertEqual(canonical_ver_schema_str, rs.schema_str,
                             'Schema string modified!')
            self.assertIn(self.event_type, rs.group_names,
                          'Topic not in registered schema object.')
        # now pull them by version and check they match what we sent originally
        for v in range(1, 50):
            rs = self.bare_get_for_topic_skeleton(self.event_type, v)
            self.assertEqual(schemas[v - 1], rs.canonical_schema_str,
                             'Unexpected version.')

    def test_bare_reg_regmod_reg_then_get_ver_1(self):
        '''get_schema_for_topic() - non-sequential re-reg'''
        alt_schema_str = copy.copy(self.schema_str)
        alt_schema_str = alt_schema_str.replace('tagged.events',
                                                'tagged.events.alt', 1)
        rs1 = self.bare_register_schema_skeleton(self.schema_str)
        self.bare_register_schema_skeleton(alt_schema_str)
        rs3 = self.bare_register_schema_skeleton(self.schema_str)
        self.assertEqual(3, rs3.current_version(self.event_type),
                         'unexpected version')
        # now get version 1 -- should be same schema, and should list
        # requested version as "current"
        rs = self.bare_get_for_topic_skeleton(self.event_type, 1)
        self.assertEqual(rs1.canonical_schema_str, rs.canonical_schema_str,
                         'Unexpected schema string change between v1 and v3.')
        self.assertEqual(1, rs.current_version(self.event_type),
                        'Expected different current version value.')

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTASRClientMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
