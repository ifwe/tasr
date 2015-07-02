'''
Created on May 7, 2014

@author: cmills
'''

from client_test import TestTASRAppClient

import unittest
import tasr.client_legacy
import copy
import httmock

APP = tasr.app.TASR_APP
APP.set_config_mode('local')


class TestTASRLegacyClientObject(TestTASRAppClient):

    def setUp(self):
        super(TestTASRLegacyClientObject, self).setUp()
        self.event_type = "gold"
        fix_rel_path = "schemas/%s.avsc" % (self.event_type)
        self.avsc_file = self.get_fixture_file(fix_rel_path, "r")
        self.schema_str = self.avsc_file.read()
        # client settings
        self.host = 'localhost'  # should match netloc below
        self.port = 8080         # should match netloc below
        # clear out all the keys before beginning -- careful!
        APP.ASR.redis.flushdb()

    def tearDown(self):
        # this clears out redis after each test -- careful!
        APP.ASR.redis.flushdb()

    ########################################################################
    # registration tests
    ########################################################################
    def obj_register_schema_skeleton(self, schema_str):
        '''TASRLegacyClient.register_schema() - skeleton test'''
        # whitespace gets normalized, so do that locally to the submitted
        # schema string so we have an accurate target for comparison
        ras = tasr.registered_schema.RegisteredAvroSchema()
        ras.schema_str = schema_str
        canonical_schema_str = ras.json
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client_legacy.TASRLegacyClient(self.host, self.port)
            rs = client.register_schema(self.event_type, schema_str)
            self.assertEqual(canonical_schema_str, rs.json,
                             'Schema string modified!')
            self.assertIn(self.event_type, rs.group_names,
                          'Topic not in registered schema object.')
            self.assertIn(self.event_type, rs.ts_dict.keys(),
                          'Topic not in registration timestamps.')
            return rs

    def test_obj_register_schema(self):
        '''TASRLegacyClient.register_schema() - as expected'''
        self.obj_register_schema_skeleton(self.schema_str)

    def test_obj_reg_fail_on_empty_schema(self):
        '''TASRLegacyClient.register_schema() - fail on empty schema'''
        try:
            self.obj_register_schema_skeleton(None)
            self.fail('should have thrown a TASRError')
        except tasr.client_legacy.TASRError as te:
            self.assertTrue(te, 'Missing TASRError')

    def test_obj_reg_fail_on_invalid_schema(self):
        '''TASRLegacyClient.register_schema() - fail on invalid schema'''
        try:
            bad_schema = '%s }' % self.schema_str
            self.obj_register_schema_skeleton(bad_schema)
            self.fail('should have thrown a ValueError')
        except tasr.client_legacy.TASRError:
            self.fail('should have thrown a ValueError')
        except ValueError:
            pass

    def test_obj_reg_and_rereg(self):
        '''TASRLegacyClient.register_schema() - multi calls, same schema'''
        rs1 = self.obj_register_schema_skeleton(self.schema_str)
        rs2 = self.obj_register_schema_skeleton(self.schema_str)
        self.assertEqual(rs1, rs2, 'reg and rereg schemas unequal!')

    ########################################################################
    # topic retrieval tests for TASR API
    ########################################################################
    def test_obj_get_get_all_topics_with_none_present(self):
        '''TASRLegacyClient.get_all_topics()'''
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client_legacy.TASRLegacyClient(self.host, self.port)
            group_metas = client.get_all_topics()
            self.assertEqual(0, len(group_metas), 'expected no groups')

    def test_obj_get_get_all_topics_with_one_present(self):
        '''TASRLegacyClient.get_all_topics()'''
        self.obj_register_schema_skeleton(self.schema_str)
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client_legacy.TASRLegacyClient(self.host, self.port)
            group_metas = client.get_all_topics()
            self.assertListEqual(group_metas.keys(), [self.event_type, ],
                                 'unexpected groups: %s' % group_metas.keys())

    ########################################################################
    # schema retrieval tests for TASR API
    ########################################################################
    def obj_get_for_id_str_skeleton(self, id_str):
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client_legacy.TASRLegacyClient(self.host, self.port)
            rs = client.schema_for_id_str(id_str)
            self.assertIn(id_str, (rs.sha256_id, rs.md5_id), 'ID missing')
            return rs

    def obj_get_for_topic_skeleton(self, topic, version):
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client_legacy.TASRLegacyClient(self.host, self.port)
            return client.get_schema_version(topic, version)

    def test_obj_reg_and_get_by_md5_id(self):
        '''TASRLegacyClient.schema_for_id_str() - with md5 ID'''
        reg_rs = self.obj_register_schema_skeleton(self.schema_str)
        get_rs = self.obj_get_for_id_str_skeleton(reg_rs.md5_id)
        self.assertEqual(reg_rs, get_rs, 'got unexpected schema')

    def test_obj_reg_and_get_by_sha256_id(self):
        '''TASRLegacyClient.schema_for_id_str() - with sha256 ID'''
        reg_rs = self.obj_register_schema_skeleton(self.schema_str)
        get_rs = self.obj_get_for_id_str_skeleton(reg_rs.sha256_id)
        self.assertEqual(reg_rs, get_rs, 'got unexpected schema')

    def test_obj_reg_and_get_non_existent_version(self):
        '''TASRLegacyClient.get_schema_for_topic() - bad version'''
        reg_rs = self.obj_register_schema_skeleton(self.schema_str)
        bad_ver = reg_rs.current_version(self.event_type) + 1
        try:
            self.obj_get_for_topic_skeleton(self.schema_str, bad_ver)
            self.fail('Should have thrown an TASRError')
        except tasr.client_legacy.TASRError as te:
            self.assertTrue(te, 'Missing TASRError')

    def test_obj_reg_50_and_get_by_version(self):
        '''TASRLegacyClient.get_schema_for_topic() - multiple versions'''
        schemas = []
        for v in range(1, 50):
            ver_schema_str = copy.copy(self.schema_str)
            ver_schema_str = ver_schema_str.replace('tagged.events',
                                                    'tagged.events.%s' % v, 1)
            # whitespace gets normalized, so do that locally to the submitted
            # schema string so we have an accurate target for comparison
            ras = tasr.registered_schema.RegisteredAvroSchema()
            ras.schema_str = ver_schema_str
            canonical_ver_schema_str = ras.json
            schemas.append(canonical_ver_schema_str)
            # reg with the non-canonicalized schema string
            rs = self.obj_register_schema_skeleton(ver_schema_str)
            self.assertEqual(canonical_ver_schema_str, rs.json,
                             'Schema string modified!')
            self.assertIn(self.event_type, rs.group_names,
                          'Topic not in registered schema object.')
        # now pull them by version and check they match what we sent originally
        for v in range(1, 50):
            rs = self.obj_get_for_topic_skeleton(self.event_type, v)
            self.assertEqual(schemas[v - 1], rs.json, 'Unexpected version.')

    def test_obj_reg_regmod_reg_then_get_ver_1(self):
        '''TASRLegacyClient.get_schema_for_topic() - non-sequential re-reg'''
        alt_schema_str = copy.copy(self.schema_str)
        alt_schema_str = alt_schema_str.replace('tagged.events',
                                                'tagged.events.alt', 1)
        rs1 = self.obj_register_schema_skeleton(self.schema_str)
        self.obj_register_schema_skeleton(alt_schema_str)
        rs3 = self.obj_register_schema_skeleton(self.schema_str)
        self.assertEqual(3, rs3.current_version(self.event_type),
                         'unexpected version')
        # now get version 1 -- should be same schema, and should list
        # requested version as "current"
        rs = self.obj_get_for_topic_skeleton(self.event_type, 1)
        self.assertEqual(rs1.json, rs.json,
                         'Unexpected schema string change between v1 and v3.')
        self.assertEqual(1, rs.current_version(self.event_type),
                        'Expected different current version value.')

    def test_obj_multi_topic_reg(self):
        '''TASRLegacyClient.get_schema_for_topic() - one schema, registered for
        multiple group_names: should be ver=1 for each'''
        self.obj_register_schema_skeleton(self.schema_str)
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client_legacy.TASRLegacyClient(self.host, self.port)
            alt_topic = 'bob'
            rs = client.register_schema(alt_topic, self.schema_str)
            self.assertEqual(1, rs.current_version(alt_topic), 'bad version')
            # now grab the RS by ID, which should have all topic:versions
            rs2 = client.schema_for_id_str(rs.sha256_id)
            self.assertEqual(1, rs2.current_version(alt_topic), 'bad version')
            self.assertEqual(1, rs2.current_version(self.event_type),
                             'Expected version of 1.')

if __name__ == "__main__":
    LOADER = unittest.TestLoader()
    SUITE = LOADER.loadTestsFromTestCase(TestTASRLegacyClientObject)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
