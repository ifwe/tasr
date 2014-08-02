'''
Created on May 7, 2014

@author: cmills
'''

from client_test import TestTASRAppClient

import unittest
import tasr.app
import tasr.client_sv
import copy
import httmock


class TestTASRClientSVObject(TestTASRAppClient):

    def setUp(self):
        super(TestTASRClientSVObject, self).setUp()
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
    def obj_register_schema_skeleton(self, schema_str):
        '''TASRClientSV.register_schema() - skeleton test'''
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client_sv.TASRClientSV(self.host, self.port)
            rs = client.register_schema(self.event_type, schema_str)
            self.assertEqual(schema_str, rs.schema_str,
                             'Schema string modified!')
            self.assertIn(self.event_type, rs.group_names,
                          'Topic not in registered schema object.')
            self.assertIn(self.event_type, rs.ts_dict.keys(),
                          'Topic not in registration timestamps.')
            return rs

    def test_obj_register_schema(self):
        '''TASRClientSV.register_schema() - as expected'''
        self.obj_register_schema_skeleton(self.schema_str)

    def test_obj_reg_fail_on_empty_schema(self):
        '''TASRClientSV.register_schema() - fail on empty schema'''
        try:
            self.obj_register_schema_skeleton(None)
            self.fail('should have thrown a TASRError')
        except tasr.client_sv.TASRError as te:
            self.assertTrue(te, 'Missing TASRError')

    def test_obj_reg_fail_on_invalid_schema(self):
        '''TASRClientSV.register_schema() - fail on invalid schema'''
        try:
            bad_schema = '%s }' % self.schema_str
            self.obj_register_schema_skeleton(bad_schema)
            self.fail('should have thrown a TASRError')
        except tasr.client_sv.TASRError as te:
            self.assertTrue(te, 'Missing TASRError')

    def test_obj_reg_and_rereg(self):
        '''TASRClientSV.register_schema() - multi calls, same schema'''
        rs1 = self.obj_register_schema_skeleton(self.schema_str)
        rs2 = self.obj_register_schema_skeleton(self.schema_str)
        self.assertEqual(rs1, rs2, 'reg and rereg schemas unequal!')

    ########################################################################
    # topic retrieval tests for TASR S+V API
    ########################################################################
    def obj_register_subject_skeleton(self):
        '''TASRClientSV.register_subject() - skeleton test'''
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client_sv.TASRClientSV(self.host, self.port)
            meta = client.register_subject(self.event_type)
            self.assertIn(self.event_type, meta.name, 'Bad subject name.')
            return meta

    def test_obj_get_get_all_subjects_with_none_present(self):
        '''TASRClientSV.get_all_subjects()'''
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client_sv.TASRClientSV(self.host, self.port)
            subject_names = client.get_all_subjects()
            self.assertEqual(0, len(subject_names), 'expected no groups')

    def test_obj_get_get_all_subjects_with_one_present(self):
        '''TASRClientSV.get_all_subjects()'''
        self.obj_register_subject_skeleton()
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client_sv.TASRClientSV(self.host, self.port)
            subject_names = client.get_all_subjects()
            self.assertListEqual(subject_names, [self.event_type, ],
                                 'unexpected groups: %s' % subject_names)

    def test_obj_register_subject(self):
        '''TASRClientSV.register_subject()'''
        self.obj_register_subject_skeleton()

    def test_obj_lookup_subject(self):
        '''TASRClientSV.lookup_subject()'''
        self.obj_register_subject_skeleton()
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client_sv.TASRClientSV(self.host, self.port)
            self.assertTrue(client.lookup_subject(self.event_type))

    def test_obj_lookup_missing_subject(self):
        '''TASRClientSV.lookup_subject()'''
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client_sv.TASRClientSV(self.host, self.port)
            self.assertFalse(client.lookup_subject(self.event_type))

    ########################################################################
    # schema retrieval tests for TASR API
    ########################################################################
    def obj_lookup_by_version_skeleton(self, topic, version):
        with httmock.HTTMock(self.route_to_testapp):
            client = tasr.client_sv.TASRClientSV(self.host, self.port)
            return client.lookup_by_version(topic, version)

    def test_obj_reg_and_get_non_existent_version(self):
        '''TASRClientSV.lookup_by_version() - bad version'''
        reg_rs = self.obj_register_schema_skeleton(self.schema_str)
        bad_ver = reg_rs.current_version(self.event_type) + 1
        try:
            self.obj_lookup_by_version_skeleton(self.schema_str, bad_ver)
            self.fail('Should have thrown an TASRError')
        except tasr.client_sv.TASRError as te:
            self.assertTrue(te, 'Missing TASRError')

    def test_obj_reg_50_and_get_by_version(self):
        '''TASRClientSV.get_schema_for_topic() - multiple versions'''
        schemas = []
        for v in range(1, 50):
            ver_schema_str = copy.copy(self.schema_str)
            ver_schema_str = ver_schema_str.replace('tagged.events',
                                                    'tagged.events.%s' % v, 1)
            schemas.append(ver_schema_str)
            rs = self.obj_register_schema_skeleton(ver_schema_str)
            self.assertEqual(ver_schema_str, rs.schema_str,
                             'Schema string modified!')
            self.assertIn(self.event_type, rs.group_names,
                          'Topic not in registered schema object.')
        # now pull them by version and check they match what we sent originally
        for v in range(1, 50):
            rs = self.obj_lookup_by_version_skeleton(self.event_type, v)
            self.assertEqual(schemas[v - 1], rs.canonical_schema_str,
                             'Unexpected version.')

    def test_obj_reg_regmod_reg_then_get_ver_1(self):
        '''TASRClientSV.get_schema_for_topic() - non-sequential re-reg'''
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
        rs = self.obj_lookup_by_version_skeleton(self.event_type, 1)
        self.assertEqual(rs1.canonical_schema_str, rs.canonical_schema_str,
                         'Unexpected schema string change between v1 and v3.')
        self.assertEqual(1, rs.current_version(self.event_type),
                        'Expected different current version value.')

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTASRClientSVObject)
    unittest.TextTestRunner(verbosity=2).run(suite)
