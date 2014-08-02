'''
Created on Apr 8, 2014

@author: cmills
'''

from tasr_test import TASRTestCase

import unittest
import avro.schema
import time
from tasr import AvroSchemaRepository

try:
    import redis
    # note we use port 5379 (instead of 6379) in prod, so we test against that
    R_TEST = redis.StrictRedis(host='localhost', port=5379, db=0)
    R_TEST.keys('no_match_pattern')  # should throw exception if no redis
    HASR_LOCAL_REDIS = True
except ImportError:
    HASR_LOCAL_REDIS = False


class TestTASR(TASRTestCase):

    def setUp(self):
        self.event_type = "gold"
        fix_rel_path = "schemas/%s.avsc" % (self.event_type)
        self.avsc_file = TASRTestCase.get_fixture_file(fix_rel_path, "r")
        self.schema_str = self.avsc_file.read()
        self.schema_version = 0
        self.asr = None
        if HASR_LOCAL_REDIS:
            self.asr = AvroSchemaRepository(host='localhost', port=5379)
            # clear out all the keys before beginning -- careful!
            self.asr.redis.flushdb()
        else:
            self.fail(u'Redis not available on localhost:5379')

    def tearDown(self):
        if HASR_LOCAL_REDIS:
            # clear out any added data
            self.asr.redis.flushdb()

    # registration tests
    def test_register_schema(self):
        '''register_schema() - as expected'''
        rs = self.asr.register_schema(self.event_type, self.schema_str)
        self.assertFalse(rs == None, u'Failed to register_schema schema')
        dif = long(time.time()) - rs.current_version_timestamp(self.event_type)
        self.assertTrue(dif <= 1, 'crazy timestamp')

    def test_register_fail_for_empty_schema(self):
        '''register_schema() - error case'''
        try:
            self.asr.register_schema(self.event_type, None)
            self.fail(u'Should have thrown a ValueError.')
        except ValueError:
            pass

    def test_register_fail_for_invalid_schema(self):
        '''register_schema() - error case'''
        try:
            self.asr.register_schema(self.event_type, "%s }" % self.schema_str)
            self.fail(u'Should have thrown a SchemaParseException.')
        except avro.schema.SchemaParseException:
            pass

    def test_reg_and_rereg(self):
        '''register_schema() - re-reg of current shouldn't change versions'''
        rs = self.asr.register_schema(self.event_type, self.schema_str)
        re_rs = self.asr.register_schema(self.event_type, self.schema_str)
        self.assertEqual(rs, re_rs, u'Re-registered schema different.')

    def test_reg_1_schema_for_2_topics(self):
        '''register_schema() - same schema for two topics'''
        rs = self.asr.register_schema(self.event_type, self.schema_str)
        get_rs = self.asr.get_latest_schema_for_group(self.event_type)
        self.assertEqual(rs, get_rs, u'Recovered registered schema unequal.')
        alt_topic = 'bob'
        rs2 = self.asr.register_schema(alt_topic, self.schema_str)
        get_rs2 = self.asr.get_latest_schema_for_group(alt_topic)
        self.assertEqual(rs2, get_rs2, u'Recovered reg schema unequal.')
        self.assertEqual(get_rs, get_rs2, u'Recovered reg schema unequal.')

    # retrieval tests
    def test_lookup(self):
        '''lookup_subject() - as expected'''
        self.assertFalse(self.asr.lookup_subject(self.event_type),
                         'Topic should not be registered yet.')
        self.asr.register_schema(self.event_type, self.schema_str)
        self.assertTrue(self.asr.lookup_subject(self.event_type),
                        'Topic should be registered.')

    def test_get_latest_for_topic(self):
        '''get_latest_schema_for_group() - as expected'''
        rs = self.asr.register_schema(self.event_type, self.schema_str)
        rs2 = self.asr.get_latest_schema_for_group(self.event_type)
        self.assertEqual(rs, rs2, u'Recovered registered schema unequal.')

    def test_get_latest_fail_for_missing_topic(self):
        '''get_latest_schema_for_group() - error case'''
        rs = self.asr.get_latest_schema_for_group(self.event_type)
        self.assertEqual(None, rs, 'expected None back for missing topic')

    def test_reg_then_reg_new_and_get_latest_for_topic(self):
        '''get_latest_schema_for_group() - as expected'''
        rs = self.asr.register_schema(self.event_type, self.schema_str)
        # modify the namespace in the schema to ensure a non-whitespace change
        schema_str_2 = self.schema_str.replace('tagged.events',
                                               'tagged.events.alt', 1)
        rs2 = self.asr.register_schema(self.event_type, schema_str_2)
        self.assertNotEqual(rs, rs2, u'Modded schema unexpectedly equal')

        # should have two versions of the gold schema now, so grab the latest
        latest_schema = self.asr.get_latest_schema_for_group(self.event_type)
        latest_schema_str = latest_schema.canonical_schema_str
        self.assertNotEqual(rs.canonical_schema_str, latest_schema_str,
                            u'Latest schema equal to earlier version')
        self.assertEqual(rs2.canonical_schema_str, latest_schema_str,
                         u'Latest schema unequal to later version')

    def test_reg_50_then_get_for_topic_and_version(self):
        '''get_schema_for_group_and_version() - as expected'''
        rs_list = []
        for v in range(1, 50):
            ver_schema_str = self.schema_str.replace('tagged.events',
                                                     'tagged.events.%s' % v, 1)
            rs_list.append(self.asr.register_schema(self.event_type, ver_schema_str))

        for v in range(1, 50):
            re_rs = self.asr.get_schema_for_group_and_version(self.event_type, v)
            self.assertEqual(rs_list[v - 1], re_rs,
                             u'retrieved schema unequal.')

    def test_get_for_topic_and_version_fail_for_missing_version(self):
        '''get_schema_for_group_and_version() - error case'''
        self.asr.register_schema(self.event_type, self.schema_str)
        rs = self.asr.get_schema_for_group_and_version(self.event_type, 2)
        self.assertEqual(None, rs, 'expected None back for missing topic')

    def test_get_for_id(self):
        '''get_schema_for_id_str() - as expected'''
        rs = self.asr.register_schema(self.event_type, self.schema_str)
        rs.version = None
        rs.topic = None
        self.assertEqual(rs, self.asr.get_schema_for_id_str(rs.md5_id),
                         u'MD5 ID retrieved unequal registered schema')
        self.assertEqual(rs, self.asr.get_schema_for_id_str(rs.sha256_id),
                         u'SHA256 ID retrieved unequal registered schema')

    def test_get_first_for_id(self):
        '''get_schema_for_id_str() - with non-sequential re-registration'''
        rs = self.asr.register_schema(self.event_type, self.schema_str)
        # modify the namespace in the schema to ensure a non-whitespace change
        schema_str_2 = self.schema_str.replace('tagged.events',
                                               'tagged.events.alt', 1)
        rs2 = self.asr.register_schema(self.event_type, schema_str_2)
        self.assertNotEqual(rs, rs2, u'Modded schema unexpectedly equal')

        # now pull the first by id and assert equality to _rs
        re_rs = self.asr.get_schema_for_id_str(rs.md5_id)
        self.assertEqual(rs, re_rs, u'MD5 ID retrieved unequal reg schema')
        self.assertEqual(rs, self.asr.get_schema_for_id_str(rs.sha256_id),
                         u'SHA256 ID retrieved unequal registered schema')

    def test_get_for_schema_str(self):
        '''get_schema_for_schema_str() - as expected'''
        rs = self.asr.register_schema(self.event_type, self.schema_str)
        re_rs = self.asr.get_schema_for_schema_str(self.schema_str)
        self.assertEqual(rs, re_rs, u'Schema str retrieved unequal reg schema')

    def test_get_for_schema_str_fail_for_bad_schema(self):
        '''get_schema_for_schema_str() - error case'''
        self.asr.register_schema(self.event_type, self.schema_str)
        try:
            self.asr.get_schema_for_schema_str("%s }" % self.schema_str)
            self.fail(u'Should have got raised a SchemaParseException')
        except avro.schema.SchemaParseException:
            pass

    def test_get_latest_versions_for_topic(self):
        '''get_latest_schema_versions_for_group() - pull recent 2 when there are 3.'''
        rs1 = self.asr.register_schema(self.event_type, self.schema_str)
        schema_str_2 = self.schema_str.replace('tagged.events',
                                               'tagged.events.2', 1)
        rs2 = self.asr.register_schema(self.event_type, schema_str_2)
        schema_str_3 = self.schema_str.replace('tagged.events',
                                               'tagged.events.3', 1)
        rs3 = self.asr.register_schema(self.event_type, schema_str_3)
        rs_list = self.asr.get_latest_schema_versions_for_group(self.event_type, 2)
        self.assertEqual(2, len(rs_list), 'Expected a list of length 2.')
        self.assertEqual(rs2, rs_list[0], 'Expecting RS2 as first entry.')
        self.assertEqual(rs3, rs_list[1], 'Expecting RS3 as second entry.')
        # now test getting all the versions
        rs_list = self.asr.get_latest_schema_versions_for_group(self.event_type, -1)
        self.assertEqual(3, len(rs_list), 'Expected a list of length 2.')
        self.assertEqual(rs1, rs_list[0], 'Expecting RS1 as first entry.')
        self.assertEqual(rs2, rs_list[1], 'Expecting RS2 as second entry.')
        self.assertEqual(rs3, rs_list[2], 'Expecting RS3 as third entry.')

    def test_get_all_subjects(self):
        '''get_all_topics() - as expected'''
        self.assertEqual(0, len(self.asr.get_all_topics()),
                         'should not be any topics yet')
        self.asr.register_schema(self.event_type, self.schema_str)
        subjects = self.asr.get_all_topics()
        self.assertEqual(1, len(subjects), 'should have 1')
        self.assertEqual(self.event_type, subjects[0].name,
                         'expected subject missing')
        schema_str_2 = self.schema_str.replace('tagged.events',
                                               'tagged.events.alt', 1)
        # reg another version -- should not increase number of topics
        self.asr.register_schema(self.event_type, schema_str_2)
        subjects = self.asr.get_all_topics()
        self.assertEqual(1, len(subjects), 'should still have 1 subject')

    def test_multi_version_for_topic(self):
        '''get_versions_for_id_str_and_group() - as expected'''
        rs = self.asr.register_schema(self.event_type, self.schema_str)
        # modify the namespace in the schema to ensure a non-whitespace change
        schema_str_2 = self.schema_str.replace('tagged.events',
                                               'tagged.events.alt', 1)
        self.asr.register_schema(self.event_type, schema_str_2)
        # now re-register_schema the original schema, which should become version 3
        rs3 = self.asr.register_schema(self.event_type, self.schema_str)
        self.assertEqual(rs.sha256_id, rs3.sha256_id,
                         u'Unequal SHA256 IDs on re-reg!')
        self.assertNotEqual(rs, rs3, u'Expected different versions for topic.')
        vlist = self.asr.get_versions_for_id_str_and_group(rs3.sha256_id,
                                                       self.event_type)
        self.assertEqual(2, len(vlist), u'Expected two entry version list.')
        self.assertEqual(1, vlist[0], u'Expected first version to be 1.')
        self.assertEqual(3, vlist[1], u'Expected second version to be 3.')

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTASR)
    unittest.TextTestRunner(verbosity=2).run(suite)
