'''
Created on Apr 8, 2014

@author: cmills
'''
import sys, os
test_dir = os.path.abspath(os.path.dirname(__file__))
src_dir = os.path.abspath(os.path.dirname('%s/../../src/py/tagged' % test_dir))
sys.path.insert(0, os.path.join(test_dir, src_dir))
fix_dir = os.path.abspath(os.path.dirname("%s/../fixtures/" % test_dir))

import unittest
import avro.schema
from tasr import AvroSchemaRepository

try:
    import redis
    local_redis = redis.StrictRedis(host='localhost', port=6379, db=0)
    local_redis.keys('no_match_pattern') # should throw exception if no redis
    LOCAL_REDIS = True
except:
    LOCAL_REDIS = False


class TestTASR(unittest.TestCase):

    def setUp(self):
        self.event_type = "gold"
        self.avsc_file = "%s/schemas/%s.avsc" % (fix_dir, self.event_type)
        self.schema_str = open(self.avsc_file, "r").read()
        self.schema_version = 0
        self.asr = None
        if LOCAL_REDIS:
            self.asr = AvroSchemaRepository()
        else:
            self.fail(u'Redis not available on localhost:6379')
    
    def tearDown(self):
        if LOCAL_REDIS:
            # clear out any added data
            for k in self.asr.redis.keys():
                self.asr.redis.delete(k)

    # registration tests
    def test_register_schema(self):
        rs = self.asr.register(self.event_type, self.schema_str)
        self.assertFalse(rs == None, u'Failed to register schema')

    def test_register_fail_for_empty_schema(self):
        try:
            self.asr.register(self.event_type, None)
            self.fail(u'Should have thrown an exception.')
        except:
            pass

    def test_register_fail_for_invalid_schema(self):
        try:
            self.asr.register(self.event_type, "%s }" % self.schema_str)
            self.fail(u'Should have thrown an exception.')
        except avro.schema.SchemaParseException:
            pass
        except Exception as ex:
            self.fail(u'Unexpected exception: %s' % ex)
        
    def test_reg_and_rereg(self):
        rs = self.asr.register(self.event_type, self.schema_str)
        re_rs = self.asr.register(self.event_type, self.schema_str)
        self.assertEqual(rs, re_rs, u'Re-registered schema different.')
        
    # retrieval tests
    def test_register_schema_and_get_latest_for_topic(self):
        rs = self.asr.register(self.event_type, self.schema_str)
        rs2 = self.asr.get_latest_for_topic(self.event_type)
        self.assertEqual(rs, rs2, u'Recovered registered schema unequal.')
        
    def test_reg_and_get_by_id(self):
        rs = self.asr.register(self.event_type, self.schema_str)
        rs.version = None
        rs.topic = None
        self.assertEqual(rs, self.asr.get_for_id(rs.md5_id), 
                         u'MD5 ID retrieved unequal registered schema')
        self.assertEqual(rs, self.asr.get_for_id(rs.sha256_id), 
                         u'SHA256 ID retrieved unequal registered schema')

    def test_reg_then_reg_new_then_get_first_by_id(self):
        rs = self.asr.register(self.event_type, self.schema_str)
        # modify the namespace in the schema to ensure a non-whitespace change
        schema_str_2 = self.schema_str.replace('tagged.events', 'tagged.events.alt', 1)
        rs2 = self.asr.register(self.event_type, schema_str_2)
        self.assertNotEqual(rs, rs2, u'Modded schema unexpectedly equal on get')
        
        # now pull the first by id and assert equality to _rs (for md5 and sha256 ids)
        _re_rs = self.asr.get_for_id(rs.md5_id)
        self.assertEqual(rs, _re_rs, u'MD5 ID retrieved unequal registered schema')
        self.assertEqual(rs, self.asr.get_for_id(rs.sha256_id), 
                         u'SHA256 ID retrieved unequal registered schema')

    def test_reg_50_then_get_by_version(self):
        rs_list = []
        for v in range(1, 50):
            ver_schema_str = self.schema_str.replace('tagged.events', 'tagged.events.%s' % v, 1)
            rs_list.append(self.asr.register(self.event_type, ver_schema_str))
            
        for v in range(1, 50):
            re_rs = re_rs = self.asr.get_for_topic_and_version(self.event_type, v)
            self.assertEqual(rs_list[v - 1], re_rs, u'retrieved schema unequal.')
        
    def test_reg_then_reg_new_and_get_latest_for_topic(self):
        rs = self.asr.register(self.event_type, self.schema_str)
        # modify the namespace in the schema to ensure a non-whitespace change
        schema_str_2 = self.schema_str.replace('tagged.events', 'tagged.events.alt', 1)
        rs2 = self.asr.register(self.event_type, schema_str_2)
        self.assertNotEqual(rs, rs2, u'Modded schema unexpectedly equal on get')
        
        # we should have two versions of the gold schema now, so grab the latest
        latest_schema_str = self.asr.get_latest_for_topic(self.event_type).canonical_schema_str
        self.assertNotEqual(rs.canonical_schema_str, latest_schema_str, 
                            u'Latest schema unexpectedly equal to earlier version')
        self.assertEqual(rs2.canonical_schema_str, latest_schema_str, 
                         u'Latest schema unexpectedly unequal to later version')
        
    def test_multi_version_for_topic(self):
        rs = self.asr.register(self.event_type, self.schema_str)
        # modify the namespace in the schema to ensure a non-whitespace change
        schema_str_2 = self.schema_str.replace('tagged.events', 'tagged.events.alt', 1)
        self.asr.register(self.event_type, schema_str_2)
        # now re-register the original schema, which should become version 3
        rs3 = self.asr.register(self.event_type, self.schema_str)
        self.assertEqual(rs.sha256_id, rs3.sha256_id, u'Unequal SHA256 IDs on re-reg!')
        self.assertNotEqual(rs, rs3, u'Expected different versions for topic.')
        vlist = self.asr.get_all_versions_for_id_and_topic(rs3.sha256_id, self.event_type)
        self.assertEqual(2, len(vlist), u'Expected two entry version list.')
        self.assertEqual(1, vlist[0], u'Expected first version to be 1.')
        self.assertEqual(3, vlist[1], u'Expected second version to be 3.')
    
    def test_reg_for_2_topics(self):
        rs = self.asr.register(self.event_type, self.schema_str)
        get_rs = self.asr.get_latest_for_topic(self.event_type)
        self.assertEqual(rs, get_rs, u'Recovered registered schema unequal.')
        alt_topic = 'bob'
        rs2 = self.asr.register(alt_topic, self.schema_str)
        get_rs2 = self.asr.get_latest_for_topic(alt_topic)
        self.assertEqual(rs2, get_rs2, u'Recovered registered schema unequal.')
        self.assertEqual(get_rs, get_rs2, u'Recovered registered schema unequal.')
        
if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTASR)
    unittest.TextTestRunner(verbosity=2).run(suite)


        
