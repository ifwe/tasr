'''
Created on Apr 8, 2014

@author: cmills
'''
import sys, os
_test_dir = os.path.abspath(os.path.dirname(__file__))
_src_dir = os.path.abspath(os.path.dirname('%s/../../src/py/tagged' % _test_dir))
sys.path.insert(0, os.path.join(_test_dir, _src_dir))
_fix_dir = os.path.abspath(os.path.dirname("%s/../fixtures/" % _test_dir))

import unittest
import avro.schema
from tasr import AvroSchemaRepository

try:
    import redis
    _local_redis = redis.StrictRedis(host='localhost', port=6379, db=0)
    _local_redis.keys('no_match_pattern') # should throw exception if no redis
    LOCAL_REDIS = True
except:
    LOCAL_REDIS = False


class TestTASR(unittest.TestCase):

    def setUp(self):
        self.event_type = "gold"
        self.avsc_file = "%s/schemas/%s.avsc" % (_fix_dir, self.event_type)
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
            for _k in self.asr.redis.keys():
                self.asr.redis.delete(_k)

    # registration tests
    def test_register_schema(self):
        _rs = self.asr.register(self.event_type, self.schema_str)
        self.assertFalse(_rs == None, u'Failed to register schema')

    def test_register_fail_for_empty_schema(self):
        try:
            _rs = self.asr.register(self.event_type, None)
            self.fail(u'Should have thrown an exception.')
        except:
            pass

    def test_register_fail_for_invalid_schema(self):
        try:
            _rs = self.asr.register(self.event_type, "%s }" % self.schema_str)
            self.fail(u'Should have thrown an exception.')
        except avro.schema.SchemaParseException:
            pass
        except Exception as ex:
            self.fail(u'Unexpected exception: %s' % ex)
        
    def test_reg_and_rereg(self):
        _rs = self.asr.register(self.event_type, self.schema_str)
        _re_rs = self.asr.register(self.event_type, self.schema_str)
        self.assertEqual(_rs, _re_rs, u'Re-registered schema different.')
        
    # retrieval tests
    def test_register_schema_and_get_latest_for_topic(self):
        _rs = self.asr.register(self.event_type, self.schema_str)
        _rs2 = self.asr.get_latest_for_topic(self.event_type)
        self.assertEqual(_rs, _rs2, u'Recovered registered schema unequal.')
        
    def test_reg_and_get_by_id(self):
        _rs = self.asr.register(self.event_type, self.schema_str)
        _rs.version = None
        _rs.topic = None
        self.assertEqual(_rs, self.asr.get_for_id(_rs.md5_id), 
                         u'MD5 ID retrieved unequal registered schema')
        self.assertEqual(_rs, self.asr.get_for_id(_rs.sha256_id), 
                         u'SHA256 ID retrieved unequal registered schema')

    def test_reg_then_reg_new_then_get_first_by_id(self):
        _rs = self.asr.register(self.event_type, self.schema_str)
        # modify the namespace in the schema to ensure a non-whitespace change
        _schema_str_2 = self.schema_str.replace('tagged.events', 'tagged.events.alt', 1)
        _rs2 = self.asr.register(self.event_type, _schema_str_2)
        self.assertNotEqual(_rs, _rs2, u'Modded schema unexpectedly equal on get')
        
        # now pull the first by id and assert equality to _rs (for md5 and sha256 ids)
        _re_rs = self.asr.get_for_id(_rs.md5_id)
        self.assertEqual(_rs, _re_rs, u'MD5 ID retrieved unequal registered schema')
        self.assertEqual(_rs, self.asr.get_for_id(_rs.sha256_id), 
                         u'SHA256 ID retrieved unequal registered schema')
        
    def test_reg_then_reg_new_and_get_latest_for_topic(self):
        _rs = self.asr.register(self.event_type, self.schema_str)
        # modify the namespace in the schema to ensure a non-whitespace change
        _schema_str_2 = self.schema_str.replace('tagged.events', 'tagged.events.alt', 1)
        _rs2 = self.asr.register(self.event_type, _schema_str_2)
        self.assertNotEqual(_rs, _rs2, u'Modded schema unexpectedly equal on get')
        
        # we should have two versions of the gold schema now, so grab the latest
        _latest_schema_str = self.asr.get_latest_for_topic(self.event_type).canonical_schema_str
        self.assertNotEqual(_rs.canonical_schema_str, _latest_schema_str, 
                            u'Latest schema unexpectedly equal to earlier version')
        self.assertEqual(_rs2.canonical_schema_str, _latest_schema_str, 
                         u'Latest schema unexpectedly unequal to later version')
        
if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTASR)
    unittest.TextTestRunner(verbosity=2).run(suite)


        
