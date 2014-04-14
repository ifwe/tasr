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
from tasr import AvroSchemaRepository

try:
    import redis
    _local_redis = redis.StrictRedis(host='localhost', port=6379, db=0)
    LOCAL_REDIS = True
except:
    LOCAL_REDIS = False


class TestASR(unittest.TestCase):

    def setUp(self):
        self.event_type = "gold"
        self.avsc_file = "%s/schemas/%s.avsc" % (_fix_dir, self.event_type)
        self.schema_str = open(self.avsc_file, "r").read()
        self.schema_version = 0
        self.asr = None
        if LOCAL_REDIS:
            self.asr = AvroSchemaRepository()
    
    def tearDown(self):
        if LOCAL_REDIS:
            # clear out any added data
            for _k in self.asr.redis.keys():
                self.asr.redis.delete(_k)

    @unittest.skipUnless(LOCAL_REDIS, u'Redis not available on localhost:6379')
    def test_register_schema(self):
        _rs = self.asr.register(self.event_type, self.schema_str)
        self.assertFalse(_rs == None, u'Failed to register schema')

    def test_register_fail_for_invalid_schema(self):
        try:
            _rs = self.asr.register(self.event_type, None)
            self.fail(u'Should have thrown an exception.')
        except:
            pass

    def test_register_schema_and_get_latest_for_topic(self):
        _rs = self.asr.register(self.event_type, self.schema_str)
        _rs2 = self.asr.getLatestRegisteredSchemaForTopic(self.event_type)
        self.assertEqual(_rs, _rs2, u'Recovered registered schema unequal.')
        
    def test_reg_and_rereg(self):
        _rs = self.asr.register(self.event_type, self.schema_str)
        _re_rs = self.asr.register(self.event_type, self.schema_str)
        self.assertEqual(_rs, _re_rs, u'Re-registered schema different.')
        
    def test_reg_and_get_by_id(self):
        _rs = self.asr.register(self.event_type, self.schema_str)
        self.assertEqual(_rs, self.asr.getAllRegisteredSchemasForID(_rs.md5_id)[0], 
                         u'MD5 ID retrieved unequal registered schema')
        self.assertEqual(_rs, self.asr.getAllRegisteredSchemasForID(_rs.sha256_id)[0], 
                         u'SHA256 ID retrieved unequal registered schema')
        
    def test_reg_then_reg_new_and_get_latest_for_topic(self):
        _rs = self.asr.register(self.event_type, self.schema_str)
        # modify the namespace in the schema to ensure a non-whitespace change
        _schema_str_2 = self.schema_str.replace('tagged.events', 'tagged.events.alt', 1)
        _rs2 = self.asr.register(self.event_type, _schema_str_2)
        self.assertNotEqual(_rs, _rs2, u'Modded schema unexpectedly equal on get')
        
        # we should have two versions of the gold schema now, so grab the latest
        _latest_schema_str = self.asr.getLatestForTopic(self.event_type)
        self.assertNotEqual(_rs.cannonical_schema, _latest_schema_str, 
                            u'Latest schema unexpectedly equal to earlier version')
        self.assertEqual(_rs2.cannonical_schema, _latest_schema_str, 
                         u'Latest schema unexpectedly unequal to later version')
        
        # also grab all RSs for topic and confirm they match _rs and _rs2
        _all_rs = self.asr.getAllRegisteredSchemasForTopic(self.event_type)
        self.assertEqual(2, len(_all_rs), u'Got wrong number of versions back (%s)' % len(_all_rs))
        self.assertEqual(_rs, _all_rs[0], u'First version wrong.')
        self.assertEqual(_rs2, _all_rs[1], u'Second version wrong.')



        
