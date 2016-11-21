'''
Created on Oct 19, 2016

@author: cmills
'''
import datetime
import unittest
from tasr_test import TASRTestCase

from webtest import TestApp
import tasr.app
from tasr.headers import SchemaHeaderBot
from tasr.utils.serializer import MTSerializer, MTDeserializer
import logging

APP = tasr.app.TASR_APP
APP.set_config_mode('local')

logging.getLogger().setLevel(logging.INFO)

class TestSerializer(TASRTestCase):
    def setUp(self):
        self.event_type = "gold"
        fix_rel_path = "schemas/%s.avsc" % (self.event_type)
        self.avsc_file = TASRTestCase.get_fixture_file(fix_rel_path, "r")
        self.schema_str = self.avsc_file.read()
        self.tasr_app = TestApp(APP)
        self.url_prefix = 'http://%s:%s' % (APP.config.host, APP.config.port)
        self.subject_url = '%s/tasr/subject/%s' % (self.url_prefix,
                                                   self.event_type)
        self.content_type = 'application/json; charset=utf8'
        # clear out all the keys before beginning -- careful!
        APP.ASR.redis.flushdb()

    def tearDown(self):
        # this clears out redis after each test -- careful!
        APP.ASR.redis.flushdb()

    def abort_diff_status(self, resp, code):
        self.assertEqual(code, resp.status_code,
                         u'Non-%s status code: %s' % (code, resp.status_code))

    def register_schema(self, subject_name, schema_str, expect_errors=False):
        reg_url = '%s/tasr/subject/%s/register' % (self.url_prefix,
                                                   subject_name)
        return self.tasr_app.request(reg_url, method='PUT',
                                     content_type=self.content_type,
                                     expect_errors=expect_errors,
                                     body=schema_str)

    @staticmethod
    def minimal_gold_event_dict(uid=long(123), dt=None):
        if dt is None:
            dt = datetime.datetime.now()
        ged = dict()
        ged['source__timestamp'] = long(dt.strftime('%s'))
        ged['source__agent'] = 'pyunit_test'
        ged['source__ip_address'] = '127.0.0.1'
        ged['gold__user_id'] = uid
        return ged

    # tests
    def test_fail_create_mtserializer(self):
        try:
            MTSerializer()
            self.fail('Should have thrown exception for null creation.')
        except RuntimeError:
            pass

    def test_create_mts_from_sub_and_ver(self):
        # first reg the schema
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        meta = SchemaHeaderBot.extract_metadata(resp)
        vnum = meta.group_version(self.event_type)
        # with the schema registered, try to instantiate an MTSerializer
        mts = MTSerializer(topic=self.event_type, version_number=vnum,
                           tasr_url=self.url_prefix, tasr_app=self.tasr_app)
        self.assertEqual(self.event_type, mts.topic, 'topic mismatch')

    def test_create_mts_from_sha(self):
        # first reg the schema
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        meta = SchemaHeaderBot.extract_metadata(resp)
        # with the schema registered, try to instantiate an MTSerializer
        mts = MTSerializer(sha256_id=meta.sha256_id, tasr_url=self.url_prefix,
                           tasr_app=self.tasr_app)
        self.assertEqual(self.event_type, mts.topic, 'topic mismatch')

    def test_create_mtd_from_mts(self):
        # first reg the schema
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        meta = SchemaHeaderBot.extract_metadata(resp)
        # with the schema registered, try to instantiate an MTSerializer
        mts = MTSerializer(sha256_id=meta.sha256_id, tasr_url=self.url_prefix,
                           tasr_app=self.tasr_app)
        self.assertEqual(self.event_type, mts.topic, 'topic mismatch')
        # create the MTDeserializer using the MTSerializer convenience method
        mtd = mts.get_deserializer()
        self.assertEqual(self.event_type, mtd.topic, 'topic mismatch')
        self.assertEqual(mts.sha256_id, mtd.sha256_id, 'sha256 mismatch')

    def test_serialize_event(self):
        # prelims -- reg schema, instantiate mts
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        meta = SchemaHeaderBot.extract_metadata(resp)
        mts = MTSerializer(sha256_id=meta.sha256_id, tasr_url=self.url_prefix,
                           tasr_app=self.tasr_app)
        # create a dict to serialize, then pass it to mts
        gold_event = self.minimal_gold_event_dict()
        serialized_event = mts.serialize_event(gold_event)
        logging.info('encoded event: %s', serialized_event)

    def test_create_mtd_from_sub_and_ver(self):
        # first reg the schema
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        meta = SchemaHeaderBot.extract_metadata(resp)
        vnum = meta.group_version(self.event_type)
        # with the schema registered, try to instantiate an MTDeserializer
        mtd = MTDeserializer(topic=self.event_type, version_number=vnum,
                             tasr_url=self.url_prefix, tasr_app=self.tasr_app)
        self.assertEqual(self.event_type, mtd.topic, 'topic mismatch')

    def test_create_mtd_from_sha(self):
        # first reg the schema
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        meta = SchemaHeaderBot.extract_metadata(resp)
        # with the schema registered, try to instantiate an MTDeserializer
        mtd = MTDeserializer(sha256_id=meta.sha256_id, tasr_url=self.url_prefix,
                             tasr_app=self.tasr_app)
        self.assertEqual(self.event_type, mtd.topic, 'topic mismatch')

    def test_create_mts_from_mtd(self):
        # first reg the schema
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        meta = SchemaHeaderBot.extract_metadata(resp)
        # with the schema registered, try to instantiate an MTDeserializer
        mtd = MTDeserializer(sha256_id=meta.sha256_id, tasr_url=self.url_prefix,
                             tasr_app=self.tasr_app)
        self.assertEqual(self.event_type, mtd.topic, 'topic mismatch')
        # create the MTDeserializer using the MTSerializer convenience method
        mts = mtd.get_serializer()
        self.assertEqual(self.event_type, mts.topic, 'topic mismatch')
        self.assertEqual(mts.sha256_id, mtd.sha256_id, 'sha256 mismatch')
    
    def test_serde_roundtrip(self):
        # prelims -- reg schema, instantiate mts
        resp = self.register_schema(self.event_type, self.schema_str)
        self.abort_diff_status(resp, 201)
        meta = SchemaHeaderBot.extract_metadata(resp)
        mts = MTSerializer(sha256_id=meta.sha256_id, tasr_url=self.url_prefix,
                           tasr_app=self.tasr_app)
        # create a dict to serialize, then pass it to mts
        gold_event = self.minimal_gold_event_dict()
        serialized_event = mts.serialize_event(gold_event)
        logging.info('encoded event: %s', serialized_event)
        
        mtd = mts.get_deserializer()
        deserialized_event_dict = mtd.deserialize_event(serialized_event)
        # Serialization fills in default field vals, so don't assert equal
        self.assertDictContainsSubset(gold_event, deserialized_event_dict,
                                      'serde round trip corrupted fields')

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
