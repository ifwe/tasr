'''
Created on December 5, 2014

@author: cmills
'''

from tasr_test import TASRTestCase

import unittest
import logging
from tasr.registered_schema import RegisteredAvroSchema

logging.basicConfig(level=logging.DEBUG)

class TestRegisteredAvroSchema(TASRTestCase):

    def setUp(self):
        self.event_type = "gold"
        fix_rel_path = "schemas/%s.avsc" % (self.event_type)
        self.avsc_file = TASRTestCase.get_fixture_file(fix_rel_path, "r")
        self.schema_str = self.avsc_file.read()
        self.schema_version = 0
        self.expect_sha256_id = 'IOXz+ZvqY5zZmnEPTyOk9UCvuqeEhrp82jVEeOwznq4P'

    def test_create(self):
        RegisteredAvroSchema()

    def test_set_schema_str(self):
        ras = RegisteredAvroSchema()
        ras.schema_str = self.schema_str
        self.assertEqual(self.expect_sha256_id, ras.sha256_id, 'unexpected ID')

    def test_back_compatible_with(self):
        ras = RegisteredAvroSchema()
        ras.schema_str = self.schema_str
        field = '{"name": "gold__user_id", "type": "long"}'
        new_field = '{"name": "gold__user_id", "type": ["null", "long"], "default": null}'
        new_str = self.schema_str.replace(field, new_field)
        new_ras = RegisteredAvroSchema()
        new_ras.schema_str = new_str
        self.assertTrue(new_ras.back_compatible_with(ras),
                        'expected new schema to be back-compatible')
        self.assertFalse(ras.back_compatible_with(new_ras),
                        'expected old schema to NOT be back-compatible w/ new')


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestRegisteredAvroSchema)
    unittest.TextTestRunner(verbosity=2).run(suite)
