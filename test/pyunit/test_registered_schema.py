'''
Created on December 5, 2014

@author: cmills
'''

from tasr_test import TASRTestCase

import unittest
import logging
import json
from tasr.registered_schema import RegisteredAvroSchema, MasterAvroSchema

logging.basicConfig(level=logging.DEBUG)


class TestRegisteredAvroSchema(TASRTestCase):
    def setUp(self):
        self.event_type = "gold"
        fix_rel_path = "schemas/%s.avsc" % (self.event_type)
        self.avsc_file = TASRTestCase.get_fixture_file(fix_rel_path, "r")
        self.schema_str = self.avsc_file.read()
        self.schema_version = 0
        self.expect_sha256_id = 'IEAsvOGuZfJFblDinapW428TgEn19HQX/AWBKzDeIzCR'
        self.extra_field_dict = {"name": "gold__extra_field",
                                 "type": ["null", "string"],
                                 "default": None}

    def test_create(self):
        RegisteredAvroSchema()

    def test_set_schema_str(self):
        ras = RegisteredAvroSchema()
        ras.schema_str = self.schema_str
        self.assertEqual(self.expect_sha256_id, ras.sha256_id, 'unexpected ID')

    def test_compatible_with_self(self):
        '''A schema should always be back-compatible with itself.'''
        ras = RegisteredAvroSchema()
        ras.schema_str = self.schema_str
        self.assertTrue(MasterAvroSchema([ras, ]).is_compatible(ras),
                        'expected schema to be back-compatible with self')
        # and confirm that it works with the convenience method
        self.assertTrue(ras.back_compatible_with(ras),
                        'expected schema to be back-compatible with self')

    def test_compatible_with_nullable_field_added(self):
        '''Adding a nullable field (with default null) should be fine.'''
        ras = RegisteredAvroSchema()
        ras.schema_str = self.schema_str
        # create schema with extra field added
        jd = json.loads(self.schema_str)
        jd['fields'].append(self.extra_field_dict)
        new_ras = RegisteredAvroSchema()
        new_ras.schema_str = json.dumps(jd)
        self.assertTrue(MasterAvroSchema([ras, ]).is_compatible(new_ras),
                        'expected new schema to be back-compatible')
        # and confirm that it works with the convenience method
        self.assertTrue(new_ras.back_compatible_with(ras),
                        'expected new schema to be back-compatible')

    def test_compatible_with_nullable_field_removed(self):
        '''Removing a nullable field (with default null) should be fine.'''
        ras = RegisteredAvroSchema()
        ras.schema_str = self.schema_str
        # create schema with extra field added
        jd = json.loads(self.schema_str)
        jd['fields'].append(self.extra_field_dict)
        new_ras = RegisteredAvroSchema()
        new_ras.schema_str = json.dumps(jd)
        self.assertTrue(MasterAvroSchema([new_ras, ]).is_compatible(ras),
                        'expected schema to be back-compatible')
        # and confirm that it works with the convenience method
        self.assertTrue(ras.back_compatible_with(new_ras),
                        'expected schema to be back-compatible')

    def test_not_compatible_with_non_nullable_field_added(self):
        '''Adding a non-nullable field is not allowed.'''
        ras = RegisteredAvroSchema()
        ras.schema_str = self.schema_str
        # create schema with extra field added
        jd = json.loads(self.schema_str)
        non_nullable_field_dict = {"name": "gold__extra_field",
                                   "type": "string"}
        jd['fields'].append(non_nullable_field_dict)
        new_ras = RegisteredAvroSchema()
        new_ras.schema_str = json.dumps(jd)
        self.assertFalse(MasterAvroSchema([ras, ]).is_compatible(new_ras),
                         'expected new schema to NOT be back-compatible')
        # and confirm that it works with the convenience method
        self.assertFalse(new_ras.back_compatible_with(ras),
                         'expected new schema to be back-compatible')

    def test_compatible_with_required_field_made_null_then_removed(self):
        '''Converting a required field to a nullable one is fine, and so is
        removing a nullable field.  We test the whole sequence here.'''
        ras = RegisteredAvroSchema()
        ras.schema_str = self.schema_str
        # create first schema with extra, non-nullable field added
        jd = json.loads(self.schema_str)
        non_nullable_field_dict = {"name": "gold__extra_field",
                                   "type": "string"}
        jd['fields'].append(non_nullable_field_dict)
        first_ras = RegisteredAvroSchema()
        first_ras.schema_str = json.dumps(jd)
        # now create second schema, with the extra field made nullable
        jd2 = json.loads(self.schema_str)
        jd2['fields'].append(self.extra_field_dict)
        second_ras = RegisteredAvroSchema()
        second_ras.schema_str = json.dumps(jd2)
        # the base ras is the third (newest) schema in the sequence
        mas = MasterAvroSchema([first_ras, second_ras])
        self.assertTrue(mas.is_compatible(ras),
                        'expected new schema to be back-compatible')
        # and confirm that it works with the convenience method
        self.assertTrue(ras.back_compatible_with([first_ras, second_ras]),
                        'expected new schema to be back-compatible')

        # make sure that reversing the order of the first and second fails
        try:
            MasterAvroSchema([second_ras, first_ras])
            self.fail('should have raised a ValueError as this order is bad')
        except ValueError:
            pass
        # ensure that using the convenience method avoidf the raise
        self.assertFalse(ras.back_compatible_with([second_ras, first_ras]),
                         'expected new schema to NOT be back-compatible')

    def test_compatible_with_non_nullable_field_removed(self):
        '''Removing a non-nullable field is OK -- treated as converting it to a
        nullable field with a default null, then removing that field.'''
        ras = RegisteredAvroSchema()
        ras.schema_str = self.schema_str
        # create schema with extra field added
        jd = json.loads(self.schema_str)
        non_nullable_field_dict = {"name": "gold__extra_field",
                                   "type": "string"}
        jd['fields'].append(non_nullable_field_dict)
        new_ras = RegisteredAvroSchema()
        new_ras.schema_str = json.dumps(jd)
        self.assertTrue(MasterAvroSchema([new_ras, ]).is_compatible(ras),
                        'expected schema to be back-compatible')
        self.assertTrue(ras.back_compatible_with(new_ras),
                        'expected schema to be back-compatible')
        # make sure the reverse order fails
        self.assertFalse(MasterAvroSchema([ras, ]).is_compatible(new_ras),
                         'expected schema to NOT be back-compatible')
        self.assertFalse(new_ras.back_compatible_with(ras),
                         'expected schema to NOT be back-compatible')

    def test_not_compatible_with_field_type_change(self):
        '''Changing the type of a field is not allowed.'''
        # create schema with a string field added
        jd = json.loads(self.schema_str)
        string_field_dict = {"name": "gold__extra_field",
                             "type": "string"}
        jd['fields'].append(string_field_dict)
        str_ras = RegisteredAvroSchema()
        str_ras.schema_str = json.dumps(jd)
        # create a new schema where the field is an int type
        jd2 = json.loads(self.schema_str)
        int_field_dict = {"name": "gold__extra_field",
                          "type": "int"}
        jd2['fields'].append(int_field_dict)
        int_ras = RegisteredAvroSchema()
        int_ras.schema_str = json.dumps(jd2)
        self.assertFalse(MasterAvroSchema([str_ras, ]).is_compatible(int_ras),
                         'expected schema to NOT be back-compatible')
        self.assertFalse(int_ras.back_compatible_with(str_ras),
                         'expected schema to NOT be back-compatible')
        # and test the reverse case as well
        self.assertFalse(MasterAvroSchema([int_ras, ]).is_compatible(str_ras),
                         'expected schema to NOT be back-compatible')
        self.assertFalse(str_ras.back_compatible_with(int_ras),
                         'expected schema to NOT be back-compatible')

    def test_not_compatible_with_nullable_field_type_change(self):
        '''Changing the type of a field is not allowed.'''
        # create schema with a string field added
        jd = json.loads(self.schema_str)
        jd['fields'].append(self.extra_field_dict)  # type is [null, string]
        str_ras = RegisteredAvroSchema()
        str_ras.schema_str = json.dumps(jd)
        # create a new schema where the field is a nullable int type
        jd2 = json.loads(self.schema_str)
        int_field_dict = {"name": "gold__extra_field",
                          "type": ["null", "int"],
                          "default": None}
        jd2['fields'].append(int_field_dict)
        int_ras = RegisteredAvroSchema()
        int_ras.schema_str = json.dumps(jd2)
        self.assertFalse(MasterAvroSchema([str_ras, ]).is_compatible(int_ras),
                         'expected schema to NOT be back-compatible')
        self.assertFalse(int_ras.back_compatible_with(str_ras),
                         'expected schema to NOT be back-compatible')
        # and test the reverse case as well
        self.assertFalse(MasterAvroSchema([int_ras, ]).is_compatible(str_ras),
                         'expected schema to NOT be back-compatible')
        self.assertFalse(str_ras.back_compatible_with(int_ras),
                         'expected schema to NOT be back-compatible')


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestRegisteredAvroSchema)
    unittest.TextTestRunner(verbosity=2).run(suite)
