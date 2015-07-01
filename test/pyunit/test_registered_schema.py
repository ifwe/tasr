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
        new_ras = RegisteredAvroSchema()
        new_ras.schema_str = self.get_schema_permutation(self.schema_str)
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
        new_ras = RegisteredAvroSchema()
        new_ras.schema_str = self.get_schema_permutation(self.schema_str)
        # we reverse the order, using "new" as the pre-existing schema
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
        non_nullable_field_dict = {"name": "gold__extra",
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
        non_nullable_field_dict = {"name": "gold__extra",
                                   "type": "string"}
        jd['fields'].append(non_nullable_field_dict)
        first_ras = RegisteredAvroSchema()
        first_ras.schema_str = json.dumps(jd)
        # now create second schema, with the extra field made nullable
        second_ras = RegisteredAvroSchema()
        second_ras.schema_str = self.get_schema_permutation(self.schema_str,
                                                            "gold__extra",
                                                            "string")
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
        non_nullable_field_dict = {"name": "gold__extra",
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
        string_field_dict = {"name": "gold__extra",
                             "type": "string"}
        jd['fields'].append(string_field_dict)
        str_ras = RegisteredAvroSchema()
        str_ras.schema_str = json.dumps(jd)
        # create a new schema where the field is an int type
        jd2 = json.loads(self.schema_str)
        int_field_dict = {"name": "gold__extra",
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
        str_ras = RegisteredAvroSchema()
        str_ras.schema_str = self.get_schema_permutation(self.schema_str,
                                                         "gold__extra",
                                                         "string")
        # create a new schema where the field is a nullable int type
        int_ras = RegisteredAvroSchema()
        int_ras.schema_str = self.get_schema_permutation(self.schema_str,
                                                         "gold__extra",
                                                         "int")
        self.assertFalse(MasterAvroSchema([str_ras, ]).is_compatible(int_ras),
                         'expected schema to NOT be back-compatible')
        self.assertFalse(int_ras.back_compatible_with(str_ras),
                         'expected schema to NOT be back-compatible')
        # and test the reverse case as well
        self.assertFalse(MasterAvroSchema([int_ras, ]).is_compatible(str_ras),
                         'expected schema to NOT be back-compatible')
        self.assertFalse(str_ras.back_compatible_with(int_ras),
                         'expected schema to NOT be back-compatible')

    def test_required_map_field_is_self_compatible(self):
        '''Check that schemas with a required map field type are compatible.'''
        jd = json.loads(self.schema_str)
        req_map_field_dict = {"name": "extra",
                          "type": "map",
                          "values": "string"}
        jd['fields'].append(req_map_field_dict)
        ras = RegisteredAvroSchema()
        ras.schema_str = json.dumps(jd)
        self.assertTrue(MasterAvroSchema([ras, ]).is_compatible(ras),
                        'expected schema to be back-compatible with self')
        # and confirm that it works with the convenience method
        self.assertTrue(ras.back_compatible_with(ras),
                        'expected schema to be back-compatible with self')

    def test_nullable_map_values_field_is_self_compatible(self):
        '''Check that schemas with a required map field type are compatible.'''
        jd = json.loads(self.schema_str)
        opt_values_map_field_dict = {"name": "extra",
                          "type": "map",
                          "values": ["null", "string"],
                          "default": None}
        jd['fields'].append(opt_values_map_field_dict)
        ras = RegisteredAvroSchema()
        ras.schema_str = json.dumps(jd)
        self.assertTrue(MasterAvroSchema([ras, ]).is_compatible(ras),
                        'expected schema to be back-compatible with self')
        # and confirm that it works with the convenience method
        self.assertTrue(ras.back_compatible_with(ras),
                        'expected schema to be back-compatible with self')

    def test_nullable_map_field_is_self_compatible(self):
        '''Check that schemas with a required map field type are compatible.'''
        jd = json.loads(self.schema_str)
        opt_map_field_dict = {"name": "extra",
                              "type": ["null",
                                       {"type": "map",
                                        "default": None,
                                        "values": ["null", "string"]
                                        }
                                       ],
                              "default": None}
        jd['fields'].append(opt_map_field_dict)
        ras = RegisteredAvroSchema()
        ras.schema_str = json.dumps(jd)
        self.assertTrue(MasterAvroSchema([ras, ]).is_compatible(ras),
                        'expected schema to be back-compatible with self')
        # and confirm that it works with the convenience method
        self.assertTrue(ras.back_compatible_with(ras),
                        'expected schema to be back-compatible with self')

    def test_req_to_opt_map_field_is_compatible(self):
        '''Check that schemas with a required map field type are compatible.'''
        req_jd = json.loads(self.schema_str)
        req_map_field_dict = {"name": "extra",
                          "type": "map",
                          "values": "string"}
        req_jd['fields'].append(req_map_field_dict)
        req_ras = RegisteredAvroSchema()
        req_ras.schema_str = json.dumps(req_jd)

        opt_jd = json.loads(self.schema_str)
        opt_map_field_dict = {"name": "extra",
                          "type": "map",
                          "values": ["null", "string"],
                          "default": None}
        opt_jd['fields'].append(opt_map_field_dict)
        opt_ras = RegisteredAvroSchema()
        opt_ras.schema_str = json.dumps(opt_jd)

        self.assertTrue(MasterAvroSchema([req_ras, ]).is_compatible(opt_ras),
                        'expected schema to be back-compatible with self')
        # and confirm that it works with the convenience method
        self.assertTrue(opt_ras.back_compatible_with(req_ras),
                        'expected schema to be back-compatible with self')


if __name__ == "__main__":
    LOADER = unittest.TestLoader()
    SUITE = LOADER.loadTestsFromTestCase(TestRegisteredAvroSchema)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
