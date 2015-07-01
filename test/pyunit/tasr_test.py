'''
Created on July 1, 2014

@author: cmills
'''

import sys
import os
import json
TEST_DIR = os.path.abspath(os.path.dirname(__file__))
SRC_DIR = os.path.abspath(os.path.dirname('%s/../../src/py/' % TEST_DIR))
sys.path.insert(0, os.path.join(TEST_DIR, SRC_DIR))
FIX_DIR = os.path.abspath(os.path.dirname("%s/../fixtures/" % TEST_DIR))

import unittest


class TASRTestCase(unittest.TestCase):
    '''These tests check that the TASR S+V REST API, expected by the Avro-1124
    repo code.  This does not check the TASR native API calls.
    '''
    test_dir = TEST_DIR
    src_dir = SRC_DIR
    fix_dir = FIX_DIR

    @staticmethod
    def get_fixture_file(rel_path, mode):
        path = '%s/%s' % (TASRTestCase.fix_dir, rel_path)
        return open(path, mode)

    @staticmethod
    def get_schema_permutation(schema_str, field_name=None, field_type=None):
        jd = json.loads(schema_str)
        field_name = "extra" if not field_name else field_name
        field_type = "string" if not field_type else field_type
        jd['fields'].append({"name": field_name, "type": ["null", field_type],
                             "default": None})
        return json.dumps(jd)
