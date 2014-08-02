'''
Created on July 1, 2014

@author: cmills
'''

import sys
import os
TEST_DIR = os.path.abspath(os.path.dirname(__file__))
SRC_DIR = os.path.abspath(os.path.dirname('%s/../../src/py/tagged' % TEST_DIR))
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
