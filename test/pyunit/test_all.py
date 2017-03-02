'''
Created on Apr 8, 2014

@author: cmills
'''

from unittest import TestLoader, TextTestRunner
from test_tasr import TestTASR
from test_app_core import TestTASRCoreApp
from test_app_subject import TestTASRSubjectApp
from test_client_methods import TestTASRClientMethods
from test_client_object import TestTASRClientObject
from test_registered_schema import TestRegisteredAvroSchema
from test_redshift import TestTASRRedshift


if __name__ == "__main__":
    SUITE = TestLoader().loadTestsFromTestCase(TestTASR)
    SUITE = TestLoader().loadTestsFromTestCase(TestTASRCoreApp)
    SUITE = TestLoader().loadTestsFromTestCase(TestTASRSubjectApp)
    SUITE = TestLoader().loadTestsFromTestCase(TestTASRClientMethods)
    SUITE = TestLoader().loadTestsFromTestCase(TestTASRClientObject)
    SUITE = TestLoader().loadTestsFromTestCase(TestRegisteredAvroSchema)
    SUITE = TestLoader().loadTestsFromTestCase(TestTASRRedshift)
    TextTestRunner(verbosity=2).run(SUITE)
