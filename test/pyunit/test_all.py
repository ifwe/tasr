'''
Created on Apr 8, 2014

@author: cmills
'''

from unittest import TestLoader, TextTestRunner
from test_tasr import TestTASR
from test_app_topic import TestTASRTopicApp
from test_app_core import TestTASRCoreApp
from test_app_subject import TestTASRSubjectApp
from test_client_legacy_methods import TestTASRLegacyClientMethods
from test_client_legacy_object import TestTASRLegacyClientObject
from test_client_methods import TestTASRClientMethods
from test_client_object import TestTASRClientObject
from test_registered_schema import TestRegisteredAvroSchema


if __name__ == "__main__":
    SUITE = TestLoader().loadTestsFromTestCase(TestTASR)
    SUITE = TestLoader().loadTestsFromTestCase(TestTASRTopicApp)
    SUITE = TestLoader().loadTestsFromTestCase(TestTASRCoreApp)
    SUITE = TestLoader().loadTestsFromTestCase(TestTASRSubjectApp)
    SUITE = TestLoader().loadTestsFromTestCase(TestTASRClientMethods)
    SUITE = TestLoader().loadTestsFromTestCase(TestTASRClientObject)
    SUITE = TestLoader().loadTestsFromTestCase(TestTASRLegacyClientMethods)
    SUITE = TestLoader().loadTestsFromTestCase(TestTASRLegacyClientObject)
    SUITE = TestLoader().loadTestsFromTestCase(TestRegisteredAvroSchema)
    TextTestRunner(verbosity=2).run(SUITE)
