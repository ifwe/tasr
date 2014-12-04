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


if __name__ == "__main__":
    suite = TestLoader().loadTestsFromTestCase(TestTASR)
    suite = TestLoader().loadTestsFromTestCase(TestTASRTopicApp)
    suite = TestLoader().loadTestsFromTestCase(TestTASRCoreApp)
    suite = TestLoader().loadTestsFromTestCase(TestTASRSubjectApp)
    suite = TestLoader().loadTestsFromTestCase(TestTASRClientMethods)
    suite = TestLoader().loadTestsFromTestCase(TestTASRClientObject)
    suite = TestLoader().loadTestsFromTestCase(TestTASRLegacyClientMethods)
    suite = TestLoader().loadTestsFromTestCase(TestTASRLegacyClientObject)
    TextTestRunner(verbosity=2).run(suite)
