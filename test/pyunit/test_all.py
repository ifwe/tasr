'''
Created on Apr 8, 2014

@author: cmills
'''

from unittest import TestLoader, TextTestRunner
from test_tasr import TestTASR
from test_app import TestTASRAppNativeAPI
from test_app_sv import TestTASRAppSVAPI
from test_client_methods import TestTASRClientMethods
from test_client_object import TestTASRClientObject
from test_client_sv_methods import TestTASRClientSVMethods
from test_client_sv_object import TestTASRClientSVObject


if __name__ == "__main__":
    suite = TestLoader().loadTestsFromTestCase(TestTASR)
    suite = TestLoader().loadTestsFromTestCase(TestTASRAppNativeAPI)
    suite = TestLoader().loadTestsFromTestCase(TestTASRAppSVAPI)
    suite = TestLoader().loadTestsFromTestCase(TestTASRClientMethods)
    suite = TestLoader().loadTestsFromTestCase(TestTASRClientObject)
    suite = TestLoader().loadTestsFromTestCase(TestTASRClientSVMethods)
    suite = TestLoader().loadTestsFromTestCase(TestTASRClientSVObject)
    TextTestRunner(verbosity=2).run(suite)
