'''
Created on May 7, 2014

@author: cmills
'''

from tasr_test import TASRTestCase

import tasr.app
import requests
import logging
from requests.packages.urllib3._collections import HTTPHeaderDict
from requests.packages.urllib3.response import HTTPResponse
import httmock
from webtest import TestApp, TestRequest

APP = tasr.app.TASR_APP
APP.set_config_mode('local')
HOST_PORT = r'%s:%s' % (APP.config.host, APP.config.port)


class TestTASRAppClient(TASRTestCase):
    '''
    This is a wrapper class to encapsulate the route_to_testapp method,
    allowing us to use both httmock (in the test) and requests (in the client).
    '''

    def setUp(self):
        self.app = APP
        self.tasr = TestApp(APP)

    @httmock.urlmatch(netloc=HOST_PORT)
    def route_to_testapp(self, url, requests_req):
        '''This is some tricky stuff.  To test the client methods, we need the
        responses package calls to route to our webtest TestApp WSGI wrapper.
        We use httmock to intercept the requests call, then we handle
        processing in this function instead -- calling the TestApp wrapper.
        '''
        if url.geturl() != requests_req.url:
            logging.warn('%s != %s', url.geturl(), requests_req.url)
        # create a webtest TestRequest from the requests PreparedRequest
        webtest_req = TestRequest.blank(requests_req.url,
                                        method=requests_req.method,
                                        body=requests_req.body,
                                        headers=requests_req.headers)

        # have the TestApp wrapper process the TestRequest
        webtest_resp = self.tasr.request(webtest_req)

        '''webtest responses support multiple headers with the same key, while
        the requests package holds them in a case-insensitive dict of lists of
        (key,value) tuples.  We need to translate by hand here to keep cases
        with multiple headers with the same key
        '''
        headers = HTTPHeaderDict()
        for key, value in webtest_resp.headers.iteritems():
            headers.add(key, value)

        # use the webtest TestResponse to build a new requests HTTPResponse
        requests_http_resp = HTTPResponse(body=webtest_resp.body,
                                           headers=headers,
                                           status=webtest_resp.status_code)

        # get an HTTPAdaptor, then use it to build the requests Response object
        adap = requests.adapters.HTTPAdapter()
        requests_resp = adap.build_response(requests_req, requests_http_resp)

        '''For some reason, we need to explicitly set the _content attribute
        after the response object is built -- it is already in there as
        raw.data, but it doesn't make it to _content, so it never hits
        content() without this intervention.
        '''
        requests_resp._content = webtest_resp.body
        return requests_resp
