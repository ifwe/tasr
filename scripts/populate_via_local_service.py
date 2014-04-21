'''
Created on Apr 21, 2014

@author: cmills
'''
import os
_script_dir = os.path.abspath(os.path.dirname(__file__))
_fix_dir = os.path.abspath(os.path.dirname("%s/../test/fixtures/" % _script_dir))

import logging
import requests

EVENT_TYPES = ["browse_click_tracking","envelope","gold","login","message",
               "newsfeed_clicks","page_view"]

TOPIC_PREFIX = 'http://localhost:8080/tasr/topic'

HEADERS = {'content-type': 'application/json; charset=utf8', }

def add_schema_for_event_type(event_type):
    _avsc_filename = "%s/schemas/%s.avsc" % (_fix_dir, event_type)
    _schema_str = open(_avsc_filename, "r").read()
    logging.info(u'Registering %s schema...' % event_type)
    _resp = requests.put("%s/%s" % (TOPIC_PREFIX, event_type),
                         headers=HEADERS, data=_schema_str)
    assert(200 == _resp.status_code)
    logging.debug('Success.')

def add_all_schemas():
    for _et in EVENT_TYPES:
        add_schema_for_event_type(_et)    
    
if __name__ == "__main__":
    add_all_schemas()
