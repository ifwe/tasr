'''
Created on Jul 26, 2017

When the redis our ASR is attached to is a slave, we need to handle requests
that require write access gracefully. This plugin disables PUT, POST and DELETE
methods at the route level, and wraps other route calls to return raised
SlaveModExceptions in a more transparent way.

@author: cmills
'''

import logging
from tasr.app.wsgi import TASRApp
from tasr.repository import SlaveModException


class SlaveBlockPlugin(object):
    name = 'slaveblock'
    api = 2

    def __init__(self):
        pass

    def setup(self, app):
        pass

    def apply(self, callback, route):
        if (isinstance(route.app, TASRApp) and route.app.ASR.is_slave() and
                route.method in ('PUT', 'DELETE')):
            # these methods must be able to modify redis, so they should
            # always fail if the connected redis instance is a slave
            def wrapper(*args, **kwargs):  # pylint: disable=W0613
                msg = 'Attached redis is a slave. Route disabled.'
                logging.error(msg)
                route.app.abort(code=500, text=msg, verbose=True)
            return wrapper
        else:
            # in all other cases, we only have to look out for a raised SME
            def wrapper(*args, **kwargs):
                try:
                    rv = callback(*args, **kwargs)
                except SlaveModException as sme:
                    route.app.abort(code=500, text=str(sme), verbose=True)
                return rv
            return wrapper
