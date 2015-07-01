'''
Created on Apr 14, 2014

@author: cmills

The TASRApp is a WSGI (web server gateway interface) web app supporting a REST
API for the Tagged Avro Schema Repository (TASR).  It is implemented in Bottle,
so it can be run as a stand-alone process (for development), deployed under a
python package that implements a faster WSGI-compliant web server (gunicorn,
FAPWS, tornado), or deployed under a WSGI plugin on a fast, general purpose web
server (e.g. -- nginx + uWSGI or Apache HTTPd + mod_wsgi).

Configuration is pulled from a 'tasr.cfg' file.  This app expects that file to
be in one of three places.  It will check, in order, the execution directory,
a 'conf' subdir of the execution directory, and /etc.  If it cannot find a
tasr.cfg file, it will send an error to stderr and exit.

This module is set up to handle the CLI arg processing required when launching
the app using Bottle's built-in WSGI server.  Used in this way, the config file
values can be overridden with command line args.  This can be helpful during
development.

Running in stand-alone mode
---------------------------
For dev, it's much easier to rely on Bottle's built-in WSGI server, usually
called from the command line.  The standard config values are probably not what
you want for dev.  The common dev settings are included in the 'local' config
mode.  So, if you have Redis running on localhost:5379 and want TASR running on
localhost:8080, just fire this off in the project root directory:

    python src/py/app_standalone.py --env local

'''
import sys
import argparse
import socket
import logging
from tasr.tasr_config import CONFIG
from tasr.app import TASR_APP

ENV = 'standard'
CONFIG.set_mode(ENV)

ARG_PARSER = argparse.ArgumentParser()
ARG_PARSER.add_argument('--debug', action='store_true')
ARG_PARSER.add_argument('--env', default=ENV)
ARG_PARSER.add_argument('--host', default=None)
ARG_PARSER.add_argument('--port', type=int, default=None)
ARG_PARSER.add_argument('--redis_host', default=None)
ARG_PARSER.add_argument('--redis_port', type=int, default=None)
ARGS = ARG_PARSER.parse_args()

CONFIG.set_mode(ARGS.env)
HOST = ARGS.host if ARGS.host else CONFIG.host
PORT = ARGS.port if ARGS.port else CONFIG.port
RHOST = ARGS.redis_host if ARGS.redis_host else CONFIG.redis_host
RPORT = ARGS.redis_port if ARGS.redis_port else CONFIG.redis_port
LOGFILE = CONFIG.log_file
LOGLEVEL = 'DEBUG' if ARGS.debug else CONFIG.log_level

try:
    logging.basicConfig(filename=LOGFILE, level=LOGLEVEL)
    logging.debug("Logging to %s at %s.", LOGFILE, LOGLEVEL)
except IOError:
    sys.stderr.write("Cannot write logs to %s.\n" % LOGFILE)
    sys.exit(1)


def main():
    '''Run the app in bottle's built-in WSGI container.'''
    sys.stdout.write('TASR ARGS [%s:%s, redis: [%s:%s] ] starting up...\n' %
                     (HOST, PORT, RHOST, RPORT))
    sys.stdout.flush()
    try:
        logging.info("Starting TASR_APP...")
        TASR_APP.set_config_mode(ARGS.env)
        TASR_APP.run(host=HOST, port=PORT)
    except socket.error:
        sys.stderr.write('Could not open %s:%s.\n' % (HOST, PORT))


if __name__ == "__main__":
    main()
