'''
Created on July 21, 2014

@author: cmills

The TASRConfig pulls config data from a common properties-style config file.
'''

from ConfigParser import SafeConfigParser
import os

CONF_FNAME = 'tasr.cfg'
CONF_SUBDIR = 'conf'
CONF_SYS_DIR = '/etc'
CONF_PATH = None
CONFIG = None

# look for a config file in some standard places to set the config path
if os.path.exists(CONF_FNAME):
    # check working dir for conf file
    CONF_PATH = CONF_FNAME
elif os.path.exists('%s/%s' % (CONF_SUBDIR, CONF_FNAME)):
    # check conf subdir of the working dir for conf file
    CONF_PATH = '%s/%s' % (CONF_SUBDIR, CONF_FNAME)
else:
    # check in the system conf dir for conf file
    CONF_PATH = '%s/%s' % (CONF_SYS_DIR, CONF_FNAME)


class TASRConfig(object):
    '''Contains the TASR config details.'''
    def __init__(self, cfile_path, mode=None):
        self.cfile_path = cfile_path
        self.config = SafeConfigParser()
        self.read_config()
        self.mode = None
        self.set_mode(mode)

    def read_config(self):
        '''read in the config file'''
        self.config.read(self.cfile_path)

    def set_mode(self, mode):
        '''The mode is set by setting the section of the config file to use as
        overrides for the defaults.'''
        if mode and self.config.has_section(mode):
            self.mode = mode
        else:
            self.mode = 'standard'

    def _get_str_or_none(self, key):
        if key in self.config.options(self.mode):
            val_str = self.config.get(self.mode, key).strip()
            if len(val_str) > 0:
                return val_str
        return None

    def _get_int_or_none(self, key):
        val_str = self._get_str_or_none(key)
        return int(val_str) if val_str else None

    @property
    def host(self):
        '''Gets the TASR host for the daemon.'''
        return self._get_str_or_none('host')

    @property
    def port(self):
        '''Gets the TASR port for the daemon.'''
        return self._get_int_or_none('port')

    @property
    def redis_host(self):
        '''Gets the Redis host for the daemon.'''
        return self._get_str_or_none('redis_host')

    @property
    def redis_port(self):
        '''Gets the Redis port for the daemon.'''
        return self._get_int_or_none('redis_port')

    @property
    def log_file(self):
        '''Gets the log file for the daemon.'''
        return self._get_str_or_none('log_file')

    @property
    def log_level(self):
        '''Gets the log level for the daemon.'''
        return self._get_str_or_none('log_level')


# On module import, look for an available config file and instantiate the obj.
CONFIG = TASRConfig(CONF_PATH)
