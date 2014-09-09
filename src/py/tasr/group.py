'''
Created on Jul 8, 2014

@author: cmills
'''

import re


class GroupMetadata(object):
    def __init__(self, name=None, version=None, timestamp=None):
        self.name = name
        self.current_version = version
        self.current_timestamp = timestamp
        self.current_sha256_id = None
        self.current_md5_id = None
        self.md5_id_list = None
        self.sha256_id_list = None


class Group(object):
    '''A wrapper for the topic/subject group used in TASR.  It needs more than
    a string when you add the config map (containing default field values) and
    a set of associated validator class names.  We may not use these initially,
    but we support storing them as part of compatibility with the Java Avro
    schema repository code.
    '''

    def __init__(self, group_name, config_dict=None, validators=None):
        '''The name is required, the config_dict and validators are not.'''
        if not Group.validate_group_name(group_name):
            raise ValueError('Invalid group name: \"%s\"' % group_name)
        self.name = group_name
        self.timestamp = None
        self.current_schema = None
        self.config = dict()
        if config_dict:
            self.config.update(config_dict)
        self.validators = set()
        if validators:
            self.validators.update(validators)

    @staticmethod
    def validate_group_name(subject):
        '''Group names (and topic names) must be composed of alphanumeric
        characters plus the underscore, with no whitespace.  This checks that
        this is the case.'''
        if re.match(r'^\w+$', subject):
            return True
        return False

    @property
    def current_version(self):
        if self.current_schema:
            return self.current_schema.current_version(self.name)