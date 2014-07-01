'''
Created on Apr 2, 2014

@author: cmills
'''

import hashlib
import io
import struct
import base64
import binascii
import avro.schema

MD5_BYTES = 16
SHA256_BYTES = 32


class RegisteredSchema(object):
    '''The RegisteredSchema represents the data we have about how a given
    schema string is currently registered for known topics.  This object only
    holds the most recent topic-version intersections, so for the (unusual but
    allowed) case where a schema has been registered more than once for the
    same topic, only the most recent version will be included.  However, _all_
    topics for which the schema string has been registered are included, and
    must each indicate their most recent versions.

    The canonical schema string is a version with whitespace and other things
    that will not affect the parsing of the schema normalized.

    The IDs are derivative of the canonical schema string, so they are surfaced
    with @property methods.
    '''
    def __init__(self):
        self.schema_str = None
        self.tv_dict = dict()
        self.ts_dict = dict()

    def update_from_dict(self, rs_dict):
        '''Updates the non-derived fields in the object with those in the
        passed in dict.
        '''
        if not rs_dict:
            return
        # these are derivative values in this object
        rs_dict.pop('sha256_id')
        rs_dict.pop('md5_id')

        self.schema_str = rs_dict.pop('schema', self.schema_str)

        # the topic and version may not be the most recent intersection
        # the tv_dict holds only the most recent topic/version intersections
        # the ts_dict holds the timestamps for those t/v intersections
        for k, val in rs_dict.iteritems():
            if k.startswith('topic.'):
                try:
                    topic = k[6:]
                    version = int(val)
                    self.tv_dict[topic] = version
                except ValueError:
                    pass
            if k.startswith('topic_ts.'):
                try:
                    topic = k[9:]
                    timestamp = long(val)
                    self.ts_dict[topic] = timestamp
                except ValueError:
                    pass

    def as_dict(self):
        '''Outputs the object as a dict.
        '''
        rsd = self.tv_dict.copy()
        rsd.update(self.ts_dict)
        # add the canonical version of the schema string
        rsd['schema'] = self.canonical_schema_str
        # add the ids -- using the 'id.' prefixes
        rsd['sha256_id'] = 'id.%s' % self.sha256_id
        rsd['md5_id'] = 'id.%s' % self.md5_id
        return rsd

    @property
    def canonical_schema_str(self):
        '''Not much normalization as of yet...
        '''
        return self.schema_str

    @property
    def md5_id(self):
        '''Access the (base64'd) md5 as a property.
        '''
        return self.md5_id_base64

    @property
    def md5_id_base64(self):
        '''Access the base64'd md5 as a property.
        '''
        if self.canonical_schema_str == None:
            return None
        return base64.b64encode(self.md5_id_bytes)

    @property
    def md5_id_hex(self):
        '''Access the hex md5 as a property.
        '''
        if self.canonical_schema_str == None:
            return None
        return binascii.hexlify(self.md5_id_bytes)

    @property
    def md5_id_bytes(self):
        '''Access the md5 bytes as a property.
        '''
        if self.canonical_schema_str == None:
            return None
        buf = io.BytesIO()
        buf.write(struct.pack('>b', MD5_BYTES))
        md5 = hashlib.md5()
        md5.update(self.canonical_schema_str)
        buf.write(md5.digest())
        id_bytes = buf.getvalue()
        buf.close()
        return id_bytes

    @property
    def sha256_id(self):
        '''Access the (base64'd) sha256 as a property.
        '''
        return self.sha256_id_base64

    @property
    def sha256_id_base64(self):
        '''Access the base64'd sha256 as a property.
        '''
        if self.canonical_schema_str == None:
            return None
        return base64.b64encode(self.sha256_id_bytes)

    @property
    def sha256_id_hex(self):
        '''Access the hex sha256 as a property.
        '''
        if self.canonical_schema_str == None:
            return None
        return binascii.hexlify(self.sha256_id_bytes)

    @property
    def sha256_id_bytes(self):
        '''Access the sha256 bytes as a property.
        '''
        if self.canonical_schema_str == None:
            return None
        buf = io.BytesIO()
        buf.write(struct.pack('>b', SHA256_BYTES))
        sha = hashlib.sha256()
        sha.update(self.canonical_schema_str)
        buf.write(sha.digest())
        id_bytes = buf.getvalue()
        buf.close()
        return id_bytes

    @property
    def topics(self):
        '''Access the topic list as a property.
        '''
        return self.tv_dict.keys()

    @property
    def is_valid(self):
        '''Access the schema validity as a boolean property.
        '''
        try:
            return self.validate_schema_str()
        except avro.schema.SchemaParseException:
            return False
        except:
            return False

    def validate_schema_str(self):
        '''The retrieval of the canonical str should do a validation.  So, if
        it comes back as None, it is either missing or bad.
        '''
        return self.canonical_schema_str != None

    def current_version(self, topic):
        '''A convenience method to get the current version for a topic
        associated with the schema.
        '''
        if topic in self.tv_dict:
            return self.tv_dict[topic]
        return None

    def current_version_timestamp(self, topic):
        '''A convenience method to get the timestamp for when a topic was
        associated with the schema.
        '''
        if topic in self.ts_dict:
            return self.ts_dict[topic]
        return None

    def __repr__(self):
        return '%r' % self.canonical_schema_str

    def __str__(self):
        return u'%s[%s, %s]' % (self.__class__.__name__,
                                self.sha256_id, self.tv_dict)

    def __eq__(self, other):
        '''Registered schemas are equal when the underlying canonical schema
        strings (and hence the SHA256 and or MD5 ids) are equal AND the topic/
        version mappings are the same.
        '''
        if not isinstance(other, RegisteredSchema):
            return False

        if not self.sha256_id == other.sha256_id:
            return False

        shared_set = set(self.tv_dict.items()) & set(other.tv_dict.items())
        if len(self.tv_dict) == len(shared_set):
            return True
        return False


class RegisteredAvroSchema(RegisteredSchema):
    '''Adds an Avro schema validation function.
    '''
    def __init__(self):
        super(RegisteredAvroSchema, self).__init__()

    def validate_schema_str(self):
        if not super(RegisteredAvroSchema, self).validate_schema_str():
            return False

        # a parse exception should bubble up, so don't catch it here
        avro.schema.parse(self.canonical_schema_str)

        # add additional checks?
        return True
