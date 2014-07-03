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


class SchemaMetadata(object):
    '''A structured place to hold schema-related metadata.  This is a helper
    class for the RegisteredSchema class.  Note that this class does not hold
    an actual schema, just metadata about a schema.  That means the SHA256 and
    MD5 based IDs are snapshots, not calculated live.
    '''
    def __init__(self, meta_dict=None):
        self.sha256_id = None
        self.md5_id = None
        self.tv_dict = dict()
        self.ts_dict = dict()
        if meta_dict:
            self.update_from_dict(meta_dict)

    def update_from_dict(self, meta_dict):
        '''Set the metadata values from a dict.'''
        if not meta_dict:
            return
        if 'sha256_id' in meta_dict:
            self.sha256_id = meta_dict['sha256_id']
        if 'md5_id' in meta_dict:
            self.sha256_id = meta_dict['md5_id']
        for key, val in meta_dict.iteritems():
            if key.startswith('topic.'):
                try:
                    topic = key[6:]
                    version = int(val)
                    self.tv_dict[topic] = version
                except ValueError:
                    pass
            if key.startswith('topic_ts.'):
                try:
                    topic = key[9:]
                    timestamp = long(val)
                    self.ts_dict[topic] = timestamp
                except ValueError:
                    pass

    def as_dict(self):
        '''Encapsulate the object values in a dict.'''
        meta_dict = dict()
        meta_dict['sha256_id'] = self.sha256_id
        meta_dict['md5_id'] = self.md5_id
        for key, value in self.tv_dict.iteritems():
            topic_key = 'topic.%s' % key
            meta_dict[topic_key] = value
        for key, value in self.ts_dict.iteritems():
            topic_key = 'topic_ts.%s' % key
            meta_dict[topic_key] = value
        return meta_dict

    @property
    def topics(self):
        '''Access the topic list as a property.
        '''
        return self.tv_dict.keys()


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
        '''A dict containing a schema and topic-version and topic-timestamp
        entries can be used to update the RS fields.  Note that even if the
        dict contains 'sha256_id' and 'md5_id' fields, they will be ignored
        as the RS only exposes those as live values calculated from the schema.
        '''
        if rs_dict:
            self.schema_str = rs_dict.pop('schema', self.schema_str)
            self.update_dicts_from_schema_metadata(SchemaMetadata(rs_dict))

    def update_dicts_from_schema_metadata(self, metadata):
        '''Updates the topic-version and topic-timestamp fields in the RS
        object based on tv_dict and ts_dict passed in a SchemaMetadata object.
        '''
        if metadata:
            self.tv_dict.update(metadata.tv_dict)
            self.ts_dict.update(metadata.ts_dict)

    def as_schema_metadata(self):
        '''Creates a new SchemaMetadata object that contains a snapshot of the
        RS object's metadata (IDs, tv_dict and ts_dict).
        '''
        metadata = SchemaMetadata()
        metadata.sha256_id = self.sha256_id
        metadata.md5_id = self.md5_id
        metadata.tv_dict = self.tv_dict.copy()
        metadata.ts_dict = self.ts_dict.copy()
        return metadata

    def as_dict(self):
        '''Outputs the object as a dict.'''
        rs_dict = dict()
        rs_dict.update(self.as_schema_metadata().as_dict())
        rs_dict['schema'] = self.canonical_schema_str
        # overwrite the SHA256 and MD5 IDs with ones derived from the schema
        rs_dict['sha256_id'] = 'id.%s' % self.sha256_id
        rs_dict['md5_id'] = 'id.%s' % self.md5_id
        return rs_dict

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
