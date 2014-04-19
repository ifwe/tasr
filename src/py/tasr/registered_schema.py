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
    '''The RegisteredSchema represents the data we have about how a given schema 
    string has been associated with topic-version intersections.  That is, a 
    schema may be registered for more than one topic, and this object should 
    know about all those associations.  The most recent intersection for each 
    topic should be in the tv_dict.  
    
    The specific attributes of topic and version are there to clarify the topic 
    and version specific to a context where that applies -- and are NOT used for
    determining equality.  If, for instance, a schema is registered for topic T 
    twice (versions 2 and 4), and the schema for T version 2 is requested, the 
    tv_dict would list 4 for T (the most recent), while the version attribute 
    would be 2 (specific to the request context).
    
    The canonical schema string is a version with whitespace and other things 
    that will not affect the parsing of the schema normalized.
    
    The IDs are derivative of the canonical schema string, so they are surfaced 
    with @property methods.
    '''
    def __init__(self, schema_str=None, topic=None, version=None, tv_dict=None):
        if not tv_dict:
            tv_dict = dict()

        self.topic = topic
        self.version = None
        if version:
            try:
                self.version = int(version)
            except:
                pass
        self.tv_dict = tv_dict
        self.schema_str = schema_str
        if self.schema_str != None:
            self.validate_schema_str()

    def update_from_dict(self, rs_dict):
        if not rs_dict:
            return
        # these are derivative values in this object
        _sha256 = rs_dict.pop('sha256_id')
        _md5 = rs_dict.pop('md5_id')
        # if the canonical version returned differs, store that
        self.schema_str = rs_dict.pop('schema', self.schema_str)
        # the topic and version may not be the most recent intersection
        self.topic = rs_dict.pop('topic', self.topic)
        _v = rs_dict.pop('version', self.version)
        if _v:
            self.version = int(_v)
        # the tv_dict holds the most recent tv intersections
        for _k, _v in rs_dict.iteritems():
            if _k.startswith('topic.'):
                try:
                    _topic = _k[6:]
                    _version = int(_v)
                    self.tv_dict[_topic] = _version
                except:
                    pass

    def as_dict(self):
        _d = self.tv_dict.copy()
        # add the specified topic and version
        #if self.topic:
        #    _d['topic'] = self.topic
        #if self.version:
        #    _d['version'] = self.version
        # add the canonical version of the schema string
        _d['schema'] = self.canonical_schema_str
        # add the ids -- using the 'id.' prefixes
        _d['sha256_id'] = 'id.%s' % self.sha256_id
        _d['md5_id'] = 'id.%s' % self.md5_id
        return _d
        
    @property
    def canonical_schema_str(self):
        '''Not much normalization as of yet...
        '''
        return self.schema_str
    
    @property
    def md5_id(self):
        return self.md5_id_base64

    @property
    def md5_id_base64(self):
        if self.canonical_schema_str == None:
            return None
        return base64.b64encode(self.md5_id_bytes)
    
    @property
    def md5_id_hex(self):
        if self.canonical_schema_str == None:
            return None
        return binascii.hexlify(self.md5_id_bytes)
    
    @property
    def md5_id_bytes(self):
        if self.canonical_schema_str == None:
            return None
        _buf = io.BytesIO()
        _buf.write(struct.pack('>b', MD5_BYTES))
        _md5 = hashlib.md5()
        _md5.update(self.canonical_schema_str)
        _buf.write(_md5.digest())
        _id = _buf.getvalue()
        _buf.close()
        return _id

    @property
    def sha256_id(self):
        return self.sha256_id_base64

    @property
    def sha256_id_base64(self):
        if self.canonical_schema_str == None:
            return None
        return base64.b64encode(self.sha256_id_bytes)
    
    @property
    def sha256_id_hex(self):
        if self.canonical_schema_str == None:
            return None
        return binascii.hexlify(self.sha256_id_bytes)
    
    @property
    def sha256_id_bytes(self):
        if self.canonical_schema_str == None:
            return None
        _buf = io.BytesIO()
        _buf.write(struct.pack('>b', SHA256_BYTES))
        _sha = hashlib.sha256()
        _sha.update(self.canonical_schema_str)
        _buf.write(_sha.digest())
        _id = _buf.getvalue()
        _buf.close()
        return _id
    
    @property
    def topics(self):
        return self.tv_dict.keys()
    
    @property
    def is_valid(self):
        try:
            return self.validate_schema_str()
        except avro.schema.SchemaParseException:
            return False
        except:
            return False
    
    def validate_schema_str(self):
        return (self.canonical_schema_str != None)
    
    def current_version(self, topic):
        if self.tv_dict.has_key(topic):
            return self.tv_dict[topic]
        return None
    
    def __repr__(self):
        return '%r' % self.canonical_schema_str
    
    def __str__(self):
        return u'%s[t=%s[%s], v=%s, md5=%s]' % (self.__class__.__name__,
                                                self.topic,
                                                self.serial, 
                                                self.version,
                                                self.md5_id)

    def __eq__(self, other):
        '''Registered schemas are equal when the underlying canonical schema
        strings (and hence the SHA256 and or MD5 ids) are equal AND the topic/
        version mappings are the same.  
        '''
        if not isinstance(other, RegisteredSchema):
            return False
        
        if not self.sha256_id == other.sha256_id:
            return False

        _shared_set = set(self.tv_dict.items()) & set(other.tv_dict.items())
        if len(self.tv_dict) == len(_shared_set):
            return True
        return False
    

class RegisteredAvroSchema(RegisteredSchema):
    def __init__(self, avro_schema_str=None, topic=None, version=None, tv_dict=None):
        super(RegisteredAvroSchema, self).__init__(avro_schema_str, topic, version, tv_dict)

    def validate_schema_str(self):
        if not super(RegisteredAvroSchema, self).validate_schema_str():
            return False

        # a parse exception should bubble up, so don't catch it here
        _schema = avro.schema.parse(self.canonical_schema_str)

        # add anything in addition to checking that the str parses as valid Avro
        return True
            








