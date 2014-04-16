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
    def __init__(self, topic=None, schema_str=None, version=None):
        self.topic = topic
        self.version = None
        try:
            self.version = int(version)
        except:
            pass
        self.schema_str = schema_str
        if self.schema_str != None:
            self.validate_schema_str()
        
    @property
    def cannonical_schema_str(self):
        return self.schema_str
    
    @property
    def md5_id(self):
        return self.md5_id_base64

    @property
    def md5_id_base64(self):
        if self.cannonical_schema_str == None:
            return None
        return base64.b64encode(self.md5_id_bytes)
    
    @property
    def md5_id_hex(self):
        if self.cannonical_schema_str == None:
            return None
        return binascii.hexlify(self.md5_id_bytes)
    
    @property
    def md5_id_bytes(self):
        if self.cannonical_schema_str == None:
            return None
        _buf = io.BytesIO()
        _buf.write(struct.pack('>b', MD5_BYTES))
        _md5 = hashlib.md5()
        _md5.update(self.cannonical_schema_str)
        _buf.write(_md5.digest())
        _id = _buf.getvalue()
        _buf.close()
        return _id

    @property
    def sha256_id(self):
        return self.sha256_id_base64

    @property
    def sha256_id_base64(self):
        if self.cannonical_schema_str == None:
            return None
        return base64.b64encode(self.sha256_id_bytes)
    
    @property
    def sha256_id_hex(self):
        if self.cannonical_schema_str == None:
            return None
        return binascii.hexlify(self.sha256_id_bytes)
    
    @property
    def sha256_id_bytes(self):
        if self.cannonical_schema_str == None:
            return None
        _buf = io.BytesIO()
        _buf.write(struct.pack('>b', SHA256_BYTES))
        _sha = hashlib.sha256()
        _sha.update(self.cannonical_schema_str)
        _buf.write(_sha.digest())
        _id = _buf.getvalue()
        _buf.close()
        return _id
    
    def validate_schema_str(self):
        return (self.cannonical_schema_str != None)
    
    @property
    def is_valid(self):
        try:
            return self.validate_schema_str()
        except avro.schema.SchemaParseException:
            return False
        except:
            return False
    
    def __repr__(self):
        return '%r' % self.cannonical_schema_str
    
    def __str__(self):
        return u'RegisteredSchema[t=%s, v=%s, md5=%s]' % (self.topic, 
                                                          self.version,
                                                          self.md5_id)

    def __eq__(self, other):
        if not isinstance(other, RegisteredSchema):
            return False
        if not self.topic == other.topic:
            return False
        if not self.version == other.version:
            return False
        if self.md5_id == other.md5_id:
            return True
        return False
    

class RegisteredAvroSchema(RegisteredSchema):
    def __init__(self, topic=None, avro_schema_str=None, version=None):
        super(RegisteredAvroSchema, self).__init__(topic, avro_schema_str, version)

    def validate_schema_str(self):
        if not super(RegisteredAvroSchema, self).validate_schema_str():
            return False

        # a parse exception should bubble up, so don't catch it here
        _schema = avro.schema.parse(self.cannonical_schema_str)

        # add anything in addition to checking that the str parses as valid Avro
        return True

    def __str__(self):
        return u'RegisteredAvroSchema[t=%s, v=%s, md5=%s]' % (self.topic, 
                                                              self.version,
                                                              self.md5_id)
            








