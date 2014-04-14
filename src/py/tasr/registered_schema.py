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
import avro.io
from tagtx.avro_tx import MD5_ID_TYPE, SHA256_ID_TYPE

class RegisteredSchema(object):
    def __init__(self, topic=None, schema=None, version=None):
        self.schema = schema
        self.topic = topic
        self.version = int(version)
        
    @property
    def cannonical_schema(self):
        return self.schema
    
    @property
    def md5_id(self):
        return self.md5_id_base64

    @property
    def md5_id_base64(self):
        return base64.b64encode(self.md5_id_bytes)
    
    @property
    def md5_id_hex(self):
        return binascii.hexlify(self.md5_id_bytes)
    
    @property
    def md5_id_bytes(self):
        _buf = io.BytesIO()
        _buf.write(struct.pack('>b', MD5_ID_TYPE))
        _md5 = hashlib.md5()
        _md5.update(self.cannonical_schema)
        _buf.write(_md5.digest())
        _id = _buf.getvalue()
        _buf.close()
        return _id

    @property
    def sha256_id(self):
        return self.sha256_id_base64

    @property
    def sha256_id_base64(self):
        return base64.b64encode(self.sha256_id_bytes)
    
    @property
    def sha256_id_hex(self):
        return binascii.hexlify(self.sha256_id_bytes)
    
    @property
    def sha256_id_bytes(self):
        _buf = io.BytesIO()
        _buf.write(struct.pack('>b', SHA256_ID_TYPE))
        _sha = hashlib.sha256()
        _sha.update(self.cannonical_schema)
        _buf.write(_sha.digest())
        _id = _buf.getvalue()
        _buf.close()
        return _id
    
    def validate_schema(self):
        return (self.cannonical_schema != None)
    
    def __repr__(self):
        return '%r' % self.cannonical_schema
    
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
    def __init__(self, topic=None, avro_schema=None, version=None):
        super(RegisteredAvroSchema, self).__init__(topic, avro_schema, version)
        if self.schema:
            self.validate_schema()

    def validate_schema(self):
        if not super(RegisteredAvroSchema, self).validate_schema():
            return False
        try:
            return True
        except:
            pass

            








