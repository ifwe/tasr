'''
Created on Apr 2, 2014

@author: cmills
'''

class AbstractSchemaRepository(object):
    '''Any schema repository should support these four basic methods:
    
      - register a new schema for a topic
      - get the latest schema registered for a topic
      - get a specific schema (with a topic and a serial version id)
      - get a specific schema (with a hash-based id) 
    
    In each case the specifics of the registration process and the backing store
    are hidden from the client.
    
    Schemas are identified one of two ways -- either by a combination of topic
    and version (with versions starting with 1, and shorthand for the most 
    recent being -1), or by a hash-based id string.  The id strings are base64
    encoded byte arrays, with a 1-byte size header (indicating how many bytes
    will follow) and the id bytes themselves.  Those bytes are the digest of the
    canonical schema string using either the MD5 (16 bytes) or SHA256 (32 bytes) 
    hashes.
    
    What gets passed back in all cases is a RegisteredSchema object (or None).
    The RegisteredSchema has the canonical schema string as a primary attribute,
    with md5_id and sha256_id attributes automatically derived from the schema.
    The object also has a tv_dict attribute, which should hold the most recent
    version the schema represents for each topic it is associated with. 
    '''
    def __init__(self):
        pass
    
    def register(self, topic, schema_str):
        raise Exception(u'Abstract class method called.')
    
    def get_latest_for_topic(self, topic):
        raise Exception(u'Abstract class method called.')
    
    def get_for_topic_and_version(self, topic, version):
        raise Exception(u'Abstract class method called.')
    
    def get_for_id(self, id_base64=None, id_bytes=None, id_hex=None):
        raise Exception(u'Abstract class method called.')


import time
import logging
import redis
import base64
import binascii
import io
import struct
from registered_schema import RegisteredSchema

class RedisSchemaRepository(AbstractSchemaRepository):
    '''The Redis-based implementation of the AbstractSchemaRepository uses the 
    List and Hash structures provided by Redis as the backing store.  
    
    The primary store is a hash type, using a key in the form 'id.<sha256_id>'.
    The hash entry has, at a minimum, the following fields: sha256_id, md5_id, 
    and schema.  Additionally, for each topic the schema is registered for, the
    hash entry has a 'topic.<topic>' field that holds a version number.  The 
    version is the latest version for a topic, so if the same schema has been 
    registered more than once for a topic, the hash's entry only lists the last.
    
    There is a secondary hash entry used to provide an index from the md5_id
    values.  The key is in the form 'id.<md5_id>', and the hash entry only holds
    one field, 'sha256_id', with the key value you need to look up the primary 
    hash entry (that is, 'id.<sha256_id>').
    
    In addition to the hash entries, there are lists for each topic that has 
    registered schemas.  Each list entry is an SHA256 id key (i.e. -- 
    'id.<sha256_id>').  The version is the "counting from 1" index of the entry
    in the list.  So, the first entry in the list corresponds to version 1.
    These are lists, not sets, as it is possible for a schema to be registered, 
    then overridden, then reverted to -- in which case the same id key can occur 
    more than once in the list.
        
    Retrieval of schemas by SHA256 ID is simple, requiring a single Redis 
    operation.  However, retrieving by MD5 ID or, more commonly, by topic and 
    version, requires two operations.  It is much faster to execute both in a 
    single call, so we use the LUA script support in Redis to enable this.  This
    approach allows us to avoid the latency of a second round-trip to Redis.  
    '''
    def __init__(self, host='localhost', port=6379, db=0):
        super(RedisSchemaRepository, self).__init__()
        self.redis = redis.StrictRedis(host, port, db)
        # register lua scripts in Redis
        self.lua_get_for_md5 = self._register_lua_get_for_md5()
        self.lua_get_for_topic_and_version = self._register_lua_get_for_topic_and_version()

    # LUA script registrations
    def _register_lua_get_for_md5(self):
        _lua = '''
        local sha256_id = redis.call('hget', KEYS[1], 'sha256_id')
        return redis.call('hgetall', sha256_id)
        '''
        return self.redis.register_script(_lua)

    def _register_lua_get_for_topic_and_version(self):
        _lua = '''
        local sha256_id = redis.call('lindex', KEYS[1], KEYS[2])
        return redis.call('hgetall', sha256_id)
        '''
        return self.redis.register_script(_lua)

    # util methods    
    def _get_registered_schema(self):
        # Override in subclasses if a more specific RegisteredSchema class is used.
        return RegisteredSchema()
        
    def _hgetall_seq_2_dict(self, vlist):
        '''The HGETALL Redis command returns a "[<name0>, <value0>, <name1>, ...]
        list, which we want to turn into a dict in several cases.
        '''
        if not len(vlist) % 2 == 0:
            raise Exception('Bad value list.  Must have even number of values.')
        _idx = 0
        _rdict = dict()
        while _idx < len(vlist):
            _k = vlist[_idx]
            _idx += 1
            _v = vlist[_idx]
            _idx += 1
            _rdict[_k] = _v
        if len(_rdict) == 0:
            return None
        return _rdict
    
    def _get_for_sha256_id(self, sha256_base64_id):
        '''A low-level method to pull the hash struct identified by the passed 
        sha256 id and return it as a dict.
        '''
        _key = u'id.%s' % sha256_base64_id
        _dict = self.redis.hgetall(_key)
        if len(_dict) == 0:
            return None
        return _dict

    def _get_for_md5_id(self, md5_base64_id):
        '''A low-level method to pull the hash struct identified by the passed 
        md5 id, using a registered LUA script, and return it as a dict.
        '''
        _key = u'id.%s' % md5_base64_id
        _rvals = self.lua_get_for_md5(keys=[_key, ])
        return self._hgetall_seq_2_dict(_rvals)

    # exposed, API methods
    def register(self, topic, schema_str):
        '''
        '''
        _rs = self._get_registered_schema()
        _rs.schema_str = schema_str
        if not _rs.validate_schema_str():
            raise Exception(u'Cannot register invalid schema.')

        # the key values are what we use as Redis keys
        _sha256_key = u'id.%s' % _rs.sha256_id
        _md5_key = u'id.%s' % _rs.md5_id
        _topic_key = u'topic.%s' % topic
        
        _d = self._get_for_sha256_id(_rs.sha256_id)
        if _d:
            _rs.update_from_dict(_d)
            _rs.version = _rs.current_version(topic)
        else:
            self.redis.hmset(_sha256_key, _rs.as_dict())
            self.redis.hset(_md5_key, 'sha256_id', _sha256_key)

        # now that we know the schema is in the hashes, reg for the topic
        if not _rs.current_version(topic):
            _ver = self.redis.rpush(_topic_key, _sha256_key)
            _rs.version = _ver 
            self.redis.hset(_sha256_key, _topic_key, _ver)
            _rs.tv_dict[topic] = _ver

        return _rs
    
    def get_latest_for_topic(self, topic):
        return self.get_for_topic_and_version(topic, -1)

    def get_for_topic_and_version(self, topic, version):
        _key = u'topic.%s' % topic
        _index = int(version)
        if _index > 0:
            _index -= 1 # ver counts from 1, index from 0        
        _rvals = self.lua_get_for_topic_and_version(keys=[_key, _index, ])
        _d = self._hgetall_seq_2_dict(_rvals)
        _rs = self._get_registered_schema()
        _rs.update_from_dict(_d)
        return _rs

    def get_for_id(self, id_base64):
        _bytes = base64.b64decode(id_base64)
        _buff = io.BytesIO(_bytes)
        _id_type = struct.unpack('>b', _buff.read(1))[0]
        if _id_type == 32:
            _d = self._get_for_sha256_id(id_base64)
        elif _id_type == 16:
            _d = self._get_for_md5_id(id_base64)
        
        if _d:
            _rs = self._get_registered_schema()
            _rs.update_from_dict(_d)
            return _rs
        return None

from registered_schema import RegisteredAvroSchema

class AvroSchemaRepository(RedisSchemaRepository):
    def __init__(self, host='localhost', port=6379, db=0):
        super(AvroSchemaRepository, self).__init__()

    def _get_registered_schema(self):
        return RegisteredAvroSchema()







