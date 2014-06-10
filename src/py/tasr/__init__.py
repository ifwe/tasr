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
    The object also has a tv_dict attribute, which should generally hold the 
    most recent version the schema represents for each topic it is associated 
    with.  The only exception is when a schema is retrieved for a topic and 
    version and that version is not the most recent -- in which case the tv_dict
    holds the expected version for the requested topic.
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
    registered schemas.  Each list key is in the form 'topic.<topic name>', and 
    each list entry is an SHA256 id key (i.e. -- 'id.<sha256_id>').  The version 
    is the "counting from 1" index of the entry in the list.  So, the first 
    entry in the list corresponds to version 1. These are lists, not sets, as it 
    is possible for a schema to be registered, then overridden, then reverted to 
    -- in which case the same id key can occur more than once in the list.
    
    A second list is used to keep track of when (UTC timestamp) a schema was
    associated with a topic.  This is a list with a key in the format 
    'topic_ts.<topic name>', storing the numeric timestamp (seconds since the 
    epoch) when the assignment happened.
    
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
        try:
            self.lua_get_for_md5 = self._register_lua_get_for_md5()
            self.lua_get_for_topic_and_version = self._register_lua_get_for_topic_and_version()
        except redis.exceptions.ConnectionError:
            raise Exception(u'Failed to connect to Redis at %s on port %s and db %s' %
                            (host, port, db))

    # LUA script registrations
    def _register_lua_get_for_md5(self):
        lua = '''
        local sha256_id = redis.call('hget', KEYS[1], 'sha256_id')
        if sha256_id then
            return redis.call('hgetall', sha256_id)
        else
            return nil
        end
        '''
        return self.redis.register_script(lua)

    def _register_lua_get_for_topic_and_version(self):
        lua = '''
        local sha256_id = redis.call('lindex', KEYS[1], KEYS[2])
        if sha256_id then
            return redis.call('hgetall', sha256_id)
        else
            return nil
        end
        '''
        return self.redis.register_script(lua)

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
        idx = 0
        rdict = dict()
        while idx < len(vlist):
            k = vlist[idx]
            idx += 1
            v = vlist[idx]
            idx += 1
            rdict[k] = v
        if len(rdict) == 0:
            return None
        return rdict
    
    def _get_for_sha256_id(self, sha256_base64_id):
        '''A low-level method to pull the hash struct identified by the passed 
        sha256 id and return it as a dict.
        '''
        key = u'id.%s' % sha256_base64_id
        d = self.redis.hgetall(key)
        if len(d) == 0:
            return None
        return d

    def _get_for_md5_id(self, md5_base64_id):
        '''A low-level method to pull the hash struct identified by the passed 
        md5 id, using a registered LUA script, and return it as a dict.
        '''
        key = u'id.%s' % md5_base64_id
        rvals = self.lua_get_for_md5(keys=[key, ])
        return self._hgetall_seq_2_dict(rvals)

    # exposed, API methods
    def register(self, topic, schema_str):
        '''Register a schema string as a version for a topic.
        '''
        rs = self._get_registered_schema()
        rs.schema_str = schema_str
        if not rs.validate_schema_str():
            raise Exception(u'Cannot register invalid schema.')

        # the key values are what we use as Redis keys
        sha256_key = u'id.%s' % rs.sha256_id
        md5_key = u'id.%s' % rs.md5_id
        topic_key = u'topic.%s' % topic
        topic_ts_key = u'topic_ts.%s' % topic
        now = long(time.time())
        
        d = self._get_for_sha256_id(rs.sha256_id)
        if d:
            rs.update_from_dict(d)
        else:
            self.redis.hmset(sha256_key, rs.as_dict())
            self.redis.hset(md5_key, 'sha256_id', sha256_key)

        # now that we know the schema is in the hashes, reg for the topic
        
        if not rs.current_version(topic):
            # no version for this topic, so add it
            ver = self.redis.rpush(topic_key, sha256_key)
            #rs.version = ver 
            self.redis.hset(sha256_key, topic_key, ver)
            rs.tv_dict[topic] = ver
            self.redis.rpush(topic_ts_key, now)
            self.redis.hset(sha256_key, topic_ts_key, now)
            rs.ts_dict[topic] = now
        else:
            last_ver_sha256_key = self.redis.lrange(topic_key, -1, -1)[0]
            if not last_ver_sha256_key == sha256_key:
                # need to override outdated version entry with new one
                ver = self.redis.rpush(topic_key, sha256_key)
                #rs.version = ver
                self.redis.hset(sha256_key, topic_key, ver)
                rs.tv_dict[topic] = ver
                self.redis.rpush(topic_ts_key, now)
                self.redis.hset(sha256_key, topic_ts_key, now)
                rs.ts_dict[topic] = now
                
        return rs
    
    def get_latest_for_topic(self, topic):
        return self.get_for_topic_and_version(topic, -1)

    def get_for_topic_and_version(self, topic, version):
        key = u'topic.%s' % topic
        index = int(version)
        if index > 0:
            index -= 1 # ver counts from 1, index from 0        
        rvals = self.lua_get_for_topic_and_version(keys=[key, index, ])
        if rvals:
            d = self._hgetall_seq_2_dict(rvals)
            rs = self._get_registered_schema()
            rs.update_from_dict(d)
            return rs

    def get_for_id(self, id_base64):
        id_bytes = base64.b64decode(id_base64)
        buff = io.BytesIO(id_bytes)
        id_type = struct.unpack('>b', buff.read(1))[0]
        if id_type == 32:
            d = self._get_for_sha256_id(id_base64)
        elif id_type == 16:
            d = self._get_for_md5_id(id_base64)
        
        if d:
            rs = self._get_registered_schema()
            rs.update_from_dict(d)
            return rs
        return None
    
    def get_all_versions_for_id_and_topic(self, id_base64, topic):
        key = u'topic.%s' % topic
        vlist = []
        version = 0
        for vid in self.redis.lrange(key, 0, -1):
            version += 1
            if vid[3:] == id_base64:
                vlist.append(version)
        return vlist

from registered_schema import RegisteredAvroSchema

class AvroSchemaRepository(RedisSchemaRepository):
    def __init__(self, host='localhost', port=6379, db=0):
        super(AvroSchemaRepository, self).__init__()

    def _get_registered_schema(self):
        return RegisteredAvroSchema()







