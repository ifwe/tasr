'''
Created on Apr 2, 2014

@author: cmills

Any schema repository should support these four basic methods:

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

import time
import redis
import base64
import binascii
import io
import struct
from tasr.registered_schema import RegisteredSchema


class RedisSchemaRepository(object):
    '''The Redis-based implementation of the schema repository uses the
    List and Hash structures provided by Redis as the backing store.

    The primary store is a hash type, using a key in the form 'id.<sha256_id>'.
    The hash entry has, at a minimum, the following fields: sha256_id, md5_id,
    and schema.  Additionally, for each topic the schema is registered for, the
    hash entry has a 'topic.<topic>' field that holds a version number.  The
    version is the latest version for a topic, so if the same schema has been
    registered more than once for a topic, the hash's entry only lists the
    version last registered for the topic.

    There is a secondary hash entry used to provide an index from the md5_id
    values.  The key is in the form 'id.<md5_id>', and the hash entry only
    holds one field, 'sha256_id', with the key value you need to look up the
    primary hash entry (that is, 'id.<sha256_id>').

    In addition to the hash entries, there are lists for each topic that has
    registered schemas.  Each list key is in the form 'topic.<topic name>', and
    each list entry is an SHA256 id key (i.e. -- 'id.<sha256_id>').  The
    version is the "counting from 1" index of the entry in the list.  So, the
    first entry in the list corresponds to version 1. These are lists, not
    sets, as it is possible for a schema to be registered, then overridden,
    then reverted to -- in which case the same id key can occur more than once
    in the list.

    A second list is used to keep track of when (UTC timestamp) a schema was
    associated with a topic.  This is a list with a key in the format
    'topic_ts.<topic name>', storing the numeric timestamp (seconds since the
    epoch) when the assignment happened.

    Retrieval of schemas by SHA256 ID is simple, requiring a single Redis
    operation.  However, retrieving by MD5 ID or, more commonly, by topic and
    version, requires two operations.  It is much faster to execute both in a
    single call, so we use the LUA script support in Redis to enable this. This
    approach allows us to avoid the latency of a second round-trip to Redis.
    '''
    def __init__(self, host='localhost', port=6379, db=0):
        super(RedisSchemaRepository, self).__init__()
        self.redis = redis.StrictRedis(host, port, db)
        # register lua scripts in Redis
        self.lua_get_for_md5 = None
        self.lua_get_for_topic_and_version = None
        self.lua_getcur_versions = None
        try:
            self._register_lua_get_for_md5()
            self._register_lua_get_for_t_and_v()
            self._register_lua_get_cur_versions()
        except redis.exceptions.ConnectionError:
            raise Exception(u'No Redis at %s on port %s and db %s' %
                            (host, port, db))

    ##########################################################################
    # LUA script registrations
    ##########################################################################
    def _register_lua_get_for_md5(self):
        '''Registers a LUA script to retrieve a registered schema's main hash
        data starting with an md5 id.
        '''
        lua = '''
        local sha256_id = redis.call('hget', KEYS[1], 'sha256_id')
        if sha256_id then
            return redis.call('hgetall', sha256_id)
        else
            return nil
        end
        '''
        self.lua_get_for_md5 = self.redis.register_script(lua)

    def _register_lua_get_for_t_and_v(self):
        '''Registers a LUA script to retrieve a registered schema's main hash
        data starting with a topic and version.
        '''
        lua = '''
        local sha256_id = redis.call('lindex', KEYS[1], KEYS[2])
        if sha256_id then
            return redis.call('hgetall', sha256_id)
        else
            return nil
        end
        '''
        self.lua_get_for_topic_and_version = self.redis.register_script(lua)

    def _register_lua_get_cur_versions(self):
        '''  '''
        lua = '''
        local tvlist={}
        local topics=redis.call('keys', 'topic.*')
        for _,topic_name in ipairs(topics) do
            tvlist[#tvlist+1] = topic_name
            tvlist[#tvlist+1] = redis.call('llen', topic_name)
        end
        return tvlist
        '''
        self.lua_getcur_versions = self.redis.register_script(lua)


    ##########################################################################
    # util methods
    ##########################################################################
    def _get_registered_schema(self):
        '''Returns a RegisteredSchema object.  Override this in subclasses
        when a more specific class (i.e. -- RegisteredAvroSchema) is used.
        '''
        return RegisteredSchema()

    @staticmethod
    def _hgetall_seq_2_dict(vlist):
        '''The HGETALL Redis command returns a "[<name0>,<value0>,<name1>, ...]
        list, which we want to turn into a dict in several cases.
        '''
        if not len(vlist) % 2 == 0:
            raise Exception('Must have an even number of values in the list.')
        idx = 0
        rdict = dict()
        while idx < len(vlist):
            k = vlist[idx]
            idx += 1
            val = vlist[idx]
            idx += 1
            rdict[k] = val
        if len(rdict) == 0:
            return None
        return rdict

    def _get_for_sha256_id(self, sha256_base64_id):
        '''A low-level method to pull the hash struct identified by the passed
        sha256 id and return it as a dict.
        '''
        key = u'id.%s' % sha256_base64_id
        hash_d = self.redis.hgetall(key)
        if len(hash_d) == 0:
            return None
        return hash_d

    def _get_for_md5_id(self, md5_base64_id):
        '''A low-level method to pull the hash struct identified by the passed
        md5 id, using a registered LUA script, and return it as a dict.
        '''
        key = u'id.%s' % md5_base64_id
        rvals = self.lua_get_for_md5(keys=[key, ])
        return RedisSchemaRepository._hgetall_seq_2_dict(rvals)

    ##########################################################################
    # exposed, API methods
    ##########################################################################
    def register(self, topic, schema_str):
        '''Register a schema string as a version for a topic.
        '''
        new_rs = self._get_registered_schema()
        new_rs.schema_str = schema_str
        if not new_rs.validate_schema_str():
            raise ValueError(u'Cannot register invalid schema.')

        # the key values are what we use as Redis keys
        sha256_key = u'id.%s' % new_rs.sha256_id
        md5_key = u'id.%s' % new_rs.md5_id
        topic_key = u'topic.%s' % topic
        topic_ts_key = u'topic_ts.%s' % topic
        now = long(time.time())

        old_d = self._get_for_sha256_id(new_rs.sha256_id)
        if old_d:
            new_rs.update_from_dict(old_d)
        else:
            self.redis.hmset(sha256_key, new_rs.as_dict())
            self.redis.hset(md5_key, 'sha256_id', sha256_key)

        # now that we know the schema is in the hashes, reg for the topic

        if not new_rs.current_version(topic):
            # no version for this topic, so add it
            ver = self.redis.rpush(topic_key, sha256_key)
            # rs.version = ver
            self.redis.hset(sha256_key, topic_key, ver)
            new_rs.tv_dict[topic] = ver
            self.redis.rpush(topic_ts_key, now)
            self.redis.hset(sha256_key, topic_ts_key, now)
            new_rs.ts_dict[topic] = now
        else:
            last_ver_sha256_key = self.redis.lrange(topic_key, -1, -1)[0]
            if not last_ver_sha256_key == sha256_key:
                # need to override outdated version entry with new one
                ver = self.redis.rpush(topic_key, sha256_key)
                # rs.version = ver
                self.redis.hset(sha256_key, topic_key, ver)
                new_rs.tv_dict[topic] = ver
                self.redis.rpush(topic_ts_key, now)
                self.redis.hset(sha256_key, topic_ts_key, now)
                new_rs.ts_dict[topic] = now

        return new_rs

    def get_latest_for_topic(self, topic):
        '''A convenience method
        '''
        return self.get_for_topic_and_version(topic, -1)

    def get_for_topic_and_version(self, topic, version):
        '''Gets the registered schema for a topic and version using the
        registered LUA script.
        '''
        key = u'topic.%s' % topic
        index = int(version)
        if index > 0:
            index -= 1  # ver counts from 1, index from 0
        rvals = self.lua_get_for_topic_and_version(keys=[key, index, ])
        if rvals:
            rs_d = RedisSchemaRepository._hgetall_seq_2_dict(rvals)
            retrieved_rs = self._get_registered_schema()
            retrieved_rs.update_from_dict(rs_d)
            return retrieved_rs

    def get_for_id_str(self, id_base64):
        '''Gets the registered schema with a given md5 or sha256 id.  For the
        md5 id case, we use the registered LUA script.
        '''
        id_bytes = base64.b64decode(id_base64)
        buff = io.BytesIO(id_bytes)
        id_type = struct.unpack('>b', buff.read(1))[0]
        if id_type == 32:
            rs_d = self._get_for_sha256_id(id_base64)
        elif id_type == 16:
            rs_d = self._get_for_md5_id(id_base64)

        if rs_d:
            retrieved_rs = self._get_registered_schema()
            retrieved_rs.update_from_dict(rs_d)
            return retrieved_rs
        return None

    def get_for_schema_str(self, schema_str):
        '''Passing in a schema string, retrieve the RegisteredSchema object
        associated with the passed schema string. We rely on the
        RegisteredSchema class' canonicalization and the SHA256 fingerprint
        figured for the canonical schema string.
        '''
        # load the passed schema string into a RegisteredSchema object
        target_rs = self._get_registered_schema()
        target_rs.schema_str = schema_str
        if not target_rs.validate_schema_str():
            raise ValueError(u'Cannot register invalid schema.')
        rs_d = self._get_for_sha256_id(target_rs.sha256_id)
        if rs_d:
            retrieved_rs = self._get_registered_schema()
            retrieved_rs.update_from_dict(rs_d)
            return retrieved_rs
        return None

    def get_all_topics(self):
        '''Return a sorted list of all the topics currently associated with
        schemas in the repository.
        '''
        topic_list = []
        for topic_key in self.redis.keys('topic.*'):
            topic_list.append(topic_key[6:])
        return sorted(topic_list)

    def get_all_topics_and_cur_versions(self):
        '''Return a dict with known topics as keys and current versions for
        those topics as vals.
        '''
        tv_list = self.lua_getcur_versions()
        cur_ver_dict = self._hgetall_seq_2_dict(tv_list)
        rdict = {}
        if not cur_ver_dict:
            return rdict
        for key, val in cur_ver_dict.iteritems():
            rdict[key[6:]] = val
        return rdict

    def get_versions_for_id_and_topic(self, id_base64, topic):
        '''Return a list of all topic versions for a schema with a given
        SHA256-based id.  This lets you identify non-sequential re-registration
        of the same schema.  This does not come up much, so we don't bother
        with a LUA script.
        '''
        key = u'topic.%s' % topic
        vlist = []
        version = 0
        for vid in self.redis.lrange(key, 0, -1):
            version += 1
            if vid[3:] == id_base64:
                vlist.append(version)
        return vlist


from tasr.registered_schema import RegisteredAvroSchema


class AvroSchemaRepository(RedisSchemaRepository):
    '''This is an Avro-specific schema repository class.
    '''
    def __init__(self, host='localhost', port=6379, db=0):
        super(AvroSchemaRepository, self).__init__(host=host, port=port, db=db)

    def _get_registered_schema(self):
        '''Returns a RegisteredAvroSchema object, overriding the parent.
        '''
        return RegisteredAvroSchema()
