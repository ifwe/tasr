'''
Created on Apr 2, 2014

@author: cmills

Any schema repository that supports grouping and serial numeric versioning of
related schemas should be able to do the following:

  - find out what group_names there are in the repository
  - add a new group
  - get metadata for a group (name, creation time, defaults, etc)
  - find out what schemas have been registered for a group
  - register a schema with a group, making it the latest version
  - get the latest schema version for a group
  - get a specific schema version for a group by group and a unique identifier
  - get metadata for a schema (identifiers, current version associations, etc)

For our purposes, the "group" is defined by our event types.  So, for example,
two of our event types are "page_view" and "login_detail".  Within Kafka, a
"topic" is analogous to the more general notion of a group.  Serialized events
are carried in topics with an "s_" prefix, so the two Kafka topics for the
above mentioned event types would be "s_page_view" and "s_login_detail".  In
the Avro project's repository code and discussions, the "subject" is analogous
to the group.  In schemas themselves, the record name (e.g. -- "PageView" or
"LoginDetail") is our group equivalent.  In practice all four analogs -- group,
topic, subject, and record name -- mean pretty much the same thing.  The format
differences (snake vs. camel case, the use of the "s_" prefix) are meant to
help differentiate where a string is expected to be used.

Schemas are identified one of three ways:

 - by a combination of group and version,
 - by the schema string itself, or
 - by a hash-based "fingerprint" id string.

Using group and version is compact, but requires access to the repo to get or
confirm the version for a given schema string.  Using the whole schema string
is unambiguous but far from compact, with the identifier being larger than the
message in many cases.

The fingerprint approach is the best if access to the repo is uncertain as it
can be calculated locally, and while not as compact as group and version, it
incurs much less overhead than sending the whole schema as an identifier.  In
our implementation, the fingerprint id strings are base64 encoded byte arrays,
with a 1-byte size header (indicating how many bytes will follow) and the id
bytes themselves. Those bytes are the digest of the canonical schema string
using either the MD5 (16 bytes) or SHA256 (32 bytes) hashes.

With all this in mind, we need the following general repository methods:

  - get_all_topics()
  - register_subject()
  - lookup_subject()
  - register_schema()
  - get_schema_for_group_and_version()
  - schema_for_id_str()
  - schema_for_schema_str()
  - get_latest_schema_for_group()
  - get_latest_schemas_for_group()

The nine methods above cover the requirements for both TASR and S+V APIs.  In
each case the specifics of the registration process and the backing store are
hidden from the client.  The return types are either a single object or list of
similar objects, and where the objects are either SchemaGroup objects or
RegisteredSchema objects.  The mapping from these general methods to the
specific API endpoints should happen in the app.
'''

import time
import redis
import base64
import binascii
import io
import sys
import struct
from tasr.registered_schema import RegisteredSchema
from tasr.group import Group


class RedisSchemaRepository(object):
    '''The Redis-based implementation of the schema repository uses the
    List and Hash structures provided by Redis as the backing store.  Here is
    the overview of keys to Redis structures:

      'id.<sha256_id>':   hash (primary entry)
      'id.<md5_id>':      hash (basically an index of md5->sha256 ids)
      'g.<group name>':   hash (default field values, validators)
      'vid.<group name>': list (version sha256_id values, in order)
      'vts.<group name>': list (version timestamp values, in order)

    The primary store is a hash type, using a key in the form 'id.<sha256_id>'.
    The hash entry has, at a minimum, the following fields: 'sha256_id',
    'md5_id', and 'schema'.  Additionally, for each group the schema is
    registered for, the hash entry has 'cvs.<group>' and 'cvts.<group>' fields
    that hold the current version serial number and current version timestamp
    respectively.  Note that the version held is the latest for a group, so if
    the same schema has been registered more than once for a group, the hash's
    entry only lists the version  _last_registered_ for the group.

    There is a secondary hash entry used to provide an index from the md5_id
    values.  The key is in the form 'id.<md5_id>', and the hash entry only
    holds one field, 'sha256_id', with the key value you need to look up the
    primary hash entry (that is, 'id.<sha256_id>').

    A third hash entry is used to hold the config map (basically a set of field
    defaults) for a group if it has been specified.  This hash entry is also
    used to initialize a group prior to registering schemas for it (used in the
    S+V API).  The key for the hash type entry is in the form 'g.<group>'. The
    field keys are unrestricted, putting in whatever was passed when
    initializing the group.  Note that while a group config map is optional
    (you can register a schema for a group without providing a config map), a
    hash object _will_ be added automatically when a group is initialized or
    when a schema is registered for a group with no existing hash entry.  When
    the group config map is added without a specified map, it will contain only
    a "group_ts" field, holding the numeric timestamp (UTC, seconds since the
    epoch) of when the entry was added.  Note that this is also where validator
    class names for the group are held, with a "validators" field holding a
    whitespace-delimited string of class names.

    In addition to the hash entries, there are two lists for each group that
    has registered schemas: a 'vid.<group>' that holds the SHA256 id keys
    (i.e. -- 'id.<sha256_id>') for each schema version and a 'vts.<group>' that
    holds the UTC timestamp when each version was registered.  The version is
    the "counting from 1" index of the entry in the list.  So, the first entry
    in the list corresponds to version 1. These are lists, not sets, as it is
    possible for a schema to be registered, then overridden, then reverted to
    -- in which case the same id key can occur more than once in the list.

    Retrieval of schemas by SHA256 ID is simple, requiring a single Redis
    operation.  However, retrieving by MD5 ID, schema string, or, more
    commonly, by topic and version, require multiple operations.  It is much
    faster to execute all the ops in a single network call, so we use the LUA
    script support in Redis to enable this. This approach allows us to avoid
    the latency of a second round-trip to Redis.
    '''
    def __init__(self, host='localhost', port=6379, db=0):
        super(RedisSchemaRepository, self).__init__()
        self.redis = redis.StrictRedis(host, port, db)
        # register_schema lua scripts in Redis
        self.lua_get_for_md5 = None
        self.lua_get_for_group_and_version = None
        self.lua_get_cur_versions = None
        self.lua_init_group = None
        try:
            self.reg_lua_get_for_md5()
            self.reg_lua_get_for_group_and_version()
            self.reg_lua_get_cur_versions()
            self.reg_lua_init_group()
        except redis.exceptions.ConnectionError:
            raise Exception(u'No Redis at %s on port %s and db %s' %
                            (host, port, db))

    ##########################################################################
    # LUA script registrations
    ##########################################################################
    def reg_lua_get_for_md5(self):
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

    def reg_lua_get_for_group_and_version(self):
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
        self.lua_get_for_group_and_version = self.redis.register_script(lua)

    def reg_lua_get_cur_versions(self):
        '''Registers a LUA script to get the current schema version number for
        each of the group_names with registered schemas.'''
        lua = '''
        local gv_list={}
        local vid_keys=redis.call('keys', 'vid.*')
        for _,vid_key in ipairs(vid_keys) do
            gv_list[#gv_list+1] = vid_key
            gv_list[#gv_list+1] = redis.call('llen', vid_key)
        end
        return gv_list
        '''
        self.lua_get_cur_versions = self.redis.register_script(lua)

    def reg_lua_init_group(self):
        '''Registers a LUA script to initialize a group -- meaning add a hash
        object and set the 'group_ts' field if the hash is not already present.
        In every case, all the fields of the hash object are returned.
        '''
        lua = '''
        local group_ts = redis.call('hget', KEYS[1], 'group_ts')
        if not group_ts then
            redis.call('hset', KEYS[1], 'group_ts', KEYS[2])
        end
        return  redis.call('hgetall', KEYS[1])
        '''
        self.lua_init_group = self.redis.register_script(lua)

    ##########################################################################
    # util methods
    ##########################################################################
    def instantiate_registered_schema(self):
        '''Returns a RegisteredSchema object.  Override this in subclasses
        when a more specific class (i.e. -- RegisteredAvroSchema) is used.
        '''
        return RegisteredSchema()

    @staticmethod
    def pair_seq_2_dict(vlist):
        '''The HGETALL Redis command returns a "[<name0>,<value0>,<name1>, ...]
        pair list, which we want to turn into a dict in several cases.
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

    ##########################################################################
    # low-level retrieval methods
    ##########################################################################

    def get_schema_dict_for_sha256_id(self, sha256_base64_id):
        '''A low-level method to pull the hash struct identified by the passed
        sha256 id and return it as a dict.
        '''
        sha256_key = u'id.%s' % sha256_base64_id
        hash_d = self.redis.hgetall(sha256_key)
        if len(hash_d) == 0:
            return None
        return hash_d

    def get_schema_dict_for_md5_id(self, md5_base64_id):
        '''A low-level method to pull the hash struct identified by the passed
        md5 id, using a registered LUA script, and return it as a dict.
        '''
        md5_key = u'id.%s' % md5_base64_id
        rvals = self.lua_get_for_md5(keys=[md5_key, ])
        return RedisSchemaRepository.pair_seq_2_dict(rvals)

    ##########################################################################
    # exposed, API methods
    ##########################################################################

    def get_all_topics(self):
        '''Return a set of current group objects.'''
        group_names = []
        for group_key in self.redis.keys('g.*'):
            group_name = group_key[2:]
            group = self.lookup_subject(group_name)
            group.current_schema = self.get_latest_schema_for_group(group_name)
            group_names.append(group)
        return group_names

    def register_subject(self, group_name, config_dict=None, validators=None):
        '''Initialize a group, optionally specifying a config dict of default
        values and a set of validator class name strings.'''
        group_key = 'g.%s' % group_name
        timestamp = long(time.time())
        rvals = self.lua_init_group(keys=[group_key, timestamp, ])
        if rvals:
            # this will update the hash fields and validators if provided
            if config_dict:
                self.redis.hmset(group_key, config_dict)
            if validators:
                validators_key = 'validators.%s' % group_name
                self.redis.sadd(validators_key, validators)
            return self.lookup_subject(group_name)

    def lookup_subject(self, group_name):
        '''Retrieve a Group object with the specified name or None.  The field
        names starting with "group_" should set group level attributes.  The
        field names starting with "default_" should set field defaults for the
        group schemas.'''
        group_key = 'g.%s' % group_name
        group_dict = self.redis.hgetall(group_key)
        if group_dict:
            timestamp = group_dict.pop('group_ts')
            group = Group(group_name, group_dict if group_dict else None)
            group.timestamp = timestamp
            group.current_schema = self.get_latest_schema_for_group(group_name)
            return group

    def register_schema(self, group_name, schema_str):
        '''Register a schema string as a version for a group_name.'''
        new_rs = self.instantiate_registered_schema()
        new_rs.schema_str = schema_str
        if not new_rs.validate_schema_str():
            raise ValueError(u'Cannot register_schema invalid schema.')

        # make sure the group_name is registered first
        self.register_subject(group_name)

        # the key values are what we use as Redis keys
        sha256_key = u'id.%s' % new_rs.sha256_id
        md5_key = u'id.%s' % new_rs.md5_id
        vid_key = u'vid.%s' % group_name
        vts_key = u'vts.%s' % group_name
        now = long(time.time())
        # we also need to support the old topic.* lists as well for Vadim
        topic_key = u'topic.%s' % group_name

        old_d = self.get_schema_dict_for_sha256_id(new_rs.sha256_id)
        if old_d:
            new_rs.update_from_dict(old_d)
        else:
            self.redis.hmset(sha256_key, new_rs.as_dict())
            self.redis.hset(md5_key, 'sha256_id', sha256_key)

        # now that we know the schema is in the hashes, reg for the group_name
        if not new_rs.current_version(group_name):
            # no version for this group_name, so add it
            ver = self.redis.rpush(vid_key, sha256_key)
            # ensure we append the topic.* list as well for back-compatibility
            topic_ver = self.redis.rpush(topic_key, sha256_key)
            if ver != topic_ver:
                sys.stderr.write('vid.* and topic.* version mismatch')
            self.redis.hset(sha256_key, vid_key, ver)
            new_rs.gv_dict[group_name] = ver
            self.redis.rpush(vts_key, now)
            self.redis.hset(sha256_key, vts_key, now)
            new_rs.ts_dict[group_name] = now
        else:
            last_ver_sha256_key = self.redis.lrange(vid_key, -1, -1)[0]
            if not last_ver_sha256_key == sha256_key:
                # need to override outdated version entry with new one
                ver = self.redis.rpush(vid_key, sha256_key)
                self.redis.hset(sha256_key, vid_key, ver)
                new_rs.gv_dict[group_name] = ver
                self.redis.rpush(vts_key, now)
                self.redis.hset(sha256_key, vts_key, now)
                new_rs.ts_dict[group_name] = now

        return new_rs

    def get_schema_for_group_and_version(self, group_name, version):
        '''Gets the registered schema for a group_name and version using the
        registered LUA script.  Note that version must be a whole integer
        greater than 0 or -1, which is a flag for the most current version.
        '''
        vid_key = u'vid.%s' % group_name
        index = int(version)
        if index == 0:
            # 0 is an invalid version here, we count from 1, not 0
            return
        elif index < -1:
            # -1 is a flag for current, but other negative values are invalid
            return
        elif index > 0:
            index -= 1  # ver counts from 1, index from 0
        rvals = self.lua_get_for_group_and_version(keys=[vid_key, index, ])
        if rvals:
            rs_d = RedisSchemaRepository.pair_seq_2_dict(rvals)
            retrieved_rs = self.instantiate_registered_schema()
            retrieved_rs.update_from_dict(rs_d)
            return retrieved_rs

    def get_schema_for_id_str(self, id_str):
        '''Gets the registered schema with a given md5- or sha256-based id
        string using low-level retrieval methods.
        '''
        base64_id = id_str[3:] if id_str.startswith('id.') else id_str
        id_bytes = base64.b64decode(base64_id)
        buff = io.BytesIO(id_bytes)
        id_type = struct.unpack('>b', buff.read(1))[0]
        if id_type == 32:
            rs_d = self.get_schema_dict_for_sha256_id(id_str)
        elif id_type == 16:
            rs_d = self.get_schema_dict_for_md5_id(id_str)

        if rs_d:
            retrieved_rs = self.instantiate_registered_schema()
            retrieved_rs.update_from_dict(rs_d)
            return retrieved_rs
        return None

    def get_schema_for_schema_str(self, schema_str):
        '''Passing in a schema string, retrieve the RegisteredSchema object
        associated with the passed schema string. We rely on the
        RegisteredSchema class' canonicalization and the SHA256 fingerprint
        figured for the canonical schema string.
        '''
        # load the passed schema string into a RegisteredSchema object
        target_rs = self.instantiate_registered_schema()
        target_rs.schema_str = schema_str
        if not target_rs.validate_schema_str():
            raise ValueError(u'Cannot register_schema invalid schema.')
        rs_d = self.get_schema_dict_for_sha256_id(target_rs.sha256_id)
        if rs_d:
            retrieved_rs = self.instantiate_registered_schema()
            retrieved_rs.update_from_dict(rs_d)
            return retrieved_rs
        return None

    def get_latest_schema_for_group(self, group):
        '''A convenience method'''
        return self.get_schema_for_group_and_version(group, -1)

    def get_latest_schema_versions_for_group(self, group, max_versions=5):
        '''This retrieves the n most recent schema versions for a group.  If
        max_versions is set to -1, it will return ALL versions for the group.
        Note that this is iterative, and will generate a call for each version
        returned, so keep the depth reasonable.
        '''
        versions = []
        rs = self.get_latest_schema_for_group(group)
        versions.insert(0, rs)
        cur_ver = rs.current_version(group)
        if max_versions < 0:
            first_ver = 1
        else:
            first_ver = cur_ver - max_versions + 1  # counting from 1, not 0
            first_ver = 1 if first_ver < 1 else first_ver
        for version in sorted(range(first_ver, cur_ver), reverse=True):
            version = self.get_schema_for_group_and_version(group, version)
            versions.insert(0, version)
        return versions

    def get_versions_for_id_str_and_group(self, id_str, group):
        '''Given an id_str and a group, we should be able to figure out which
        of the group's registered schema versions used the identified schema.
        We ensure we are using an sha256-based id (which are used to populate
        the vid.* lists), then we step through the list for the group and note
        which list ids match.  This lets you identify non-sequential re-
        registration of the same schema.  This does not come up much, so we
        don't bother with a LUA script.
        '''
        base64_id = id_str[3:] if id_str.startswith('id.') else id_str
        sha256_key = None
        if len(base64_id) == 44:
            # we have an sha256 id already
            sha256_key = u'id.%s' % base64_id
        elif len(base64_id) == 24:
            # we have an md5 id, so get the sha256 id from redis
            md5_key = u'id.%s' % base64_id
            sha256_key = self.redis.hget(md5_key, 'sha256_id')
        vid_key = u'vid.%s' % group
        vlist = []
        version = 0
        for vid in self.redis.lrange(vid_key, 0, -1):
            version += 1
            if vid == sha256_key:
                vlist.append(version)
        return vlist


from tasr.registered_schema import RegisteredAvroSchema


class AvroSchemaRepository(RedisSchemaRepository):
    '''This is an Avro-specific schema repository class.
    '''
    def __init__(self, host='localhost', port=6379, db=0):
        super(AvroSchemaRepository, self).__init__(host=host, port=port, db=db)

    def instantiate_registered_schema(self):
        '''Returns a RegisteredAvroSchema object, overriding the parent.
        '''
        return RegisteredAvroSchema()
