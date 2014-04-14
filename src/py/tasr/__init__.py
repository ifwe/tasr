'''
Created on Apr 2, 2014

@author: cmills
'''

class AbstractSchemaRepository(object):
    '''Any schema repository should support these three basic methods:
    
      - register
      - getLatestForTopic
      - getById
    
    In each case the specifics of the registration process and the backing store
    are hidden from the client.  What gets passed back and forth are strings --
    topic, schema_str and id (which is base64-encoded bytes -- so, a string).
    '''
    def __init__(self):
        pass
    
    def register(self, topic, schema):
        raise Exception(u'Abstract class method called.')
    
    def getLatestForTopic(self, topic):
        raise Exception(u'Abstract class method called.')
    
    def getByID(self, id_base64=None, id_bytes=None, id_hex=None):
        raise Exception(u'Abstract class method called.')


import time
import logging
import redis
import base64
import binascii
from registered_schema import RegisteredSchema

class RedisSchemaRepository(AbstractSchemaRepository):
    '''
    '''
    def __init__(self, host='localhost', port=6379, db=0):
        super(RedisSchemaRepository, self).__init__()
        self.redis = redis.StrictRedis(host, port, db)

    def _package_registered_schema(self, topic, schema_str, version):
        return RegisteredSchema(topic, schema_str, version)

    def _get_all_for_id(self, base64_id):
        _id = u'id.%s' % base64_id
        _ver = None
        _schema_str = None
        _topic_ids = None
        _rsa = []
        if self.redis.exists(_id):
            _topic_ids = self.redis.hkeys(_id)
        for _tid in _topic_ids:
            _ver = self.redis.hget(_id, _tid)
            _schema_str = self.redis.zrangebyscore(_tid, _ver, _ver)[0]
            _rsa.append(self._package_registered_schema(_tid[6:], _schema_str, _ver))
        return _rsa

    def _get_by_id_and_topic(self, base64_id, topic):
        _topic_id = u'topic.%s' % topic
        _id = u'id.%s' % base64_id
        _ver = None
        _schema_str = None
        if self.redis.exists(_id):
            _ver = self.redis.hget(_id,_topic_id)
        if _ver:
            _schema_str = self.redis.zrangebyscore(_topic_id, _ver, _ver)[0]
        if _schema_str:
            return self._package_registered_schema(topic, _schema_str, _ver)

    def _get_all_for_topic(self, topic, min_ver=0, max_ver=-1):
        _topic_id = u'topic.%s' % topic
        _ver = None
        _schemas = []
        if self.redis.exists(_topic_id):
            for (_schema, _ver) in self.redis.zrange(_topic_id, min_ver, max_ver, withscores=True):
                _schemas.append(self._package_registered_schema(topic, _schema, _ver))
        return _schemas

    def _add_registered_schema(self, registered_schema):
        if not registered_schema.validate_schema():
            raise Exception(u'Cannot register invalid schema.')
        _topic_id = u'topic.%s' % registered_schema.topic
        # add the schema to the sorted set first
        logging.debug(u'zadd %s %s %s' % (_topic_id, registered_schema.version, 
                                          registered_schema.cannonical_schema))
        self.redis.zadd(_topic_id, registered_schema.version, 
                        registered_schema.cannonical_schema)
        # then add references to the id indexes
        self.redis.hset(u'id.%s' % registered_schema.md5_id, _topic_id, 
                        registered_schema.version)
        self.redis.hset(u'id.%s' % registered_schema.sha256_id, _topic_id, 
                        registered_schema.version)
    
    def register(self, topic, schema_str):
        _version = int(time.time())
        _new_rs = self._package_registered_schema(topic, schema_str, _version)
        if not _new_rs.validate_schema():
            raise Exception(u'Refusing to register invalid schema.')

        _md5_rs = self._get_by_id_and_topic(_new_rs.md5_id_base64, topic)
        _sha_rs = self._get_by_id_and_topic(_new_rs.sha256_id_base64, topic)
        if (_md5_rs and _md5_rs.validate_schema() and _sha_rs and 
            _sha_rs.validate_schema() and _md5_rs == _sha_rs):
            logging.debug(u'Schema already registered.')
            return _md5_rs
              
        if not (_md5_rs or _sha_rs):
            self._add_registered_schema(_new_rs)
            return _new_rs
        
    def getLatestRegisteredSchemaForTopic(self, topic):
        _rs = self._get_all_for_topic(topic, -1, -1)[0]
        if _rs.validate_schema():
            return _rs
        return None

    def getAllRegisteredSchemasForTopic(self, topic):
        return self._get_all_for_topic(topic)

    def getAllRegisteredSchemasForID(self, id_base64=None, id_bytes=None, id_hex=None):
        if not id_base64 and not id_bytes and id_hex:
            _id_bytes = binascii.unhexlify(id_hex)
        if not id_base64 and id_bytes:
            _id_base64 = base64.b64encode(id_bytes)
        return self._get_all_for_id(id_base64)

    def getLatestForTopic(self, topic):
        _rs = self.getLatestRegisteredSchemaForTopic(topic)
        if _rs:
            return _rs.cannonical_schema
    
    def getByID(self, id_base64=None, id_bytes=None, id_hex=None):
        _rsa = self.getAllRegisteredSchemasForID(id_base64, id_bytes, id_hex)
        if _rsa and len(_rsa) > 0:
            return _rsa[0].cannonical_schema
        return None


from registered_schema import RegisteredAvroSchema

class AvroSchemaRepository(RedisSchemaRepository):
    def __init__(self, host='localhost', port=6379, db=0):
        super(AvroSchemaRepository, self).__init__()

    def _package_registered_schema(self, topic, schema_str, version):
        return RegisteredAvroSchema(topic, schema_str, version)







