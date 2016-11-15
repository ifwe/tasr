'''
Created on Nov 15, 2016

@author: cmills
'''

from kafka import KafkaConsumer
from tasr.serializer import MTLeader, MTDeserializer
import logging

class KTail(object):
    '''
    Support tail-like functionality for Kafka topics containing Avro serialized
    events with multi-type message leaders.
    '''

    def __init__(self, topic, bootstrap_servers=['kafkadatahub01:9092']):
        '''
        Constructor
        '''
        self.topic = topic
        logging.info('KTail topic: %s', self.topic)
        self.consumer = KafkaConsumer(topic, group_id='ktail',
                                      auto_offset_reset='latest',
                                      enable_auto_commit=False,
                                      bootstrap_servers=bootstrap_servers)
        logging.info('KTail consumer: %s', self.consumer)
        self.mtd_ver_map = dict()
        self.mtd_sha_map = dict()

    def __iter__(self):
        return self

    def next(self):
        msg = self.consumer.next()
        logging.debug('kt: topic: %s', msg.topic)
        logging.debug('kt: partition: %s', msg.partition)
        logging.info('kt: offset: %s', msg.offset)
        logging.debug('kt: key: %s', msg.key)
        logging.debug('kt: value: %s', msg.value)
        mt_message = msg.value

        # use the leader to figure out which deserializer we'll want to use
        leader = MTLeader(mt_message)
        deserializer = None
        if leader.sha256_id:
            if leader.sha256_id not in self.mtd_sha_map:
                # deserializer not in the cache, so instantiate and add it
                deserializer = MTDeserializer(sha256_id=leader.sha256_id)
                self.mtd_sha_map[deserializer.sha256_id] = deserializer
                self.mtd_ver_map[deserializer.version_number] = deserializer
                logging.info('kt: added MTDeserializer: %s', deserializer)
            else:
                deserializer = self.mtd_sha_map[leader.sha256_id]
        elif leader.version_number and leader.version_number > 0:
            if leader.version_number not in self.mtd_ver_map:
                # deserializer not in the cache, so instantiate and add it
                deserializer = MTDeserializer(topic=self.topic,
                                     version_number=leader.version_number)
                self.mtd_sha_map[deserializer.sha256_id] = deserializer
                self.mtd_ver_map[deserializer.version_number] = deserializer
                logging.info('kt: added MTDeserializer: %s', deserializer)
            else:
                deserializer = self.mtd_ver_map[leader.version_number]
        # return the deserialized event dict
        return deserializer.mt_message_to_dict(mt_message)
