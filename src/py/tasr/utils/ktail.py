'''
Created on Nov 15, 2016

@author: cmills
'''

import sys
import os
import logging
import argparse

from kafka import KafkaConsumer
from tasr.utils.serializer import MTLeader, MTDeserializer

class KTail(object):
    '''
    A class encapsulating both a Kafka consumer and a deserializer for Avro
    serialized events with multi-type headers. This is the guts of the ktail
    util, supporting tail-like functionality for Kafka topics.
    '''

    def __init__(self, topic, kafka_hosts, tasr_host, offset='latest'):
        '''
        Constructs an MTTopicFollower object.

        topic        The name of the Kafka topic to follow.
        kafka_hosts  A list of '<address>:<port>' host description strings.
        tasr_host    An '<address>:<port>' host description string.
        offset       One of 'latest' or 'earliest'.
        '''
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.setLevel(logging.WARN)

        self.topic = topic
        self.tasr_url = 'http://' + tasr_host
        self.consumer = KafkaConsumer(topic, group_id='ktail',
                                      auto_offset_reset=offset,
                                      enable_auto_commit=False,
                                      bootstrap_servers=kafka_hosts)
        self.log.debug('consumer: %s', self.consumer)
        self.mtd_ver_map = dict()
        self.mtd_sha_map = dict()

    def __iter__(self):
        return self

    def add_deserializer(self, sha256_id=None, version_number=None):
        '''Adds a new MTDeserializer to the cache based on either a SHA256 id
        or the object's topic and a passed version number.'''
        deser = None
        if sha256_id:
            deser = MTDeserializer(sha256_id=sha256_id, tasr_url=self.tasr_url)
        elif version_number and version_number > 0:
            deser = MTDeserializer(topic=self.topic,
                                   version_number=version_number,
                                   tasr_url=self.tasr_url)
        self.mtd_sha_map[deser.sha256_id] = deser
        self.mtd_ver_map[deser.version_number] = deser
        self.log.info('Added: %s', deser)
        return deser

    def next(self):
        '''Return the next message in the followed Kafka topic, deserialized
        into an event dict.
        '''
        msg = self.consumer.next()
        self.log.debug('message topic: %s', msg.topic)
        self.log.debug('message partition: %s', msg.partition)
        self.log.info('message offset: %s', msg.offset)
        self.log.debug('message key: %s', msg.key)
        self.log.debug('message value: %s', msg.value)
        mt_message = msg.value

        # use the leader to figure out which deserializer we'll want to use
        leader = MTLeader(mt_message)
        deser = None
        if leader.sha256_id:
            if leader.sha256_id not in self.mtd_sha_map:
                # grab and add to cache
                deser = self.add_deserializer(sha256_id=leader.sha256_id)
            else:
                deser = self.mtd_sha_map[leader.sha256_id]
        elif leader.version_number and leader.version_number > 0:
            if leader.version_number not in self.mtd_ver_map:
                # grab and add to cache
                vnum = leader.version_number
                deser = self.add_deserializer(version_number=vnum)
            else:
                deser = self.mtd_ver_map[leader.version_number]
        # return the deserialized event dict
        return deser.mt_message_to_dict(mt_message)


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('topic', help='The Kafka topic to tail. (required)')
    parser.add_argument('-v', '--verbose', action='count')
    parser.add_argument('--tasr', default='tasr.tagged.com:80')
    parser.add_argument('--kafka', nargs='*', default=['kafkadatahub01:9092'])
    args = parser.parse_args(argv[1:])

    verbosity = logging.ERROR
    if args.verbose > 2:
        verbosity = logging.DEBUG
    elif args.verbose == 2:
        verbosity = logging.INFO
    elif args.verbose == 1:
        verbosity = logging.WARN

    logging.getLogger().setLevel(verbosity)
    logging.info('topic: %s, tasr_host: %s, kafka_hosts: %s',
                 args.topic, args.tasr, args.kafka)

    try:
        events = KTail(args.topic, kafka_hosts=args.kafka, tasr_host=args.tasr)
        events.log.setLevel(verbosity)
        for event_dict in events:
            sys.stdout.write(str(event_dict) + '\n')
    except KeyboardInterrupt:
        sys.stderr.write('Interrupted.\n')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

if __name__ == "__main__":
    main(sys.argv)





