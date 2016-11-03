'''
Created on Nov 2, 2016

Taken from Dave's example distributed with Bruce.

@author: cmills
'''
import io
import struct


class BruceTopicTooLarge(Exception):
    'exception thrown when Kafka topic is too large'
    def __init__(self):
        Exception.__init__(self, 'Kafka topic is too large')


class BruceMsgTooLarge(Exception):
    'exception thrown when message would exceed max possible size'
    def __init__(self):
        Exception.__init__(self, 'Cannot create a message that large')


class BruceMsgCreator(object):
    '''Class for creating datagrams to be sent to Bruce.'''

    MSG_SIZE_FIELD_SIZE = 4
    API_KEY_FIELD_SIZE = 2
    API_VERSION_FIELD_SIZE = 2
    FLAGS_FIELD_SIZE = 2
    PARTITION_KEY_FIELD_SIZE = 4
    TOPIC_SIZE_FIELD_SIZE = 2
    TIMESTAMP_FIELD_SIZE = 8
    KEY_SIZE_FIELD_SIZE = 4
    VALUE_SIZE_FIELD_SIZE = 4

    ANY_PARTITION_FIXED_BYTES = MSG_SIZE_FIELD_SIZE + API_KEY_FIELD_SIZE + \
            API_VERSION_FIELD_SIZE + FLAGS_FIELD_SIZE + \
            TOPIC_SIZE_FIELD_SIZE + TIMESTAMP_FIELD_SIZE + \
            KEY_SIZE_FIELD_SIZE + VALUE_SIZE_FIELD_SIZE

    PARTITION_KEY_FIXED_BYTES = ANY_PARTITION_FIXED_BYTES + \
            PARTITION_KEY_FIELD_SIZE

    ANY_PARTITION_API_KEY = 256
    ANY_PARTITION_API_VERSION = 0

    PARTITION_KEY_API_KEY = 257
    PARTITION_KEY_API_VERSION = 0

    # This is the maximum topic size allowed by Kafka.
    @staticmethod
    def getMaxTopicSize():
        return (2 ** 15) - 1

    # This is an extremely loose upper bound, based on the maximum value that
    # can be stored in a 32-bit signed integer field.  The actual maximum is a
    # much smaller value: the maximum UNIX domain datagram size supported by
    # the operating system, which has been observed to be 212959 bytes on a
    # CentOS 7 x86_64 system.
    @staticmethod
    def getMaxMsgSize():
        return (2 ** 31) - 1

    def __init__(self):
        pass

    def create_any_partition_msg(self, topic, timestamp, key_bytes,
            value_bytes):
        topic_bytes = bytes(topic)

        if len(topic_bytes) > BruceMsgCreator.getMaxTopicSize():
            raise BruceTopicTooLarge()

        msg_size = BruceMsgCreator.ANY_PARTITION_FIXED_BYTES + \
                len(topic_bytes) + len(key_bytes) + len(value_bytes)

        if msg_size > BruceMsgCreator.getMaxMsgSize():
            raise BruceMsgTooLarge()

        buf = io.BytesIO()
        flags = 0
        buf.write(struct.pack('>ihhhh', msg_size,
                BruceMsgCreator.ANY_PARTITION_API_KEY,
                BruceMsgCreator.ANY_PARTITION_API_VERSION, flags,
                len(topic_bytes)))
        buf.write(topic_bytes)
        buf.write(struct.pack('>qi', timestamp, len(key_bytes)))
        buf.write(key_bytes)
        buf.write(struct.pack('>i', len(value_bytes)))
        buf.write(value_bytes)
        result_bytes = buf.getvalue()
        buf.close()
        return result_bytes

    def create_partition_key_msg(self, partition_key, topic, timestamp, \
            key_bytes, value_bytes):
        topic_bytes = bytes(topic)

        if len(topic_bytes) > BruceMsgCreator.getMaxTopicSize():
            raise BruceTopicTooLarge()

        msg_size = BruceMsgCreator.PARTITION_KEY_FIXED_BYTES + \
                len(topic_bytes) + len(key_bytes) + len(value_bytes)

        if msg_size > BruceMsgCreator.getMaxMsgSize():
            raise BruceMsgTooLarge()

        buf = io.BytesIO()
        flags = 0
        buf.write(struct.pack('>ihhhih', msg_size,
                BruceMsgCreator.PARTITION_KEY_API_KEY,
                BruceMsgCreator.PARTITION_KEY_API_VERSION, flags,
                partition_key, len(topic_bytes)))
        buf.write(topic_bytes)
        buf.write(struct.pack('>qi', timestamp, len(key_bytes)))
        buf.write(key_bytes)
        buf.write(struct.pack('>i', len(value_bytes)))
        buf.write(value_bytes)
        result_bytes = buf.getvalue()
        buf.close()
        return result_bytes
