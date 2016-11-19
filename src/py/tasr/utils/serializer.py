'''
Created on Oct 6, 2016

@author: cmills
'''
import logging
import base64
import io
import struct
import avro.io
import avro.schema
import requests
from tasr.headers import SchemaHeaderBot


class MTLeader(object):
    '''Simplify access to the features of a MT message leader.'''
    def __init__(self, msg_bytes):
        '''Construct a leader object from raw bytes of a multitype message.'''
        msg = bytearray(msg_bytes[:33])
        self.flag_byte = int(msg[0])
        self.version_number = None
        self.sha256_id = None
        if self.flag_byte == MTSerDe.SV1:
            self.version_number = int(msg[1])
        elif self.flag_byte == MTSerDe.SV2:
            self.version_number = struct.unpack('>H', msg[1:3])[0]
        elif self.flag_byte == MTSerDe.SHA256:
            self.sha256_id = base64.b64encode(bytes(msg))


class MTSerDe(object):
    '''
    Common superclass for serializer and deserializer classes supporting the
    multi-type leaders.
    '''
    SHA256 = 0x20
    SV1 = 0x01
    SV2 = 0x02

    def __init__(self, sha256_id=None, topic=None, version_number=None,
                 tasr_url='http://tasr.tagged.com', tasr_app=None):
        '''
        The schema must be registered in TASR and must be identified uniquely.
        Your two choices are:
        - an SHA-256 schema ID
        - a topic and version number
        '''
        self.tasr_url = tasr_url
        self.tasr_app = tasr_app
        self.topic = None
        self.version_number = None
        self.sha256_id = None
        self.schema_json = None
        self.schema = None
        self.sha256_bytes = None
        self.sv1_bytes = None
        self.sv2_bytes = None
        if sha256_id:
            self.sha256_id = sha256_id
            self.topic_versions = dict()
            url = self.tasr_url + '/tasr/id/' + self.sha256_id
            resp = None
            if self.tasr_app is None and self.tasr_url is not None:
                # get the schema from a live TASR via the URL
                resp = requests.get(url)
            elif self.tasr_app is not None:
                # get the schema from a passed app (testing)
                resp = self.tasr_app.request(url, method='GET')
                resp.charset = 'utf8'  # ensuer we can access response.text
            if resp.status_code == 200:
                rs_meta = SchemaHeaderBot.extract_metadata(resp)
                for subject in rs_meta.group_names:
                    if subject not in self.topic_versions:
                        self.topic_versions[subject] = []
                    ver = rs_meta.group_version(subject)
                    self.topic_versions[subject].append(ver)
                # use the first topic as the topic if there are more
                self.topic = self.topic_versions.keys()[0]
                # use the last version for the topic as the topic
                self.version_number = self.topic_versions[self.topic][-1]
                self.schema_json = resp.text
            else:
                raise RuntimeError('TASR request failed, status code %s',
                                   resp.status_code)
        elif topic and version_number:
            self.topic = topic
            url = None
            if version_number > 0:
                url = ('%s/tasr/subject/%s/version/%s' %
                       (self.tasr_url, self.topic, version_number))
            else:
                logging.warn('Grabbing latest version for %s', self.topic)
                url = ('%s/tasr/subject/%s/latest' %
                       (self.tasr_url, self.topic))
            resp = None
            if self.tasr_app is None and self.tasr_url is not None:
                # get the schema from a live TASR via the URL
                resp = requests.get(url)
            elif self.tasr_app is not None:
                # get the schema from a passed app (testing)
                resp = self.tasr_app.request(url, method='GET')
            if resp.status_code == 200:
                self.version_number = int(resp.headers['X-Tasr-Schema-Version'])
                self.sha256_id = resp.headers['X-Tasr-Schema-Sha256']
                self.schema_json = resp.text
            else:
                raise RuntimeError('TASR request failed, status code %s',
                                   resp.status_code)
        else:
            raise RuntimeError('Must define either sha256_id or both topic ' +
                               'and version_number.')

    @property
    def sha256_leader(self):
        '''Generate the SHA-256 multitype message header as (33) bytes.'''
        if not self.sha256_bytes:
            # decoded ID string already includes MTSerializer.SHA256 and bytes
            self.sha256_bytes = base64.b64decode(self.sha256_id)
        return self.sha256_bytes

    @property
    def sv1_leader(self):
        '''Generate the SV1 multitype message header as (2) bytes.'''
        if not self.sv1_bytes:
            buf = io.BytesIO()
            buf.write(struct.pack('>b', MTSerializer.SV1))
            buf.write(struct.pack('>b', self.version_number))
            self.sv1_bytes = buf.getvalue()
        return self.sv1_bytes

    @property
    def sv2_leader(self):
        '''Generate the SV2 multitype message header as (3) bytes.'''
        if not self.sv2_bytes:
            buf = io.BytesIO()
            buf.write(struct.pack('>b', MTSerializer.SV2))
            buf.write(struct.pack('>H', self.version_number))
            self.sv2_bytes = buf.getvalue()
        return self.sv2_bytes

    def __repr__(self):
        return '%s[%s,%s,%s]' % (self.__class__.__name__, self.topic,
                                 self.version_number, self.sha256_id)

    def __str__(self):
        return '%s[%s,%s,%s]' % (self.__class__.__name__, self.topic,
                                 self.version_number, self.sha256_id)


class MTDeserializer(MTSerDe):
    '''
    The MTDeserializer class is initialized for a specific schema, passed as an
    SHA-256 id, a topic and version number, or as the actual schema JSON (as a
    string). Once initialized, the object can take serialized message bytes and
    produce a more easily accessed and used dict version of the event.
    '''
    def __init__(self, sha256_id=None, topic=None, version_number=None,
                 tasr_url='http://tasr.tagged.com', tasr_app=None):
        super(MTDeserializer, self).__init__(sha256_id=sha256_id,
                                             topic=topic,
                                             version_number=version_number,
                                             tasr_url=tasr_url,
                                             tasr_app=tasr_app)
        self.reader = None

    def validate_mt_message(self, msg_bytes):
        '''Check that the message header matches the object's schema. If not,
        raise an exception. If so, return the (headless) event bytes.'''
        msg = bytearray(msg_bytes)
        if int(msg[0]) == MTSerDe.SV1:
            # 1 byte s+v leader, if ver is right, return remaining bytes
            if int(msg[1]) == self.version_number:
                return bytes(msg[2:])
            else:
                raise RuntimeError('SV1-type ID mismatch: %s is not %s' %
                                   (int(msg[1]), self.version_number))
        elif int(msg[0]) == MTSerDe.SV2:
            # 2 byte s+v leader, unpack next 2 bytes for message version number
            msg_version_number = struct.unpack('>H', msg[1:3])[0]
            # if ver is right, return remaining bytes
            if msg_version_number == self.version_number:
                return bytes(msg[3:])
            else:
                raise RuntimeError('SV2-type ID mismatch: %s is not %s' %
                                   (msg_version_number, self.version_number))
        elif int(msg[0]) == MTSerDe.SHA256:
            # check whole 33 byte leader against stored SHA256 leader
            if msg[:33] == self.sha256_bytes:
                return bytes(msg[33:])
            else:
                raise RuntimeError('SHA-type ID mismatch: %s is not %s' %
                                   (msg[:33], self.sha256_bytes))
        # if we get this far, the MT leader was not valid, so raise an error
        raise RuntimeError('Bad leader type: %s', hex(msg[0]))

    def mt_message_to_dict(self, msg_bytes):
        '''Deserialize a block of bytes into an event dict.'''
        if not self.schema:
            self.schema = avro.schema.parse(self.schema_json)
        if not self.reader:
            self.reader = avro.io.DatumReader(self.schema)
        event_bytes = self.validate_mt_message(msg_bytes)
        decoder = avro.io.BinaryDecoder(io.BytesIO(event_bytes))
        return self.reader.read(decoder)

    def get_serializer(self):
        '''Convenience method to get a serializer for the same schema.'''
        return MTSerializer(sha256_id=self.sha256_id)


class MTSerializer(MTSerDe):
    '''
    The MTSerializer class is initialized for a specific schema, passed as an
    SHA-256 id, a topic and version number, or as the actual schema JSON (as a
    string). Once initialized, the object can produce serialized messages
    returned as byte arrays with or without schema identifying headers.
    '''

    def __init__(self, sha256_id=None, topic=None, version_number=None,
                 tasr_url='http://tasr.tagged.com', tasr_app=None):
        super(MTSerializer, self).__init__(sha256_id=sha256_id,
                                           topic=topic,
                                           version_number=version_number,
                                           tasr_url=tasr_url,
                                           tasr_app=tasr_app)
        self.writer = None

    def serialize_event(self, event_dict):
        '''Serialize the passed dict using the object's Avro schema.  The bytes
        of the serialized message are returned.'''
        if not self.schema:
            self.schema = avro.schema.parse(self.schema_json)
        if not self.writer:
            self.writer = avro.io.DatumWriter(self.schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        self.writer.write(event_dict, encoder)
        raw_bytes = bytes_writer.getvalue()
        return raw_bytes

    def dict_to_mt_message(self, event_dict, leader=0x02):
        '''Generate a complete multitype message from an event dict, including
        the leader bytes and the bytes of the Avro-serialized event.'''
        mbytes = self.serialize_event(event_dict)
        if leader == MTSerDe.SV1:
            return self.sv1_leader + mbytes
        elif leader == MTSerDe.SV2:
            return self.sv2_leader + mbytes
        elif leader == MTSerDe.SHA256:
            return self.sha256_leader + mbytes
        raise RuntimeError('Bad leader type: %s', leader)

    def get_deserializer(self):
        '''Convenience method to get a deserializer for the same schema.'''
        return MTDeserializer(sha256_id=self.sha256_id)
