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


class MTSerializer(object):
    '''
    The MTSerializer class is initialized for a specific schema, passed as an
    SHA-256 id, a topic and version number, or as the actual schema JSON (as a
    string). Once initialized, the object can produce serialized messages
    returned as byte arrays with or without schema identifying headers.
    '''

    SHA256 = 0x20
    SV1 = 0x01
    SV2 = 0x02

    def __init__(self, sha256_id=None, topic=None, version_number=None,
                 tasr_url='http://tasr.tagged.com'):
        '''
        The schema must be registered in TASR and must be identified uniquely.
        Your two choices are:
        - an SHA-256 schema ID
        - a topic and version number
        '''
        self.tasr_url = tasr_url
        self.topic = None
        self.version_number = None
        self.sha256_id = None
        self.schema_json = None
        self.schema = None
        self.writer = None
        self.sha256_bytes = None
        self.sv1_bytes = None
        self.sv2_bytes = None
        if sha256_id:
            self.sha256_id = sha256_id
            self.topic_versions = dict()
            r = requests.get(self.tasr_url + '/tasr/id/' + self.sha256_id)
            if r.status_code == 200:
                logging.info('headers: %s', r.headers)
                svl = r.headers['X-Tasr-Schema-Subject-Version-Map'].split(',')
                for sv in svl:
                    sva = sv.split('=', 1)
                    subject = sva[0]
                    version = int(sva[1])
                    if subject not in self.topic_versions:
                        self.topic_versions[subject] = []
                    self.topic_versions[subject].append(version)
                # use the first topic as the topic if there are more
                self.topic = self.topic_versions.keys()[0]
                # use the last version for the topic as the topic
                self.version_number = self.topic_versions[self.topic][-1]
                self.schema_json = r.text
            else:
                raise RuntimeError('TASR request failed, status code %s',
                                   r.status_code)
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
            r = requests.get(url)
            if r.status_code == 200:
                self.version_number = int(r.headers['X-Tasr-Schema-Version'])
                self.sha256_id = r.headers['X-Tasr-Schema-Sha256']
                self.schema_json = r.text
            else:
                raise RuntimeError('TASR request failed, status code %s',
                                   r.status_code)
        else:
            raise RuntimeError('Must define either sha256_id or both topic ' +
                               'and version_number.')

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

    @property
    def sha256_leader(self):
        '''Generate the SHA-256 multitype message header as bytes.'''
        if not self.sha256_bytes:
            # decoded ID already includes MTSerializer.SHA256 and bytes
            self.sha256_bytes = base64.b64decode(self.sha256_id)
        return self.sha256_bytes

    @property
    def sv1_leader(self):
        '''Generate the SV1 multitype message header as bytes.'''
        if not self.sv1_bytes:
            buf = io.BytesIO()
            buf.write(struct.pack('>b', MTSerializer.SV1))
            buf.write(struct.pack('>b', self.version_number))
            self.sv1_bytes = buf.getvalue()
        return self.sv1_bytes

    @property
    def sv2_leader(self):
        '''Generate the SV2 multitype message header as bytes.'''
        if not self.sv2_bytes:
            buf = io.BytesIO()
            buf.write(struct.pack('>b', MTSerializer.SV2))
            buf.write(struct.pack('>H', self.version_number))
            self.sv2_bytes = buf.getvalue()
        return self.sv2_bytes

    def mt_message(self, event_dict, leader=0x02):
        '''Generate a complete multitype message from an event dict, including
        the leader bytes and the bytes of the Avro-serialized event.'''
        mbytes = self.serialize_event(event_dict)
        if leader == MTSerializer.SV1:
            return self.sv1_leader + mbytes
        elif leader == MTSerializer.SV2:
            return self.sv2_leader + mbytes
        elif leader == MTSerializer.SHA256:
            return self.sha256_leader + mbytes
        raise RuntimeError('Bad leader type: %s', leader)
