'''
Created on Mar 3, 2016

@author: cmills
'''

import collections
import json
import avro.schema
from tasr.registered_schema import MasterAvroSchema


def ordered_json_obj(json_str):
    return json.loads(json_str, object_pairs_hook=collections.OrderedDict)


class RedshiftMasterAvroSchema(MasterAvroSchema):
    def __init__(self, slist=None):
        super(RedshiftMasterAvroSchema, self).__init__(slist)

    def rs_json_obj(self, group_name=None):
        schema_str = self.rs_master_schema_string(group_name)
        json_obj = json.loads(schema_str,
                              object_pairs_hook=collections.OrderedDict)
        return json_obj

    def rs_master_schema_string(self, group_name=None):
        '''This generates the RedShift-specific form of the master Avro schema.
        The RS form has the event type prefixes removed from the event-specific
        field names, kvpairs turned into a json string, and other complex types
        removed.'''
        master_schema_str = json.dumps(self.json_obj)
        if not master_schema_str and len(master_schema_str) > 0:
            return None

        master_schema = avro.schema.parse(master_schema_str)

        m_name = master_schema.name
        m_namespace = "%s.redshift" % master_schema.namespace
        m_type = master_schema.type

        # preserve the field order from the master schema
        m_fields = collections.OrderedDict()
        for msf in master_schema.fields:
            if '__' in msf.name:
                prefix = msf.name[:(msf.name.index('__') + 2)]
                if prefix in ('source__', 'meta__'):
                    m_fields[msf.name] = msf
                elif group_name and len(group_name) > 0:
                    # with a group name set, only clip the group name prefix
                    if prefix[:-2] == group_name:
                        m_fields[msf.name[len(prefix):]] = msf
                    else:
                        m_fields[msf.name] = msf
                else:
                    # with no group name set, clip all not source or meta
                    m_fields[msf.name[len(prefix):]] = msf
            else:
                # with no identifiable prefix, pass straight through
                m_fields[msf.name] = msf

        env = dict()
        env[m_name] = u'"%s"' % m_name

        # build the RedShift schema
        skip_comma = True
        mss = (u'{"name":"%s","namespace":"%s","type":"%s","fields":[' %
               (m_name, m_namespace, m_type))
        for mfname, mf in m_fields.iteritems():
            if skip_comma:
                skip_comma = False
            else:
                mss += u','

            if mfname == 'meta__kvpairs':
                # special case for kvpairs
                mss += u'{"default":null,"name":"meta__kvpairs_json",'
                mss += u'"type":["null","string"]}'
            elif mfname == 'meta__topic_name':
                # special case exclude
                skip_comma = True
            elif mf.type.type in avro.schema.PRIMITIVE_TYPES:
                # primitive types are all OK
                mss += (u'{"name":"%s","type":"%s"}' % (mfname, mf.type.type))
            elif mf.type.type == u'union':
                # leave out complex unions other than kvpairs
                complex_union = False
                non_null_type = None
                for subtype in mf.type._schemas:
                    if not subtype.type in avro.schema.PRIMITIVE_TYPES:
                        complex_union = True
                        break
                    else:
                        if not subtype.type == 'null':
                            non_null_type = subtype.type
                if complex_union:
                    skip_comma = True
                else:
                    mss += (u'{"default":null,"name":"%s","type":%s}' %
                            (mfname, '["null", "%s"]' % non_null_type))
            else:
                # leave out all non-union complex types
                skip_comma = True
        # add a 'dt' field of type 'string'
        if not skip_comma:
            mss += ','
        mss += '{"name":"dt","type":"string"}'
        mss += u']}'
        return mss
