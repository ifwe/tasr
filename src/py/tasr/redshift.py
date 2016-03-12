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

    def get_master_schema(self):
        master_schema_str = json.dumps(self.json_obj)
        if not master_schema_str and len(master_schema_str) > 0:
            return None
        return avro.schema.parse(master_schema_str)

    def guess_group_name_from_schema(self):
        for msf in self.get_master_schema().fields:
            if '__' in msf.name:
                prefix = msf.name[:(msf.name.index('__') + 2)]
                if prefix not in ('source__', 'meta__'):
                    return prefix[:-2]

    def rs_avro_type_string(self, field):
        if field.name == 'meta__kvpairs':
            # special case for kvpairs
            return u'["null","string"]'
        elif field.name == 'meta__topic_name':
            # special case exclude
            return None
        elif field.type.type in avro.schema.PRIMITIVE_TYPES:
            # primitive types are all OK
            return field.type.type
        elif field.type.type == u'union':
            # leave out complex unions other than kvpairs
            complex_union = False
            non_null_type = None
            for subtype in field.type._schemas:
                if not subtype.type in avro.schema.PRIMITIVE_TYPES:
                    complex_union = True
                    break
                else:
                    if not subtype.type == 'null':
                        non_null_type = subtype.type
            if not complex_union:
                return '["null", "%s"]' % non_null_type

    def rs_dml_type_string(self, field):
        if field.name == 'meta__kvpairs':
            # special case for kvpairs
            return u'varchar(4096)'
        elif field.name == 'dt':
            # special case
            return u'timestamp'
        elif field.name == 'meta__topic_name':
            # special case exclude
            return None
        else:
            non_null_type = field.type.type
            if field.type.type == u'union':
                # leave out complex unions other than kvpairs
                complex_union = False
                for subtype in field.type._schemas:
                    if not subtype.type in avro.schema.PRIMITIVE_TYPES:
                        complex_union = True
                        break
                    else:
                        if not subtype.type == 'null':
                            non_null_type = subtype.type
                if complex_union:
                    return None

            if non_null_type in avro.schema.PRIMITIVE_TYPES:
                # map the avro type to an RS type
                if non_null_type in ('int', 'boolean'):
                    return non_null_type
                elif non_null_type == u'long':
                    return 'bigint'
                elif non_null_type == u'double':
                    return 'float'
                elif non_null_type == u'string':
                    return 'varchar'

    def get_name_to_field_map(self, group):
        # preserve the field order from the master schema
        g_name = group.name
        if 's_' in g_name:
            g_name = g_name[2:]

        nf_map = collections.OrderedDict()
        for msf in self.get_master_schema().fields:
            rs_field_name = msf.name
            # clip the 's_' first if present
            if 's_' in rs_field_name:
                rs_field_name = rs_field_name[2:]
            if '__' in msf.name:
                prefix = msf.name[:(msf.name.index('__') + 2)]
                if not prefix in ('source__', 'meta__'):
                    if prefix[:-2] == g_name:
                        rs_field_name = msf.name[len(prefix):]
            nf_map[rs_field_name] = msf
        return nf_map

    def rs_json_obj(self, group):
        return json.loads(self.rs_master_schema_string(group),
                          object_pairs_hook=collections.OrderedDict)

    def rs_master_schema_string(self, group):
        '''This generates the RedShift-specific form of the master Avro schema.
        The RS form has the event type prefixes removed from the event-specific
        field names, kvpairs turned into a json string, and other complex types
        removed.'''
        master_schema = self.get_master_schema()

        m_name = master_schema.name
        m_namespace = "%s.redshift" % master_schema.namespace
        m_type = master_schema.type

        nf_map = self.get_name_to_field_map(group)
        env = dict()
        env[m_name] = u'"%s"' % m_name

        # build the RedShift schema
        sec_ts_fields = []
        if 'redshift.sec_timestamp_fields' in group.config:
            sec_ts_fields = group.config['redshift.sec_timestamp_fields']
        ms_ts_fields = []
        if 'redshift.ms_timestamp_fields' in group.config:
            ms_ts_fields = group.config['redshift.ms_timestamp_fields']

        skip_comma = True
        mss = (u'{"name":"%s","namespace":"%s","type":"%s","fields":[' %
               (m_name, m_namespace, m_type))
        for rs_name, field in nf_map.iteritems():
            if skip_comma:
                skip_comma = False
            else:
                mss += u','

            if rs_name in sec_ts_fields or rs_name in ms_ts_fields:
                # handle timestamp conversion fields explicitly
                mss += ('{"default":null,"name":"%s","type":%s}' %
                        (rs_name, '["null", "string"]'))
                continue
            # everything else handled here
            type_str = self.rs_avro_type_string(field)
            if not type_str:
                skip_comma = True
            elif type_str and field.type.type in avro.schema.PRIMITIVE_TYPES:
                mss += u'{"name":"%s","type":"%s"}' % (rs_name, type_str)
            else:
                mss += (u'{"default":null,"name":"%s","type":%s}' %
                        (rs_name, type_str))

        # add a 'dt' field of type 'string' expected to hold an ISO date string
        # generated from the source__timestamp
        if not skip_comma:
            mss += ','
        mss += '{"name":"dt","type":"string"},'

        # finally, add an MD5 event hash field of type 'bigint' that acts as a
        # primary key in the RedShift event tables.
        mss += '{"name":"redshift__event_md5_hash","type":"long"}'
        mss += u']}'
        return mss

    def rs_dml_create(self, group):
        '''This generates the CREATE TABLE DML statement that can be run in
        RedShift to create the table for the group based on the most current
        RedShift-specific master schema.'''

        sec_ts_fields = []
        if 'redshift.sec_timestamp_fields' in group.config:
            sec_ts_fields = group.config['redshift.sec_timestamp_fields']
        ms_ts_fields = []
        if 'redshift.ms_timestamp_fields' in group.config:
            ms_ts_fields = group.config['redshift.ms_timestamp_fields']
        string_4k_fields = []
        if 'redshift.4k_string_fields' in group.config:
            string_4k_fields = group.config['redshift.4k_string_fields']
        string_64k_fields = []
        if 'redshift.64k_string_fields' in group.config:
            string_4k_fields = group.config['redshift.64k_string_fields']

        # figure user ID field to use as distkey
        uid = 'user_id'
        if 'segmentation.user_id' in group.config:
            uid = group.config['segmentation.user_id']
        # handle prefix if present
        if uid and '__' in uid:
            prefix = uid[:(uid.index('__') + 2)]
            if prefix not in ('source__', 'meta__'):
                uid = uid[len(prefix):]

        rs_master_json = self.rs_master_schema_string(group)
        rs_master_schema = avro.schema.parse(rs_master_json)
        sort = []
        create_statement = u'CREATE TABLE %s_event(' % group.name
        skip_comma = True
        for field in rs_master_schema.fields:
            if skip_comma:
                skip_comma = False
            else:
                create_statement += u','

            if field.name == 'dt':
                sort.append(field.name)

            # now add the actual field definition
            if field.name in sec_ts_fields or field.name in ms_ts_fields:
                create_statement += u'%s timestamp' % field.name
            else:
                dml_type = self.rs_dml_type_string(field)
                if dml_type:
                    create_statement += u'%s %s' % (field.name, dml_type)
                    if field.name in string_4k_fields:
                        create_statement += u'(4096)'
                    elif field.name in string_64k_fields:
                        create_statement += u'(65535)'
                else:
                    skip_comma = True
        create_statement += ')'
        if uid:
            create_statement += 'distkey(%s)' % uid
        if len(sort) > 0:
            ss = ''
            first = True
            for sf in sort:
                if first:
                    first = False
                else:
                    ss += ','
                ss += sf
            create_statement += 'compound sortkey(%s)' % ss
        create_statement += ';'
        return create_statement

