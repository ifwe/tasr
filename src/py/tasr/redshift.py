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
    '''
    This class extends the MasterAvroSchema, providing support for several RS-
    specific needs.  Everything in RS is based on the master Avro schema, so
    these methods should be seen as views or re-formatting of that.
    '''
    def __init__(self, slist=None):
        super(RedshiftMasterAvroSchema, self).__init__(slist)

    def get_master_schema_object(self):
        '''Grab the master schema as a string, then parse it and return that.
        This ensures that the field order matches what is returned for the
        current master endpoint.'''
        master_schema_str = json.dumps(self.json_obj)
        if not master_schema_str and len(master_schema_str) > 0:
            return None
        return avro.schema.parse(master_schema_str)

    def get_config_array(self, group, key):
        '''A helper method to simplify behavior conditioned on config vals.'''
        if key in group.config:
            return json.loads(group.config[key])
        return []

    def get_uid_field_name(self, group):
        uid = 'user_id'
        if 'segmentation.user_id' in group.config:
            uid = group.config['segmentation.user_id']
        # handle prefix if present
        if uid and '__' in uid:
            prefix = uid[:(uid.index('__') + 2)]
            if prefix not in ('source__', 'meta__'):
                uid = uid[len(prefix):]
        return uid

    def rs_avro_type_string(self, field):
        '''RS can import directly from Avro files -- with some caveats.  This
        method allows us to map fields from our master schemas into RS-safe
        field types based on the field names and (original) types.'''
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

    def rs_ddl_type_string(self, field):
        '''This method maps Avro schema fields from our master schemas to RS-
        native data types used in DDL statements (i.e. -- CREATE and ALTER).'''
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

    def rs_name_to_field_map(self, group):
        '''This returns an ordered dict with RS-specific field names as keys
        and unmodified Avro field objects as values.  The field order is
        preserved from the master Avro schema.'''
        g_name = group.name
        if 's_' in g_name:
            g_name = g_name[2:]

        nf_map = collections.OrderedDict()
        for msf in self.get_master_schema_object().fields:
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
        '''Called to get the JSON object to return with the object_response()
        app method.  This preserves the field order from the master schema.'''
        return json.loads(self.rs_master_schema_string(group),
                          object_pairs_hook=collections.OrderedDict)

    def rs_master_schema_string(self, group):
        '''This generates the RedShift-specific form of the master Avro schema.
        The RS form has the event type prefixes removed from the event-specific
        field names, kvpairs turned into a json string, and other complex types
        removed.

        This is the Avro schema used to serialize events added from the hourly
        archives.  It does _not_ match the native master Avro schema.'''
        master_schema = self.get_master_schema_object()

        m_name = master_schema.name
        m_namespace = "%s.redshift" % master_schema.namespace
        m_type = master_schema.type

        nf_map = self.rs_name_to_field_map(group)
        env = dict()
        env[m_name] = u'"%s"' % m_name

        # build the RedShift schema
        sec_ts_fields = self.get_config_array(group,
                                              'redshift.sec_timestamp_fields')
        ms_ts_fields = self.get_config_array(group,
                                             'redshift.ms_timestamp_fields')

        skip_comma = True
        mss = (u'{"name":"%s","namespace":"%s","type":"%s","fields":[' %
               (m_name, m_namespace, m_type))
        for rs_name, field in nf_map.iteritems():
            if skip_comma:
                skip_comma = False
            else:
                mss += u','

            # 'in' also catches substrings, so we need to loop with '=='
            if rs_name in (sec_ts_fields + ms_ts_fields):
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

        # add an optional RS batch ID
        mss += '{"default":null,"name":"redshift__batch_id",'
        mss += '"type":["null","string"]},'

        # finally, add an MD5 event hash field of type 'bigint' that acts as a
        # primary key in the RedShift event tables.
        mss += '{"name":"redshift__event_md5_hash","type":"long"}'
        mss += u']}'
        return mss

    def generate_rs_create_statement(self, group, schema,
                                     rs_schemaname='ramblas', convert_ts=True):
        g_name = group.name
        if 's_' in g_name:
            g_name = g_name[2:]

        sec_ts_fields = self.get_config_array(group,
                                              'redshift.sec_timestamp_fields')
        ms_ts_fields = self.get_config_array(group,
                                             'redshift.ms_timestamp_fields')
        string_4k_fields = self.get_config_array(group,
                                                 'redshift.4k_string_fields')
        string_64k_fields = self.get_config_array(group,
                                                 'redshift.64k_string_fields')

        # figure user ID field to use as distkey
        uid_confirmed = False
        uid = self.get_uid_field_name(group)

        sort_fields = []
        t_name = '%s.%s_event' % (rs_schemaname, g_name)
        create_stmt = u'CREATE TABLE IF NOT EXISTS %s(' % t_name
        skip_comma = True
        for field in schema.fields:
            if field.name == uid:
                uid_confirmed = True
            if skip_comma:
                skip_comma = False
            else:
                create_stmt += u','

            if field.name == 'dt':
                sort_fields.append(field.name)

            # now add the actual field definition
            if convert_ts and field.name in (sec_ts_fields + ms_ts_fields):
                create_stmt += u'%s timestamp' % field.name
            else:
                ddl_type = self.rs_ddl_type_string(field)
                if ddl_type:
                    create_stmt += u'%s %s' % (field.name, ddl_type)
                    if ddl_type == 'varchar':
                        if field.name in string_4k_fields:
                            create_stmt += u'(4096)'
                        elif field.name in string_64k_fields:
                            create_stmt += u'(65535)'
                else:
                    skip_comma = True
        create_stmt += ')'
        if uid and uid_confirmed:
            create_stmt += 'distkey(%s)' % uid
        if len(sort_fields) > 0:
            ss = ''
            first = True
            for sf in sort_fields:
                if first:
                    first = False
                else:
                    ss += ','
                ss += sf
            create_stmt += 'compound sortkey(%s)' % ss
        create_stmt += ';'
        return create_stmt

    def rs_ddl_create(self, group):
        '''This generates the CREATE TABLE DDL statement that can be run in
        RedShift to create the table for the group based on the most current
        RedShift-specific master schema.  It works from the RS-specific master
        Avro schema.'''

        # work from the RS-specific master schema string
        rs_schema = avro.schema.parse(self.rs_master_schema_string(group))
        return self.generate_rs_create_statement(group, rs_schema)

    def rs_ddl_alter(self, group, old_fields=None):
        '''This method starts with the CREATE DDL statement.  If no fields are
        passed as pre-existing, we just return the CREATE.  If there are old
        fields, we return a set of ALTER statements (separated by newlines)
        that will add the missing fields.'''
        create = self.rs_ddl_create(group)
        if not old_fields:
            return create

        g_name = group.name
        if 's_' in g_name:
            g_name = g_name[2:]

        colstr = None
        if ')distkey' in create:
            colstr = create[(create.index('(') + 1):create.index(')distkey')]
        elif ')compound' in create:
            colstr = create[(create.index('(') + 1):create.index(')compound')]
        else:
            raise RuntimeError('Unparseable create.')
        cols = colstr.split(',')
        rval = ''
        for col in cols:
            if not col.split()[0] in old_fields:
                if len(rval) > 0:
                    rval += '\n'
                rval += ('ALTER TABLE ramblas.%s_event ADD COLUMN %s' %
                         (g_name, col))
        return rval

    def rs_ddl_create_staging(self, group):
        '''This generates a CREATE DDL statement for a staging table in RS.  A
        staging table matches the field names and types of the native master
        Avro schema, allowing Avro files copied directly from our HDFS cluter
        to be loaded with a COPY statement.  This is important for handling
        backfill jobs, but should be avoided for regular pipelines as the
        staging step should not be needed.'''

        # work from the RS-specific master schema string
        ddl = self.rs_ddl_drop_staging(group)
        ddl += '\n'
        master_schema = self.get_master_schema_object()
        ddl += self.generate_rs_create_statement(group, master_schema,
                                                 'staging', False)
        return ddl

    def rs_ddl_drop_staging(self, group):
        '''This generates a DROP DDL statement for a staging table in RS.'''
        g_name = group.name
        if 's_' in g_name:
            g_name = g_name[2:]
        return 'DROP TABLE IF EXISTS staging.%s_event;' % g_name

    def rs_dml_insert_from_staging(self, group):
        '''This generates a DML statement to insert all the rows in a staging
        table into the main (non-staging) table, with the required conversions
        for "ms since the epoch" bigint fields to timestamp fields.'''

        g_name = group.name
        if 's_' in g_name:
            g_name = g_name[2:]

        sec_ts_fields = self.get_config_array(group,
                                              'redshift.sec_timestamp_fields')
        ms_ts_fields = self.get_config_array(group,
                                             'redshift.ms_timestamp_fields')

        insert_stmt = 'INSERT INTO ramblas.%s_event(SELECT' % g_name
        skip_comma = True
        master_schema = self.get_master_schema_object()
        for field in master_schema.fields:
            if skip_comma:
                skip_comma = False
            else:
                insert_stmt += u','

            if field.name in (sec_ts_fields + ms_ts_fields):
                insert_stmt += u' (TIMESTAMP \'epoch\' + (%s' % field.name
                if field.name in ms_ts_fields:
                    insert_stmt += u' / 1000'
                insert_stmt += u') * INTERVAL \'1 Second\')'
            else:
                insert_stmt += u' %s' % field.name
        insert_stmt += ' FROM staging.%s_event);' % g_name

        # prepend a conditional create to ensure the target table is there
        create_stmt = self.rs_ddl_create(group)
        return '%s\n%s' % (create_stmt, insert_stmt)
