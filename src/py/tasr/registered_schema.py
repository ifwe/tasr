'''
Created on Apr 2, 2014

@author: cmills
'''

import hashlib
import io
import struct
import base64
import binascii
import avro.schema
import collections
import json
import logging

MD5_BYTES = 16
SHA256_BYTES = 32


class SchemaMetadata(object):
    '''A structured place to hold schema-related metadata.  This is a helper
    class for the RegisteredSchema class.  Note that this class does not hold
    an actual schema, just metadata about a schema.  That means the SHA256 and
    MD5 based IDs are snapshots, not calculated live.
    '''
    def __init__(self, meta_dict=None):
        self.sha256_id = None
        self.md5_id = None
        self.gv_dict = dict()
        self.ts_dict = dict()
        if meta_dict:
            self.update_from_dict(meta_dict)

    def update_from_dict(self, meta_dict):
        '''Set the metadata values from a dict.'''
        if not meta_dict:
            return
        if 'sha256_id' in meta_dict:
            self.sha256_id = meta_dict['sha256_id']
        if 'md5_id' in meta_dict:
            self.sha256_id = meta_dict['md5_id']
        for key, val in meta_dict.iteritems():
            if key.startswith('vid.'):
                try:
                    group_name = key[4:]
                    version = int(val)
                    self.gv_dict[group_name] = version
                except ValueError:
                    pass
            if key.startswith('vts.'):
                try:
                    group_name = key[4:]
                    timestamp = long(val)
                    self.ts_dict[group_name] = timestamp
                except ValueError:
                    pass

    def as_dict(self):
        '''Encapsulate the object values in a dict.'''
        meta_dict = dict()
        meta_dict['sha256_id'] = self.sha256_id
        meta_dict['md5_id'] = self.md5_id
        for key, value in self.gv_dict.iteritems():
            topic_key = 'vid.%s' % key
            meta_dict[topic_key] = value
        for key, value in self.ts_dict.iteritems():
            topic_key = 'vts.%s' % key
            meta_dict[topic_key] = value
        return meta_dict

    def group_version(self, group_name):
        '''Convenience accessor for the version number of a specified group.'''
        if group_name in self.gv_dict.keys():
            return int(self.gv_dict[group_name])

    def group_timestamp(self, group_name):
        '''Convenience accessor for the registration timestamp for a specified
        group.'''
        if group_name in self.ts_dict.keys():
            return long(self.ts_dict[group_name])

    @property
    def group_names(self):
        '''Access the group version list keys (group names) as a property.
        '''
        return self.gv_dict.keys()


class RegisteredSchema(object):
    '''The RegisteredSchema represents the data we have about how a given
    schema string is currently registered for known groups.  This object only
    holds the most recent topic-version intersections, so for the (unusual but
    allowed) case where a schema has been registered more than once for the
    same topic, only the most recent version will be included.  However, _all_
    groups for which the schema string has been registered are included, and
    must each indicate their most recent versions.

    The canonical schema string is a version with whitespace and other things
    that will not affect the parsing of the schema normalized.

    The IDs are derivative of the canonical schema string, so they are surfaced
    with @property methods.
    '''
    def __init__(self):
        self._schema_str = None
        self.gv_dict = dict()
        self.ts_dict = dict()
        self.created = False

    @property
    def schema_str(self):
        return self._schema_str

    @schema_str.setter
    def schema_str(self, value):
        self.clear_cache()
        self._schema_str = value

    def clear_cache(self):
        pass

    def update_from_dict(self, rs_dict):
        '''A dict containing a schema and topic-version and topic-timestamp
        entries can be used to update the RS fields.  Note that even if the
        dict contains 'sha256_id' and 'md5_id' fields, they will be ignored
        as the RS only exposes those as live values calculated from the schema.
        '''
        if rs_dict:
            self.schema_str = rs_dict.pop('schema', self.schema_str)
            self.update_from_schema_metadata(SchemaMetadata(rs_dict))

    def update_from_schema_metadata(self, metadata):
        '''Updates the topic-version and topic-timestamp fields in the RS
        object based on gv_dict and ts_dict passed in a SchemaMetadata object.
        '''
        if metadata:
            self.gv_dict.update(metadata.gv_dict)
            self.ts_dict.update(metadata.ts_dict)

    def as_schema_metadata(self):
        '''Creates a new SchemaMetadata object that contains a snapshot of the
        RS object's metadata (IDs, gv_dict and ts_dict).
        '''
        metadata = SchemaMetadata()
        metadata.sha256_id = self.sha256_id
        metadata.md5_id = self.md5_id
        metadata.gv_dict = self.gv_dict.copy()
        metadata.ts_dict = self.ts_dict.copy()
        return metadata

    def as_dict(self):
        '''Outputs the object as a dict.'''
        rs_dict = dict()
        rs_dict.update(self.as_schema_metadata().as_dict())
        rs_dict['schema'] = self.schema_str
        # overwrite the SHA256 and MD5 IDs with ones derived from the schema
        rs_dict['sha256_id'] = 'id.%s' % self.sha256_id
        rs_dict['md5_id'] = 'id.%s' % self.md5_id
        return rs_dict

    @property
    def canonical_schema_str(self):
        '''Generic canonicalization just normalizes whitespace.'''
        if not self.schema_str:
            return None
        elems = self.schema_str.split()
        return ' '.join(elems)

    @property
    def md5_id(self):
        '''Access the (base64'd) md5 as a property.
        '''
        return self.md5_id_base64

    @property
    def md5_id_base64(self):
        '''Access the base64'd md5 as a property.
        '''
        if self.canonical_schema_str == None:
            return None
        return base64.b64encode(self.md5_id_bytes)

    @property
    def md5_id_hex(self):
        '''Access the hex md5 as a property.
        '''
        if self.canonical_schema_str == None:
            return None
        return binascii.hexlify(self.md5_id_bytes)

    @property
    def md5_id_bytes(self):
        '''Access the md5 bytes as a property.
        '''
        if self.canonical_schema_str == None:
            return None
        buf = io.BytesIO()
        buf.write(struct.pack('>b', MD5_BYTES))
        md5 = hashlib.md5()
        md5.update(self.canonical_schema_str)
        buf.write(md5.digest())
        id_bytes = buf.getvalue()
        buf.close()
        return id_bytes

    @property
    def sha256_id(self):
        '''Access the (base64'd) sha256 as a property.
        '''
        return self.sha256_id_base64

    @property
    def sha256_id_base64(self):
        '''Access the base64'd sha256 as a property.
        '''
        if self.canonical_schema_str == None:
            return None
        return base64.b64encode(self.sha256_id_bytes)

    @property
    def sha256_id_hex(self):
        '''Access the hex sha256 as a property.
        '''
        if self.canonical_schema_str == None:
            return None
        return binascii.hexlify(self.sha256_id_bytes)

    @property
    def sha256_id_bytes(self):
        '''Access the sha256 bytes as a property.
        '''
        if self.canonical_schema_str == None:
            return None
        buf = io.BytesIO()
        buf.write(struct.pack('>b', SHA256_BYTES))
        sha = hashlib.sha256()
        sha.update(self.canonical_schema_str)
        buf.write(sha.digest())
        id_bytes = buf.getvalue()
        buf.close()
        return id_bytes

    @property
    def group_names(self):
        '''Access the topic list as a property.
        '''
        return self.gv_dict.keys()

    @property
    def is_valid(self):
        '''Access the schema validity as a boolean property.'''
        try:
            return self.validate_schema_str()
        except avro.schema.SchemaParseException:
            return False
        except:
            return False

    def validate_schema_str(self):
        '''The retrieval of the canonical str should do a validation.  So, if
        it comes back as None, it is either missing or bad.
        '''
        return self.schema_str != None

    def current_version(self, group_name):
        '''A convenience method to get the current version for a group
        associated with the schema.
        '''
        if group_name in self.gv_dict:
            return self.gv_dict[group_name]
        return None

    def current_version_timestamp(self, group_name):
        '''A convenience method to get the timestamp for when a group was
        associated with the schema.
        '''
        if group_name in self.ts_dict:
            return self.ts_dict[group_name]
        return None

    def __repr__(self):
        return u'%s[%s]' % (self.__class__.__name__, self.sha256_id)

    def __str__(self):
        return '%r' % self.schema_str

    def __eq__(self, other):
        '''Registered schemas are equal when the underlying canonical schema
        strings (and hence the SHA256 and or MD5 ids) are equal AND the topic/
        version mappings are the same.
        '''
        if not isinstance(other, RegisteredSchema):
            return False

        if not self.sha256_id == other.sha256_id:
            return False

        shared_set = set(self.gv_dict.items()) & set(other.gv_dict.items())
        if len(self.gv_dict) == len(shared_set):
            return True
        return False


def build_pcf(env, schema):
    '''Returns the "Parsing Canonical Form" (PCF) of a passed Avro schema
    object as defined in:

        http://avro.apache.org/docs/1.7.6/spec.html#Parsing+Canonical+Form+for+Schemas

    This is a recursive method. The PCF string should be used for calculating
    hashes and comparing versions as it removes non-functional elements and
    normalizes formatting.

    The PCF only keeps atrtibutes involved in parsing, that is: type, name,
    fields, symbols, items, values, size.  Note that default is NOT here. The
    PCF also enforces a canonical order of attributes for elements, namely:
    name, type, fields, symbols, items, values, size.
    '''

    pcf = None
    firstElem = True
    if env == None:
        env = dict()

    if schema.type in avro.schema.PRIMITIVE_TYPES:
        pcf = u'"%s"' % schema.type
    elif schema.type == u'union':
        pcf = u'['
        for us in schema.schemas:
            if firstElem:
                firstElem = False
            else:
                pcf += u','
            pcf += build_pcf(env, us)
        pcf += u']'
    elif schema.type == u'array':
        pcf = (u'{"type":"%s","items":%s}' %
               (schema.type, build_pcf(env, schema.items)))
    elif schema.type == u'map':
        pcf = (u'{"type":"%s","values":%s}' %
               (schema.type, build_pcf(env, schema.values)))
    elif schema.type in avro.schema.NAMED_TYPES:
        # covers enum, fixed, record, and error types
        pcf = ''
        name = schema.fullname
        if name in env:
            pcf += env[name]
            return pcf
        qname = u'"%s"' % name
        env[name] = qname
        pcf += u'{"name":%s,"type":"%s"' % (qname, schema.type)
        if schema.type == u'enum':
            pass
        elif schema.type == u'fixed':
            pass
        elif schema.type == u'record':
            pcf += u',"fields":['
            for sf in schema.fields:
                if firstElem:
                    firstElem = False
                else:
                    pcf += u','
                pcf += (u'{"name":"%s","type":%s}' %
                        (sf.name, build_pcf(env, sf.type)))
            pcf += ']'
        else:  # i.e. -- error type
            pass
        pcf += '}'
    return pcf


def ordered_json_obj(json_str):
    return json.loads(json_str, object_pairs_hook=collections.OrderedDict)


class RegisteredAvroSchema(RegisteredSchema):
    '''Adds an Avro schema validation function.'''
    def __init__(self):
        super(RegisteredAvroSchema, self).__init__()
        self.clear_cache()

    def clear_cache(self):
        self._schema = None
        self._pcf = None
        self._json_obj = None
        self._json = None

    @property
    def schema(self):
        if not self.schema_str:
            return None
        elif self.schema_str and not self._schema:
            try:
                self._schema = avro.schema.parse(self.schema_str)
            except avro.schema.SchemaParseException as spe:
                raise ValueError("Failed to parse schema: %s" % spe)
        return self._schema

    @property
    def pcf(self):
        '''Accessing PCF this way caches the build_pcf results.  Remember, the
        PCF strips defaults.'''
        if self.schema_str == None:
            return None
        if not self._pcf:
            self._pcf = build_pcf(dict(), self.schema)
        return self._pcf

    @property
    def json_obj(self):
        '''We want a JSON representation that keeps the defaults and other
        elements stripped from the PCF, but normalizes the key order to make
        comparisons and other processing easier.'''
        if not self.schema_str:
            return None
        if not self._json_obj:
            self._json_obj = collections.OrderedDict()
            self._json_obj["name"] = self.schema.fullname
            #self._json_obj["namespace"] = self.schema.namespace
            self._json_obj["type"] = self.schema.type

            # use a JSON object as an intermediary to order the field keys
            jo = json.loads(str(self.schema))
            fields = ordered_json_obj(json.dumps(jo["fields"], sort_keys=True))
            self._json_obj["fields"] = fields
        return self._json_obj

    @property
    def json(self):
        if not self.schema_str:
            return None
        if not self._json:
            self._json = json.dumps(self.json_obj)
        return self._json

    @property
    def canonical_schema_str(self):
        '''Alternate sig, also uses cached copy if available.'''
        return self.pcf

    def __str__(self):
        return self.schema_str

    def validate_schema_str(self):
        '''Always checks the actual schema string directly.'''
        if not super(RegisteredAvroSchema, self).validate_schema_str():
            return False
        try:
            avro.schema.parse(self.schema_str)
        except avro.schema.SchemaParseException as spe:
            raise ValueError("Bad schema: %s" % spe)
        return True

    def back_compatible_with(self, obj):
        '''Convenience method that obscures the use of the MasterAvroSchema in
        determining compatibility.'''
        if obj == None or (isinstance(obj, list) and len(obj) == 0):
            # If there is no "back", then it's compatible.
            return True
        try:
            mas = None
            if isinstance(obj, list):
                mas = MasterAvroSchema(obj)
            elif isinstance(obj, RegisteredAvroSchema):
                mas = MasterAvroSchema([obj, ])
            else:
                logging.debug('Can only be compatible with a RAS or a list.')
                return False
            # we have a master schema, so check compatibility with it
            return mas.is_compatible(self)
        except ValueError as err:
            logging.warn(err)
            return False


class MasterAvroSchema(RegisteredAvroSchema):
    '''A MasterAvroSchema (MAS) is an Avro schema constructed from a list of
    compatible Avro schema versions. By "compatible" here we mean that every
    field in each of the versions can be mapped to a field in the master, that
    fields in different versions may not have conflicting types, and that
    optional fields (those with a union type of a null plus a non-null type)
    have a default.

    For purposes of discussion, there are 3 kinds of field we care about:

        - REQ: primitive type, a required field (missing == invalid), no nulls
        - OPT: union type of null plus a primitive type, with a null default
        - FIXED: any other allowable Avro type

    The first schema version can have any valid combination of REQ, OPT and
    FIXED fields.  All subsequent schema versions are constrained by previous
    versions in the following ways:

        - Field order may change.  This will result in a different PCF, and a
            different hash-based ID.  But such a change _is_ compatible.
        - The PCF of FIXED fields must remain the same.  Defaults can change.
        - A REQ field may convert to an OPT where the REQ type is kept as the
            non-null type in the OPT type union.  There must be a null default.
        - REQ and OPT fields may be deleted (REQ->OPT is implicit for REQ)
        - Deleted fields may return as OPT fields when the non-null field stays
            the same.

    From the sequence of compatible schema versions we can construct a master
    schema (the MAS) which will have ALL the fields that have been in any
    version in the sequence.  If a field has been a REQ and an OPT, the OPT
    will be used.  The last default for each field will be used.

    Why do we care?  Hive, basically.  The files in HDFS each contain their
    own serialization schema in the header.  However, for the abstraction
    that all those files are really a table with a shared table structure
    to work, we need to make sure that there is a 'master' view that covers all
    the schema versions.  It's OK if some versions have fields that others do
    not as long as Hive can default the void to a null, but we cannot have
    fields with conflicting definitions -- hence the rules above.

    The FIXED fields are there to allow more complex schemas -- maps, records
    and the like.  Our boilerplate uses map for kvpairs, and array to hold
    handler records, so we use this already.  Handling evolution for these more
    complex fields is tricky, so we dodge it for the time being and force them
    to remain the same across versions.
    '''

    def __init__(self, slist=None):
        super(MasterAvroSchema, self).__init__()
        self.schema_list = None
        self.set_schema_list(slist)

    def clear_cache(self):
        super(MasterAvroSchema, self).clear_cache()

    @property
    def master_schema_string(self):
        '''Checking version validity happens on adds.  We can assume anything
        that made it through the add is valid.'''
        if not self.schema_list or len(self.schema_list) == 0:
            return None
        m_name = None
        m_type = None
        m_fields = dict()
        for sv in self.schema_list:
            # name and type should not change between versions
            m_name = sv.fullname
            m_type = sv.type
            for svf in sv.fields:
                m_fields[svf.name] = svf
        # put in alpha field name order
        om_fields = collections.OrderedDict()
        for key in sorted(m_fields.keys()):
            om_fields[key] = m_fields[key]
        # setting this allows us to use build_pcf on the fields directly
        env = dict()
        env[m_name] = u'"%s"' % m_name

        first_elem = True
        mss = u'{"name":"%s","type":"%s","fields":[' % (m_name, m_type)
        for mfname, mf in om_fields.iteritems():
            if first_elem:
                first_elem = False
            else:
                mss += u','
            if mf.type.type == u'union':
                mss += u'{"name":"%s","default":null,' % mfname
            else:
                mss += u'{"name":"%s",' % mfname
            mss += '"type":%s}' % build_pcf(env, mf.type)
        mss += u']}'
        return mss

    def add_schema_version(self, sv):
        '''Try to add a schema version to the existing MAS. An incompatible ver
        will return a list of error strings.  A compatible ver will update the
        MAS and return None.  The schema version can be passed in as either an
        RAS or a schema string.'''
        ras = None
        if isinstance(sv, basestring):
            ras = RegisteredAvroSchema()
            ras.schema_str = sv
        elif isinstance(sv, RegisteredAvroSchema):
            ras = sv
        else:
            raise ValueError('Passed object not a schema string or a RAS')
        if not ras.is_valid:
            raise ValueError("Schema version is internally invalid.")

        if self.schema_list == None:
            self.schema_list = []

        if len(self.schema_list) > 0:
            # already have version(s), check for issues
            ilist = self.incompatibilities(ras)
            if ilist and len(ilist) > 0:
                err_str = ''
                for issue in ilist:
                    err_str += '%s\n' % issue
                raise ValueError(err_str)
        self.schema_list.append(ras.schema)
        self.schema_str = self.master_schema_string

    @staticmethod
    def check_field(field):
        if not isinstance(field, avro.schema.Field):
            return ["Passed object not an avro.schema.Field."]
        if not field.type or not isinstance(field.type, avro.schema.Schema):
            return ["Passed Field does not contain a valid Schema type."]
        return

    @staticmethod
    def check_req_field(field, old_field=None):
        err = MasterAvroSchema.check_field(field)
        if err:
            return err
        schema = field.type
        # now check the schema for issues
        if schema.type in avro.schema.PRIMITIVE_TYPES:
            if schema.type == u'null':
                return ["REQ field's schema type is null."]
            if old_field and old_field.type.type != schema.type:
                return ["REQ field cannot change types."]
            # looks OK
            return
        return ["REQ field's schema type is %s." % schema.type]

    @staticmethod
    def check_opt_field(field, old_field=None):
        err = MasterAvroSchema.check_field(field)
        if err:
            return err
        schema = field.type
        if not isinstance(schema, avro.schema.UnionSchema):
            return ["OPT field not a union type."]
        if not len(schema.schemas) == 2:
            return ["OPT field does not have a 2 schema union."]
        # order is not fixed, so pull schema objects from union
        null_schema = None
        core_schema = None
        for s in schema.schemas:
            if s.type == u'null':
                null_schema = s
                continue
            if s.type in avro.schema.PRIMITIVE_TYPES:
                core_schema = s
                continue
            return ["OPT field union contains non-primitive type."]
        # now check schema objects against OPT requirements
        if not null_schema:
            return ["OPT field union missing a null schema."]
        if not core_schema:
            return ["OPT field union missing a non-null schema."]
        if old_field:
            old_core_schema = None
            if old_field.type.type in avro.schema.PRIMITIVE_TYPES:
                # REQ->OPT is OK
                old_core_schema = old_field.type
            elif isinstance(old_field, avro.schema.UnionSchema):
                # OPT->OPT is OK if core type stays the same
                for s in old_field.type.schemas:
                    if s.type != u'null':
                        old_core_schema = s
                        break
            else:
                return ["OPT field used to be FIXED."]
            if old_core_schema:
                if old_core_schema.type != core_schema.type:
                    return ["OPT field changed non-null union type."]

    def incompatibilities(self, ras):
        i_list = []
        master = avro.schema.parse(self.master_schema_string)
        if master.name != ras.schema.fullname:
            i_list.append("Name or namespace change.")
        if master.type != ras.schema.type:
            i_list.append("Top-level type change.")
        mfd = dict()
        for mf in master.fields:
            mfd[mf.name] = mf
        for nf in ras.schema.fields:
            if nf.name in mfd:
                # old field collision -- new field must be equal or REQ->OPT
                mf = mfd.pop(nf.name)
                if nf == mf:
                    # fields are the same
                    continue
                elif build_pcf(dict(), nf.type) == build_pcf(dict(), mf.type):
                    # field PCFs are the same
                    continue
                # check the new field is an OK mod (i.e. -- REQ->OPT)
                opt_issues = MasterAvroSchema.check_opt_field(nf, mf)
                if opt_issues:
                    for issue in opt_issues:
                        i_list.append(issue)
            else:
                # new field -- must be OPT
                opt_issues = MasterAvroSchema.check_opt_field(nf)
                if opt_issues:
                    if not MasterAvroSchema.check_req_field(nf):
                        i_list.append("Cannot ADD REQ field after first ver.")
                        continue
                    for issue in opt_issues:
                        i_list.append(issue)
        # remaining fields in mfd were not in passed RAS -- must be REQ or OPT
        for _mf_name, mf in mfd.iteritems():
            if not MasterAvroSchema.check_opt_field(mf):
                # removing an OPT is fine
                continue
            elif not MasterAvroSchema.check_req_field(mf):
                # removing a REQ is fine
                continue
            else:
                i_list.append("Cannot remove a FIXED field.")
        if len(i_list) > 0:
            return i_list

    def set_schema_list(self, slist):
        '''Sets the list of RegisteredAvroSchema objects that make up the
        versions spanned by the MasterAvroSchema.  If the list includes a non-
        compatible schema ordering, a ValueError will be raised.'''
        if slist == None or not isinstance(slist, list) or len(slist) == 0:
            self.schema_list = None
            logging.debug('Not a list.')
            return
        for elem in slist:
            self.add_schema_version(elem)

    def is_compatible(self, obj):
        '''Tests whether a passed RegisteredAvroSchema object is a compatible
        extension to the MasterAvroSchema object.'''
        if not obj or not isinstance(obj, RegisteredAvroSchema):
            # only RAS objects can be compatible
            logging.debug('Not a non-null RegisteredAvroSchema')
            return False

        i_list = self.incompatibilities(obj)
        if i_list == None or len(i_list) == 0:
            return True
        else:
            # TODO: better incompat logging
            for i in i_list:
                print i
        return False
