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
        self.schema_str = None
        self.gv_dict = dict()
        self.ts_dict = dict()
        self.created = False

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
        rs_dict['schema'] = self.canonical_schema_str
        # overwrite the SHA256 and MD5 IDs with ones derived from the schema
        rs_dict['sha256_id'] = 'id.%s' % self.sha256_id
        rs_dict['md5_id'] = 'id.%s' % self.md5_id
        return rs_dict

    @property
    def canonical_schema_str(self):
        '''The split() and join() normalizes whitespace.'''
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
        '''Access the schema validity as a boolean property.
        '''
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
        return '%r' % self.canonical_schema_str

    def __str__(self):
        return u'%s[%s, %s]' % (self.__class__.__name__,
                                self.sha256_id, self.gv_dict)

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


def ordered_object(obj):
    '''A recursive method to alpha-order potentially nested collection objects
    in a repeatable way.  Basically, wherever there is a non-ordered collection
    order it by the alpha order of the keys.  Dict objects become OrderedDict
    objects.  Lists remain lists (and order is preserved).

    We use this here to help with canonicalization of Avro schemas.
    '''
    if obj and hasattr(obj, '__iter__'):
        if isinstance(obj, dict):
            # for a dict, we want to alpha order by key
            odict = collections.OrderedDict()
            for key in sorted(obj.keys()):
                # recurse to order the value object
                odict[key] = ordered_object(obj[key])
            return odict
        elif isinstance(obj, list):
            processed_list = []
            for elem in obj:
                # recurse to order the value object
                processed_list.append(ordered_object(elem))
            return processed_list
    # if the object is a leaf, just return it as is
    return obj


class RegisteredAvroSchema(RegisteredSchema):
    '''Adds an Avro schema validation function.'''
    def __init__(self):
        super(RegisteredAvroSchema, self).__init__()
        self.schema = None
        self.ordered = None

    @property
    def canonical_schema_str(self):
        if not self.ordered and self.schema_str:
            self.ordered = ordered_object(json.loads(self.schema_str))
        return json.dumps(self.ordered) if self.ordered else None

    def validate_schema_str(self):
        if not super(RegisteredAvroSchema, self).validate_schema_str():
            return False

        # a parse exception should bubble up, so don't catch it here
        self.schema = avro.schema.parse(self.canonical_schema_str)

        # add additional checks?
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

    @staticmethod
    def is_valid_avro_schema(schema_str):
        '''Checks whether the passed string is valid Avro (can be parsed).'''
        test_schema = RegisteredAvroSchema()
        test_schema.schema_str = schema_str
        try:
            return test_schema.validate_schema_str()
        except ValueError:
            return False
        except avro.schema.SchemaParseException:
            return False


class MasterAvroSchema(RegisteredAvroSchema):
    '''Checks whether the current schema is a valid extension of the schema
    or list of schemas passed to the method.  For purposes of discussion,
    there are 4 basic kinds of field we care about:

        - REQ: fixed type, a required field (missing == invalid), no nulls
        - ID: fixed type, enables demo segmentation, no changes, no nulls
        - PART: fixed type, enables partitioning, no changes, no nulls
        - OPT: union type, allow nulls, must have a null default

    For an Avro schema to be back-compatible (in our system) with previous
    versions:

        - The non-fields document elements must remain the same.
        - Field _order_ may be changed.
        - REQ fields may convert to ID, PART or OPT fields, but may not revert.
        - There may be a maximum of _one_ ID field.
        - There may be a maximum of _one_ PART field.
        - Once a REQ field has become an ID or PART field, it may not change.
        - OPT fields must have a union type with 'null' and one other type.
        - OPT fields must have a 'null' default value.
        - OPT fields may be added or removed across schema versions.
        - A REQ that becomes an OPT must use the original type as the non-null
           type in the union.
        - Removal of a REQ field is treated as an implicit conversion to an OPT
           field followed by a removal of that field.
        - A removed field (REQ or OPT) may only ever return as an OPT field
           with a union type of 'null' and the original non-null type.

    Why do we care?  Hive, basically.  The files in HDFS each contain their
    own serialization schema in the header.  However, for the abstraction
    that all those files are really a table with a shared table structure
    to work, we need to make sure that there is a 'master' view that covers
    all the schema versions.  It's OK if some versions have fields that
    others do not as long as Hive can default the void to a null, but we
    cannot have fields with conflicting definitions.

    Note that there are legacy schema versions with a union type, allowing
    nulls, but no default.  These are allowed as old versions, but when
    present in the current object make it non-back-compatible.

    ADDENDUM: We do support map fields, but we don't support evolution of those
    fields.  That is, once a field with a map type (or a union of null and a
    map type) is added, it has to remain unchanged to be "compatible" in this
    context.  This is not strictly the same thing as Avro compatibility, but
    is the right definition of compatibility for our purposes.
    '''

    def __init__(self, slist):
        super(MasterAvroSchema, self).__init__()
        self.schema_list = None
        self.e_namespace = None
        self.e_type = None
        self.e_name = None
        self.id_field = None
        self.part_field = None
        self.required_fields = dict()
        self.opt_fields = dict()
        self.complex_fields = dict()
        self.map_fields = dict()
        self.field_name_list = None
        self.deleted_field_names = None
        # if one was provided, set it
        self.set_schema_list(slist)

    def set_schema_list(self, slist):
        '''Sets the list of RegisteredAvroSchema objects that make up the
        versions spanned by the MasterAvroSchema.  If the list includes a non-
        compatible schema ordering, a ValueError will be raised.'''
        if slist == None or not isinstance(slist, list) or len(slist) == 0:
            self.schema_list = None
            logging.debug('Not a list.')
            return

        is_first_version = True
        self.schema_list = []
        for elem in slist:
            ras = None
            # these can be JSON schema defs or actual RAS objects
            if isinstance(elem, basestring):
                ras = RegisteredAvroSchema()
                ras.schema_str = elem
            elif isinstance(elem, RegisteredAvroSchema):
                ras = elem
            else:
                raise ValueError('Not a schema string or a RAS')
            if not ras.canonical_schema_str:  # also ensures ordered available
                raise ValueError('RAS has no canonical schema string')

            # we use the ordered dict as a convenient way to check the version
            ver = ras.ordered
            if not basic_schema_dict_valid(ver, mas=self):
                raise ValueError('Non-field element mismatch.')
            if (self.e_namespace == None
                and self.e_type == None
                and self.e_name == None):
                self.e_namespace = ver['namespace']
                self.e_type = ver['type']
                self.e_name = ver['name']

            # check present fields
            self.field_name_list = []
            for field in ver['fields']:
                sfield = SchemaField(field, is_first_version)
                self.field_name_list.append(sfield.name)
                if sfield.is_complex:
                    # we don't really check complex fields for compatibility,
                    # you are on your own in this case
                    self.complex_fields[sfield.name] = field
                elif sfield.is_map:
                    # map values need to remain unchanged to be compatible
                    if sfield.name in self.map_fields.keys():
                        old_values = self.map_fields[sfield.name]['values']
                        if sfield.map_values != old_values:
                            _msg = 'map values changed (%s)' % sfield.name
                            raise ValueError(_msg)
                elif sfield.is_required == False:
                    # OPT field -- ID -> OPT and PART -> OPT are not allowed
                    #if self.id_field and field == self.id_field:
                    #    raise ValueError('ID -> OPT for %s' % sfield.name)
                    #if self.part_field and field == self.part_field:
                    #    raise ValueError('PART -> OPT for %s' % sfield.name)
                    if sfield.name in self.required_fields.keys():
                        # REQ -> OPT is allowed
                        old_type = self.required_fields[sfield.name]['type']
                        if not old_type in sfield.type:
                            raise ValueError('REQ -> OPT incompatible types')
                        self.required_fields.pop(sfield.name)
                        self.opt_fields[sfield.name] = field
                    elif sfield.name in self.opt_fields.keys():
                        # OPT -> OPT, so check it's the same
                        if self.opt_fields[sfield.name] != field:
                            raise ValueError('OPT field mismatch: %s != %s' %
                                             (self.opt_fields[sfield.name],
                                              field))
                    else:
                        # new OPT field
                        self.opt_fields[sfield.name] = field

                elif sfield.is_required:
                    if sfield.name in self.required_fields.keys():
                        # never allow REQ -> REQ' changes
                        if field != self.required_fields[sfield.name]:
                            _msg = ('REQ field mismatch: %s != %s' %
                                    (self.required_fields[sfield.name], field))
                            raise ValueError(_msg)
                    elif is_first_version:
                        # allow new REQ fields in ver 1
                        self.required_fields[sfield.name] = field
                    else:
                        raise ValueError('Cannot add REQ fields after ver 1.')
                else:
                    # shouldn't happen
                    raise ValueError('WTF?  %s' % field)
                # end of present fields loop -- check for disallowed deletion
                if self.id_field:
                    if not self.id_field['name'] in self.field_name_list:
                        raise ValueError('Cannot delete ID field.')
                if self.part_field:
                    if not self.part_field['name'] in self.field_name_list:
                        raise ValueError('Cannot delete PART field.')
            is_first_version = False
            # end of ver loop

            # build deleted fields list
            self.deleted_field_names = []
            for fname in self.required_fields.keys():
                if not fname in self.field_name_list:
                    self.deleted_field_names.append(fname)
            for fname in self.opt_fields.keys():
                if not fname in self.field_name_list:
                    self.deleted_field_names.append(fname)

            # now create the master schema string
            mfields = []
            # add the retained fields in the most recent order
            for fname in self.field_name_list:
                if fname in self.required_fields:
                    mfields.append(self.required_fields[fname])
                elif fname in self.opt_fields:
                    mfields.append(self.opt_fields[fname])
                elif fname in self.complex_fields:
                    mfields.append(self.complex_fields[fname])
            # tack the deleted fields on the end
            for fname in self.deleted_field_names:
                if fname in self.required_fields:
                    mfields.append(self.required_fields[fname])
                elif fname in self.opt_fields:
                    mfields.append(self.opt_fields[fname])
            # now create the ordered dict holding it all
            odict = collections.OrderedDict()
            odict['namespace'] = self.e_namespace
            odict['type'] = self.e_type
            odict['name'] = self.e_name
            odict['fields'] = mfields
            # and from that, set the schema_str
            self.schema_str = json.dumps(odict)

    def is_compatible(self, obj):
        '''Tests whether a passed RegisteredAvroSchema object is a compatible
        extension to the MasterAvroSchema object.'''
        if self.schema_list == None:
            raise ValueError('No versions in master.')

        if not obj or not isinstance(obj, RegisteredAvroSchema):
            # only RAS objects can be compatible
            logging.debug('Not a non-null RegisteredAvroSchema')
            return False

        if not obj.canonical_schema_str:
            # short cut, also ensured obj.ordered is available
            return False

        ver = obj.ordered
        if not basic_schema_dict_valid(ver, mas=self):
            return False

        # check fields
        for field in ver['fields']:
            try:
                sfield = SchemaField(field)
                if sfield.is_complex:
                    # we don't really check complex fields for compatibility,
                    # you are on your own in this case
                    logging.debug('complex field %s', sfield.name)
                    continue
                elif sfield.is_map:
                    # map values need to remain unchanged to be compatible
                    if sfield.name in self.map_fields.keys():
                        old_values = self.map_fields[sfield.name]['values']
                        if sfield.map_values != old_values:
                            _msg = 'map values changed (%s)' % sfield.name
                            logging.debug(_msg)
                            return False
                elif sfield.is_required == False:
                    if sfield.name in self.required_fields.keys():
                        # REQ -> OPT is allowed
                        old_type = self.required_fields[sfield.name]['type']
                        if not old_type in sfield.type:
                            logging.debug('REQ -> OPT incompatible types')
                            return False
                    elif sfield.name in self.opt_fields.keys():
                        # OPT -> OPT, so check it's the same
                        if self.opt_fields[sfield.name] != field:
                            logging.debug('OPT field mismatch: %s != %s',
                                          self.opt_fields[sfield.name], field)
                            return False
                    else:
                        # new OPT field
                        pass

                elif sfield.is_required:
                    if sfield.name in self.required_fields.keys():
                        # never allow REQ -> REQ' changes
                        if field != self.required_fields[sfield.name]:
                            logging.debug('REQ field mismatch: %s != %s',
                                          self.required_fields[sfield.name],
                                          field)
                            return False
                    else:
                        logging.debug('Cannot add REQ fields after ver 1.')
                        return False
                else:
                    # shouldn't happen
                    raise ValueError('WTF?  %s' % field)
            except ValueError as verr:
                logging.warn(verr)
                return False
        return True


def basic_schema_dict_valid(sdict, mas=None):
    '''Checks that a schema dict has the basic required elements.  If a MAS is
    specified, ensure the name, namespace and type have not changed.'''
    if not ('namespace' in sdict and 'type' in sdict and 'name' in sdict):
        logging.debug('Missing non-field element(s).')
        return False
    if mas and mas.e_namespace and mas.e_namespace != sdict['namespace']:
        logging.debug('namespace mismatch')
        return False
    if mas and mas.e_name and mas.e_name != sdict['name']:
        logging.debug('name mismatch')
        return False
    if mas and mas.e_type and mas.e_type != sdict['type']:
        logging.debug('type mismatch')
        return False
    if not ('fields' in sdict and isinstance(sdict['fields'], list)):
        logging.debug('Does not have a fields element.')
        return False
    return True


class SchemaField(dict):
    def __init__(self, obj=None, allow_defaultless_opt=False):
        super(SchemaField, self).__init__()
        self.is_part = None
        self.is_id = None
        self.is_required = None
        self.allow_defaultless_opt = allow_defaultless_opt
        self.is_defaultless_opt = None
        self.is_complex = None
        self.is_map = None
        self.name = None
        self.type = None
        self.opt_type = None
        self.default = None
        self.map_values = None
        if obj:
            self.set_all(obj)

    def set_all(self, obj):
        if obj and isinstance(obj, dict):
            for k, v in obj.iteritems():
                self[k] = v
                if k.lower() == 'name':
                    self.name = self[k]
                if k.lower() == 'type':
                    self.type = self[k]
                if k.lower() == 'default':
                    self.default = self[k]
        if not self.name:
            raise ValueError("must define a name")
        if not self.type:
            raise ValueError("must define a type (%s)" % self.name)
        if isinstance(self.type, list):
            # OPT fields
            if not "null" in self.type:
                if self.allow_defaultless_opt:
                    self.is_defaultless_opt = True
                else:
                    raise ValueError("union without a null (%s)" % self.name)
            if not "default" in self.keys():
                raise ValueError("union with no default (%s)" % self.name)
            if not self.default == None:
                raise ValueError("union with bad default (%s)" % self.name)
            if not len(self.type) == 2:
                raise ValueError("bad OPT type: %s (%s)" % (self.type,
                                                            self.name))
            # we have a valid OPT field
            self.is_required = False
            for utype in self.type:
                if not utype == "null":
                    self.opt_type = utype
                    break
            if self.opt_type and (isinstance(self.opt_type, dict) or
                                  isinstance(self.opt_type, list)):
                self.is_complex = True
            else:
                self.is_complex = False
        elif self.type == 'map':
            # REQ map field
            if not "values" in self.keys():
                raise ValueError("map with no values (%s)" % self.name)
            self.map_values = self["values"]
            self.is_complex = False
            self.is_required = True
            self.is_map = True
        else:
            # REQ field
            self.is_complex = False
            self.is_required = True
            self.is_map = False
