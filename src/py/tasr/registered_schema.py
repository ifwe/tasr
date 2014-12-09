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
import sets
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
            self.update_dicts_from_schema_metadata(SchemaMetadata(rs_dict))

    def update_dicts_from_schema_metadata(self, metadata):
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
        '''

        # ensure we have a list of RegisteredAvroSchema objects to check
        olds = []
        if obj and isinstance(obj, list):
            for ver in obj:
                if not isinstance(ver, RegisteredAvroSchema):
                    logging.debug('Old version not a RegisteredAvroSchema')
                    return False
                olds.append(ver)
        elif not obj or not isinstance(obj, RegisteredAvroSchema):
            logging.debug('Old version not a RegisteredAvroSchema')
            return False
        else:
            olds.append(obj)

        if not self.canonical_schema_str:
            # short cut, also ensures ordered dict available
            logging.debug('Missing canonical schema string.')
            return False

        # process the old versions, ensuring they are cross-compatible first
        e_namespace = None
        e_type = None
        e_name = None
        id_field = None
        part_field = None
        req_fields = dict()
        opt_fields = dict()
        is_first_ver = True
        for rs_ver in olds:
            if not rs_ver.canonical_schema_str:
                # short cut, also ensures ordered dict available
                logging.debug('Old version missing canonical schema string.')
                return False
            ver = rs_ver.ordered
            # check non-field entities
            if not ('namespace' in ver and 'type' in ver and 'name' in ver):
                logging.debug('Old version missing non-field element(s).')
                return False
            elif e_namespace == None and e_type == None and e_name == None:
                e_namespace = ver['namespace']
                e_type = ver['type']
                e_name = ver['name']
            else:
                if (e_namespace != ver['namespace']
                    or e_type != ver['type']
                    or e_name != ver['name']):
                    logging.debug('Old version non-field element mismatch.')
                    return False
            # check fields
            if not ('fields' in ver and isinstance(ver['fields'], list)):
                logging.debug('Old ver does not have a fields element.')
                return False
            for field in ver['fields']:
                if not 'name' in field:
                    logging.debug('Field name missing.')
                    return False
                if not 'type' in field:
                    logging.debug('Type missing for %s field.', field['name'])
                    return False
                if isinstance(field['type'], list):
                    # so, OPT or REQ*, should have a union of null and a type
                    if not 'null' in field['type']:
                        logging.debug('Missing null type.')
                        return False
                    if not 'default' in field or field['default'] != None:
                        logging.debug('Missing null default. Legacy REQ*, OK.')
                        if field['name'] in opt_fields.keys():
                            logging.debug('OPT -> REQ* for %s', field['name'])
                            return False
                        if id_field and field == id_field:
                            logging.debug('ID -> REQ* for %s', field['name'])
                            return False
                        if part_field and field == part_field:
                            logging.debug('PART -> REQ* for %s', field['name'])
                            return False
                        req_fields[field['name']] = field
                        continue
                    # it is an OPT field
                    if id_field and field == id_field:
                        logging.debug('ID -> OPT for %s', field['name'])
                        return False
                    if part_field and field == part_field:
                        logging.debug('PART -> OPT for %s', field['name'])
                        return False
                    if field['name'] in req_fields.keys():
                        # REQ -> OPT allowed transformation
                        old_type = req_fields[field['name']]['type']
                        if not old_type in field['type']:
                            logging.debug('REQ -> OPT incompatible types')
                            return False
                        req_fields.pop(field['name'])
                        opt_fields[field['name']] = field
                    if field['name'] in opt_fields.keys():
                        # OPT -> OPT, so check it's the same
                        if opt_fields[field['name']] != field:
                            logging.debug('OPT field mismatch: %s != %s',
                                          opt_fields[field['name']], field)
                            return False
                    else:
                        opt_fields[field['name']] = field
                else:
                    # REQ field
                    if field['name'] in req_fields.keys():
                        if field != req_fields[field['name']]:
                            logging.debug('REQ field mismatch: %s != %s',
                                          req_fields[field['name']], field)
                            return False
                    elif is_first_ver:
                        req_fields[field['name']] = field
                    else:
                        logging.debug('Cannot add fixed fields after ver 1.')
                        return False
            is_first_ver = False
            # end of ver loop

        # now check the object itself for compatibility with the old versions
        ver = self.ordered
        if not ('namespace' in ver and 'type' in ver and 'name' in ver):
            logging.debug('Missing non-field element(s).')
            return False
        else:
            if (e_namespace != ver['namespace']
                or e_type != ver['type']
                or e_name != ver['name']):
                logging.debug('Non-field element mismatch.')
                return False
        # check fields
        if not ('fields' in ver and isinstance(ver['fields'], list)):
            logging.debug('Does not have a fields element.')
            return False
        for field in ver['fields']:
            if not 'name' in field:
                logging.debug('Field name missing.')
                return False
            if not 'type' in field:
                logging.debug('Type missing for %s field.', field['name'])
                return False
            if isinstance(field['type'], list):
                # OPT should have a union of null and a type
                if not 'null' in field['type']:
                    logging.debug('Missing null type.')
                    return False
                if not 'default' in field or field['default'] != None:
                    logging.debug('Missing null default.')
                    return False
                if id_field and field == id_field:
                    logging.debug('ID -> OPT for %s', field['name'])
                    return False
                if part_field and field == part_field:
                    logging.debug('PART -> OPT for %s', field['name'])
                    return False
                if field['name'] in req_fields.keys():
                    # REQ -> OPT allowed transformation
                    old_type = req_fields[field['name']]['type']
                    if not old_type in field['type']:
                        logging.debug('REQ -> OPT incompatible types')
                        return False
                    req_fields.pop(field['name'])
                    opt_fields[field['name']] = field
                if field['name'] in opt_fields.keys():
                    # OPT -> OPT, so check it's the same
                    if opt_fields[field['name']] != field:
                        logging.debug('OPT field mismatch: %s != %s',
                                      opt_fields[field['name']], field)
                        return False
                else:
                    opt_fields[field['name']] = field
            else:
                # REQ field
                if field['name'] in req_fields.keys():
                    if field != req_fields[field['name']]:
                        logging.debug('REQ field mismatch: %s != %s',
                                      req_fields[field['name']], field)
                        return False
                else:
                    logging.debug('Cannot add fixed fields in later versions.')
                    return False
        return True

    @staticmethod
    def is_valid_avro_schema(schema_str):
        test_schema = RegisteredAvroSchema()
        test_schema.schema_str = schema_str
        try:
            return test_schema.validate_schema_str()
        except:
            return False
