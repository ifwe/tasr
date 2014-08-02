'''
Created on July 21, 2014

@author: cmills

The TASR and S+V APIs use different labels for groups and have some differences
in what kind of metadata should be returned in response headers.  This package
provides methods to add the right headers in each case.
'''
from tasr.registered_schema import SchemaMetadata
from tasr.group import GroupMetadata


class HeaderBot(object):
    def __init__(self, resp):
        self.resp = resp

    def add(self, header_name, value):
        self.resp.add_header(header_name, value)

    def set(self, header_name, value):
        self.resp.set_header(header_name, value)

    @staticmethod
    def extract(hname, resp):
        if hname in resp.headers:
            return resp.headers[hname]

    @staticmethod
    def extract_list(hname, resp):
        vals = []
        if hasattr(resp, 'headerlist'):
            for hdr in resp.headerlist:
                (key, val) = hdr
                if key.upper() == hname.upper():
                    vals.append(val)
        else:
            for key, val in resp.headers.iteritems():
                if key.upper() == hname.upper():
                    vals.append(val)
        return vals


class SubjectHeaderBot(HeaderBot):
    '''Handles adding X-TASR headers for subjects.'''

    H_NAME = 'X-TASR-SUBJECT-NAME'
    H_NAME_CUR_VER = 'X-TASR-SUBJECT-NAME-VERSION-MAP'
    H_CUR_VER = 'X-TASR-SUBJECT-CURRENT-SCHEMA-VERSION'
    H_CUR_TS = 'X-TASR-SUBJECT-CURRENT-SCHEMA-TIMESTAMP'
    H_CUR_MD5 = 'X-TASR-SUBJECT-CURRENT-SCHEMA-MD5'
    H_CUR_SHA256 = 'X-TASR-SUBJECT-CURRENT-SCHEMA-SHA256'

    @staticmethod
    def extract(label, resp):
        return HeaderBot.extract(SubjectHeaderBot.__dict__[label], resp)

    @staticmethod
    def extract_list(label, resp):
        return HeaderBot.extract_list(SubjectHeaderBot.__dict__[label], resp)

    @staticmethod
    def extract_metadata(resp):
        names = SubjectHeaderBot.extract_list('H_NAME_CUR_VER', resp)
        if not names:
            names = SubjectHeaderBot.extract_list('H_NAME', resp)

        metas = dict()
        if len(names) == 1:
            # a single group in the headers may have extra metadata in it
            if '=' in names[0]:
                name, cur_ver = names[0].split('=')
            else:
                name = names[0]
                cur_ver = SubjectHeaderBot.extract('H_CUR_VER', resp)
            cur_ts = SubjectHeaderBot.extract('H_CUR_TS', resp)
            cur_md5 = SubjectHeaderBot.extract('H_CUR_MD5', resp)
            cur_sha256 = SubjectHeaderBot.extract('H_CUR_SHA256', resp)
            meta = GroupMetadata(name, cur_ver, cur_ts)
            meta.current_sha256_id = cur_sha256
            meta.current_md5_id = cur_md5
            metas[name] = meta
        elif names and len(names) > 1:
            # handle multiple subjects
            for name in names:
                for nit in name.split(','):
                    if '=' in nit:
                        (subj, ver) = nit.split('=', 1)
                        ver = int(ver)
                        metas[subj] = GroupMetadata(subj, ver)
                    else:
                        metas[nit] = GroupMetadata(nit)
        return metas

    def __init__(self, resp, subject=None):
        super(SubjectHeaderBot, self).__init__(resp)
        self.subject = subject

    def set_subject_current_ids(self, subject=None):
        '''Adds both ID headers for the subject's current schema version:
          - <H_CUR_MD5>:    <md5 ID for current version>
          - <H_CUR_SHA256>: <sha256 ID for current version>
        '''
        subj = subject if subject else self.subject
        if subj and subj.current_schema:
            self.set(SubjectHeaderBot.H_CUR_MD5,
                     subj.current_schema.md5_id)
            self.set(SubjectHeaderBot.H_CUR_SHA256,
                     subj.current_schema.sha256_id)

    def add_subject_name_current_version(self, subject=None):
        '''Adds an <H_NAME_CUR_VER>: <subject name>=<current version> header'''
        subj = subject if subject else self.subject
        if subj and subj.current_version:
            self.add(SubjectHeaderBot.H_NAME_CUR_VER,
                     '%s=%s' % (subj.name, subj.current_version))

    def add_subject_name(self, subject=None):
        '''Adds an <H_NAME>: <subject name> header'''
        subj = subject if subject else self.subject
        if subj and subj.name:
            self.add(SubjectHeaderBot.H_NAME, subj.name)

    def add_subject_current_version(self, subject=None):
        '''Adds an <H_CUR_VER>: <current version> header'''
        subj = subject if subject else self.subject
        if subj and subj.current_version:
            self.add(SubjectHeaderBot.H_CUR_VER, subj.current_version)

    def add_subject_current_timestamp(self, subject=None):
        '''Adds an <H_CUR_TS>: <current timestamp> header'''
        subj = subject if subject else self.subject
        if subj and subj.current_schema:
            cur_ts = subj.current_schema.current_version_timestamp(subj.name)
            if cur_ts:
                self.add(SubjectHeaderBot.H_CUR_TS, cur_ts)

    def standard_headers(self, subject=None):
        '''Adds the standard subject headers.'''
        self.add_subject_name(subject)
        self.add_subject_current_version(subject)
        self.add_subject_current_timestamp(subject)
        self.set_subject_current_ids(subject)


class SchemaHeaderBot(HeaderBot):
    '''Handles adding X-TASR headers for schemas'''
    H_MD5 = 'X-TASR-SCHEMA-MD5'
    H_SHA256 = 'X-TASR-SCHEMA-SHA256'
    H_SUB_VER = 'X-TASR-SCHEMA-SUBJECT-VERSION-MAP'
    H_SUB_TS = 'X-TASR-SCHEMA-SUBJECT-TIMESTAMP-MAP'
    H_SUB_NAME = 'X-TASR-SUBJECT-NAME'
    H_VER = 'X-TASR-SCHEMA-VERSION'
    H_TS = 'X-TASR-SCHEMA-TIMESTAMP'

    # legacy header names
    LH_MD5 = 'X-SCHEMA-MD5'
    LH_SHA256 = 'X-SCHEMA-SHA256'
    LH_TOP_VER = 'X-SCHEMA-TOPIC-VERSION'
    LH_TOP_TS = 'X-SCHEMA-TOPIC-VERSION-TIMESTAMP'

    @staticmethod
    def extract(label, resp):
        return HeaderBot.extract(SchemaHeaderBot.__dict__[label], resp)

    @staticmethod
    def extract_list(label, resp):
        return HeaderBot.extract_list(SchemaHeaderBot.__dict__[label], resp)

    @staticmethod
    def extract_metadata(resp):
        metadata = SchemaMetadata()
        metadata.sha256_id = SchemaHeaderBot.extract('H_SHA256', resp)
        if not metadata.sha256_id:
            metadata.sha256_id = SchemaHeaderBot.extract('LH_SHA256', resp)
        metadata.md5_id = SchemaHeaderBot.extract('H_MD5', resp)
        if not metadata.md5_id:
            metadata.md5_id = SchemaHeaderBot.extract('LH_MD5', resp)
        # look for non-map subject version and timestamp vals
        subj = SchemaHeaderBot.extract('H_SUB_NAME', resp)
        sver = SchemaHeaderBot.extract('H_VER', resp)
        sts = SchemaHeaderBot.extract('H_TS', resp)
        if subj and sver:
            metadata.gv_dict[subj.strip()] = int(sver)
        if subj and sts:
            metadata.ts_dict[subj.strip()] = long(sts)
        # read in subject-version map headers if present
        ver_strs = SchemaHeaderBot.extract_list('H_SUB_VER', resp)
        if not ver_strs or len(ver_strs) == 0:
            ver_strs = SchemaHeaderBot.extract_list('LH_TOP_VER', resp)
        if ver_strs:
            for ver_str in ver_strs:
                for vit in ver_str.split(','):
                    (subj, ver) = vit.split('=', 1)
                    ver = int(ver)
                    metadata.gv_dict[subj.strip()] = ver
        # read in subject-timestamp map headers if present
        ts_strs = SchemaHeaderBot.extract_list('H_SUB_TS', resp)
        if not ts_strs:
            ts_strs = SchemaHeaderBot.extract_list('LH_TOP_TS', resp)
        if ts_strs:
            for ts_str in ts_strs:
                for tsit in ts_str.split(','):
                    (subj, timestamp) = tsit.split('=', 1)
                    timestamp = long(timestamp)
                    metadata.ts_dict[subj.strip()] = timestamp
        return metadata

    def __init__(self, resp, reg_schema=None, subject_name=None):
        super(SchemaHeaderBot, self).__init__(resp)
        self.reg_schema = reg_schema
        self.subject_name = subject_name

    def add_headers_for_smap(self, h_name, sdict, subject_name=None):
        '''A convenience method to loop through a dict using associated subject
        names as keys and add headers with <subject name>=<val>.'''
        headers_added = 0
        for subj, val in sdict.iteritems():
            if not subject_name or subject_name == subj:
                self.add(h_name, '%s=%s' % (subj, val))
                headers_added += 1
                if subject_name and subject_name == subj:
                    break
        return headers_added

    def set_ids(self, reg_schema=None):
        '''Adds both ID headers for this schema:
          - <H_MD5>:    <md5 ID for this schema>
          - <H_SHA256>: <sha256 ID for this schema>
        '''
        schema = reg_schema if reg_schema else self.reg_schema
        self.set(SchemaHeaderBot.H_MD5, schema.md5_id)
        self.set(SchemaHeaderBot.H_SHA256, schema.sha256_id)

    def add_current_versions(self, reg_schema=None, subject_name=None):
        '''
        Adds <H_SUB_VER>: <subject name>=<current version> headers for all
        associated subjects.  If the subject_name arg is set, only a header for
        that subject is added.
        '''
        schema = reg_schema if reg_schema else self.reg_schema
        sname = subject_name if subject_name else self.subject_name
        if subject_name:
            # use the subjectless header if subject is specified
            self.set(SchemaHeaderBot.H_SUB_NAME, sname)
            self.add(SchemaHeaderBot.H_VER, schema.gv_dict[sname])
            return 2  # as we've added 2 headers
        # otherwise use <H_SUB_VER>: <subject name>=<current version> for all
        return self.add_headers_for_smap(SchemaHeaderBot.H_SUB_VER,
                                         schema.gv_dict, sname)

    def add_current_timestamps(self, reg_schema=None, subject_name=None):
        '''
        Adds <H_SUB_TS>: <subject name>=<current timestamp> headers for all
        associated subjects.  If the subject_name arg is set, only a header for
        that subject is added.
        '''
        schema = reg_schema if reg_schema else self.reg_schema
        sname = subject_name if subject_name else self.subject_name
        if subject_name:
            # use the subjectless header if subject is specified
            self.set(SchemaHeaderBot.H_SUB_NAME, sname)
            self.add(SchemaHeaderBot.H_TS, schema.ts_dict[sname])
            return 2  # as we've added 2 headers
        # otherwise use <H_SUB_TS>: <subject name>=<current timestamp> for all
        return self.add_headers_for_smap(SchemaHeaderBot.H_SUB_TS,
                                         schema.ts_dict, sname)

    def leg_set_ids(self, reg_schema=None):
        '''Sets both ID headers for this schema:
          - <LH_MD5>:    <md5 ID for this schema>
          - <LH_SHA256>: <sha256 ID for this schema>
        '''
        schema = reg_schema if reg_schema else self.reg_schema
        self.set(SchemaHeaderBot.LH_MD5, schema.md5_id)
        self.set(SchemaHeaderBot.LH_SHA256, schema.sha256_id)

    def leg_add_current_versions(self, reg_schema=None, topic_name=None):
        '''
        Adds <LH_TOP_VER>: <topic name>=<current version> headers for all
        associated topics.  If the topic_name arg is set, only a header for
        that topic is added.
        '''
        schema = reg_schema if reg_schema else self.reg_schema
        tname = topic_name if topic_name else self.subject_name
        return self.add_headers_for_smap(SchemaHeaderBot.LH_TOP_VER,
                                                schema.gv_dict,
                                                tname)

    def leg_add_current_timestamps(self, reg_schema=None, topic_name=None):
        '''
        Adds <LH_TOP_TS>: <topic name>=<current timestamp> headers for all
        associated topics.  If the topic_name arg is set, only a header for
        that topic is added.
        '''
        schema = reg_schema if reg_schema else self.reg_schema
        tname = topic_name if topic_name else self.subject_name
        return self.add_headers_for_smap(SchemaHeaderBot.LH_TOP_TS,
                                                schema.ts_dict,
                                                tname)

    def standard_headers(self, reg_schema=None, subject_name=None):
        schema = reg_schema if reg_schema else self.reg_schema
        sname = subject_name if subject_name else self.subject_name
        self.set_ids(schema)
        self.add_current_versions(schema, sname)
        self.add_current_timestamps(schema, sname)

    def legacy_headers(self, reg_schema=None, subject_name=None):
        schema = reg_schema if reg_schema else self.reg_schema
        sname = subject_name if subject_name else self.subject_name
        self.leg_set_ids(schema)
        self.leg_add_current_versions(schema, sname)
        self.leg_add_current_timestamps(schema, sname)

    def all_headers(self, reg_schema=None, subject_name=None):
        self.standard_headers(reg_schema, subject_name)
        self.legacy_headers(reg_schema, subject_name)
