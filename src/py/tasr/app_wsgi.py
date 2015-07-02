'''
Created on Apr 14, 2014

@author: cmills

The top-level TASRApp is an umbrella object, extending the Bottle class, but
also serving as a common root on which to mount the path-specific sub-apps.
As such, it is the WSGI compliant class to instantiate for a TASR instance.  We
avoid instantiating it in this module as several other modules import from this
one.

The mount() method is overridden, so in addition to doing the regular mount
the subapp is added to a dict of mounted apps and, if the subapp is a TASRApp,
the ASR is updated to use the ASR of the umbrella instance.  This allows mode
changes applied to the unbrella object to cascade down automatically.

This module also includes some general purpose util methods used by many of the
subapps.
'''
import bottle
import json
import logging
import StringIO
import tasr.tasr_config
import re

TASR_VERSION = 2


class TASRApp(bottle.Bottle):
    '''Wrap the Bottle object to keep track of the TASRConfig, ASR, and child
    apps mounted under the root.
    '''
    def __init__(self, config=tasr.tasr_config.CONFIG):
        super(TASRApp, self).__init__()
        self.config = config
        self.mounted = dict()
        self.mounted_path = '/'
        self.ASR = tasr.AvroSchemaRepository(host=config.redis_host,
                                             port=config.redis_port)

    def set_config_mode(self, mode):
        '''Sets the mode of the associated TASRConfig.  If the app has any
        children, chin down and set the mode on them as well.
        '''
        self.config.set_mode(mode)
        # update the ASR to ensure we're pointing at the right Redis
        self.ASR = tasr.AvroSchemaRepository(host=self.config.redis_host,
                                             port=self.config.redis_port)
        # now update any submodule ASRs
        for (_, subapp) in self.mounted.iteritems():
            if isinstance(subapp, TASRApp):
                #subapp.ASR = self.ASR
                subapp.set_config_mode(mode)

    def mount(self, path, subapp):
        super(TASRApp, self).mount(path, subapp)
        self.mounted[path] = subapp
        if isinstance(subapp, TASRApp):
            subapp.ASR = self.ASR
            subapp.mounted_path = path

    def error_dict(self, status_code=500, message='Error'):
        errd = {'application': 'TASR', 'version': TASR_VERSION}
        errd["method"] = bottle.request.method
        errd["uri"] = bottle.request.fullpath
        errd["query_string"] = bottle.request.query_string
        errd["status_code"] = status_code
        errd["message"] = message
        return errd

    def default_error_handler(self, err):
        bottle.response.status = err.status_code
        errd = self.error_dict(err.status_code,
                               err.message if err.message else err.status_line)
        return self.object_response(None, errd)

    def abort(self, code=500, text='Unknown Error.'):
        log_request(code)
        rctype = response_content_type()
        if is_json_type(rctype):
            # if we expect JSON back, give error message in JSON
            bottle.response.status = code
            bottle.response.content_type = rctype
            errd = self.error_dict(code, text)
            raise bottle.HTTPResponse(body=json_body(errd), status=code)
        else:
            bottle.abort(code, text)

    def object_response(self, obj, json_obj=None, default_type='text/plain'):
        rctype = response_content_type(default_type)
        bottle.response.content_type = rctype
        callback_fn = get_jsonp_callback()
        if callback_fn and re.match(r'.*\W.*', callback_fn):
                self.abort(400, 'Invalid JSONP callback function name')

        log_request(bottle.response.status_code)
        if callback_fn:
            # return a JSONP wrapped response
            if json_obj == None:
                jbod = json_body(obj)
            else:
                jbod = json_body(json_obj)
            return ("/**/typeof %s==='function' && %s(%s);" %
                    (callback_fn, callback_fn, jbod))
        elif is_json_type(rctype):
            if json_obj == None:
                return json_body(obj)
            return json_body(json_obj)
        elif not obj == None:
            # if we're not returning JSON and obj is not None, return as lines
            buff = StringIO.StringIO()
            if hasattr(obj, '__iter__'):
                for item in obj:
                    buff.write('%s\n' % item)
            else:
                buff.write('%s\n' % obj)
            body = buff.getvalue()
            buff.close()
            return body

    def subject_response(self, subject):
        '''Returns a subject dict in JSON if JSON is accepted.  Standard
        subject headers.  If JSON is not accepted, returns an empty body.
        '''
        bot = tasr.headers.SubjectHeaderBot(bottle.response, subject)
        bot.standard_headers()
        return self.object_response(None, subject.as_dict())

    def subject_config_response(self, subject):
        '''Returns the config dict for the subject if JSON is accepted.
        Standard subject headers.  If JSON is not accepted, returns the config
        dict in the java.util.properties expected format (i.e. --
        "<key>:<value>\n" or "<key>=<value>\n").
        '''
        bot = tasr.headers.SubjectHeaderBot(bottle.response, subject)
        bot.standard_headers()
        prop_list = []
        if subject.config:
            for key, val in subject.config.iteritems():
                prop_list.append('%s=%s\n' % (key, val))
        return self.object_response(prop_list, subject.config)

    def subject_config_entry_response(self, subject, key):
        '''Returns the config entry for the subject if JSON is accepted.
        Standard subject headers.  If JSON is not accepted, returns the
        value for the entry as a string.
        '''
        bot = tasr.headers.SubjectHeaderBot(bottle.response, subject)
        bot.standard_headers()
        if subject.config and subject.config[key]:
            val = subject.config[key]
            return self.object_response(val, {key: val})
        return self.object_response(None, subject.as_dict())

    def schema_response(self, reg_schema, subject_name=None):
        '''Return the schema JSON for a registered schema.  The body will
        _ALWAYS_ be JSON, even if the client does not specifically accept it.
        Standard schema headers.
        '''
        rctype = response_content_type(default_type='application/json')
        bottle.response.content_type = rctype
        bot = tasr.headers.SchemaHeaderBot(bottle.response, reg_schema)
        bot.standard_headers(subject_name=subject_name)
        return self.object_response(reg_schema.json_obj, None, rctype)


def log_request(code=200):
    level = logging.INFO
    if code >= 400 and code < 500:
        level = logging.WARN
    elif code >= 500:
        level = logging.ERROR
    targ = bottle.request.fullpath
    if bottle.request.query_string:
        targ = "%s?%s" % (targ, bottle.request.query_string)
    logging.log(level, "[ %s ] \"%s %s\" %s",
                bottle.request.remote_addr,
                bottle.request.method,
                targ,
                code)


def is_pretty():
    for qk in bottle.request.query.dict.keys():
        if qk.strip().lower() == 'pretty':
            return True
    return False


def get_jsonp_callback():
    for qk in bottle.request.query.dict.keys():
        if qk.strip().lower() == 'callback':
            if len(bottle.request.query.dict[qk]) > 0:
                if len(bottle.request.query.dict[qk][0]) > 0:
                    return bottle.request.query.dict[qk][0]
    return None


def json_body(ob):
    if is_pretty():
        j = json.dumps(ob, sort_keys=False, indent=3,
                       separators=(',', ': '))
        return '%s\n' % j
    else:
        return json.dumps(ob)


def is_json_type(mime_type):
    if not isinstance(mime_type, basestring):
        return False
    # force to lower, and if we have an encoding, strip it first
    mime_type = mime_type.lower().split(';', 1)[0]
    return mime_type in ['application/json', 'text/json']


def response_content_type(default_type='text/plain'):
    jlist = ['application/json', 'text/json']
    acc_list = []
    for a_type in str(bottle.request.get_header('Accept')).split(','):
        type_name = a_type.split(';')[0].strip().lower()
        if type_name in jlist:
            acc_list.append(type_name)
    for jtype in jlist:
        if jtype in acc_list:
            return jtype
    return default_type
