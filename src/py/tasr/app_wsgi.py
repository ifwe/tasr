'''
Created on Apr 14, 2014

@author: cmills

The top-level TASRApp is an unbrella object, extending the Bottle class, but
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
import tasr.group
import tasr.tasr_config

TASR_VERSION = 2


class TASRApp(bottle.Bottle):
    def __init__(self, config=tasr.tasr_config.CONFIG):
        super(TASRApp, self).__init__()
        self.config = config
        self.mounted = dict()
        self.mounted_path = '/'
        self.ASR = tasr.AvroSchemaRepository(host=config.redis_host,
                                             port=config.redis_port)

    def set_config_mode(self, mode):
        self.config.set_mode(mode)
        # update the ASR to ensure we're pointing at the right Redis
        self.ASR = tasr.AvroSchemaRepository(host=self.config.redis_host,
                                             port=self.config.redis_port)
        # now update any submodule ASRs
        for (_, subapp) in self.mounted.iteritems():
            if isinstance(subapp, TASRApp):
                subapp.ASR = self.ASR

    def mount(self, path, subapp):
        super(TASRApp, self).mount(path, subapp)
        self.mounted[path] = subapp
        if isinstance(subapp, TASRApp):
            subapp.ASR = self.ASR
            subapp.mounted_path = path

    def default_error_handler(self, err):
        bottle.response.status = err.status_code
        log_request(err.status_code)
        resp_type = response_content_type(bottle.request)
        bottle.response.content_type = resp_type
        if is_json_type(resp_type):
            # if we expect JSON back, give error message in JSON
            errd = {'application': 'TASR', 'version': TASR_VERSION}
            errd["method"] = bottle.request.method
            errd["uri"] = bottle.request.fullpath
            errd["query_string"] = bottle.request.query_string
            errd["status_code"] = err.status_code
            errd["message"] = err.message if err.message else err.status_line
            return tasr.app_wsgi.json_body({'error': errd})


def collection_as_text_lines(collec, attr=None):
    buff = StringIO.StringIO()
    for obj in collec:
        if attr:
            buff.write('%s\n' % obj.__dict__[attr])
        else:
            buff.write('%s\n' % obj)
    body = buff.getvalue()
    buff.close()
    return body


def request_form_to_dict(req):
    dct = dict()
    for key in req.forms.keys():
        plist = bottle.request.forms.getall(key)
        if len(plist) > 1:
            tasr.app_wsgi.abort(400, 'Multiple values for %s key.' % key)
        if len(plist) == 1:
            dct[key] = plist[0]
    return dct


def dict_as_java_properties(dct):
    buff = StringIO.StringIO()
    for key, val in dct.iteritems():
        buff.write('%s=%s\n' % (key, val))
    resp_body = buff.getvalue()
    buff.close()
    return resp_body


def json_body(ob):
    if is_pretty(bottle.request):
        j = json.dumps(ob, sort_keys=False, indent=4, separators=(',', ': '))
        return '%s\n' % j
    else:
        return json.dumps(ob)


def is_pretty(req):
    for qk in req.query.dict.keys():
        if qk.strip().lower() == 'pretty':
            return True
    return False


def pretty_json(req):
    '''Is the client asking for nicely formatted JSON back?'''
    if accept_json(req):
        return is_pretty(req)
    return False


def subjects_response(sub_list):
    '''Return a dict of subjects as JSON if possible.  If the client does not
    accept JSON types, return a list of subject names.
    '''
    log_request(bottle.response.status_code)
    resp_type = response_content_type(bottle.request)
    bottle.response.content_type = resp_type
    if is_json_type(resp_type):
        sub_d = dict()
        for subject in sub_list:
            sub_d[subject.name] = subject.as_dict()
        return json_body(sub_d)
    else:
        return collection_as_text_lines(sub_list, 'name')


def subject_response(subject):
    '''Returns a subject dict in JSON if JSON is accepted.  Standard subject
    headers.  If JSON is not accepted, returns an empty body.
    '''
    log_request(bottle.response.status_code)
    bot = tasr.headers.SubjectHeaderBot(bottle.response, subject)
    bot.standard_headers()
    rctype = response_content_type(bottle.request)
    bottle.response.content_type = rctype
    if is_json_type(rctype):
        return json_body(subject.as_dict())


def subject_config_response(subject):
    '''Returns the config dict for the subject if JSON is accepted.  Standard
    subject headers.  If JSON is not accepted, returns the config dict in the
    java.util.properties expected format (i.e. -- "<key>:<value>\n" or
    "<key>=<value>\n").
    '''
    log_request(bottle.response.status_code)
    bot = tasr.headers.SubjectHeaderBot(bottle.response, subject)
    bot.standard_headers()
    rctype = response_content_type(bottle.request)
    bottle.response.content_type = rctype
    if is_json_type(rctype):
        return json_body(subject.config)
    return dict_as_java_properties(subject.config)


def schema_response(reg_schema):
    '''Return the schema JSON for a registered schema.  The body will _ALWAYS_
    be JSON, even if the client does not specifically accept it.
    '''
    log_request(bottle.response.status_code)
    bot = tasr.headers.SchemaHeaderBot(bottle.response, reg_schema)
    bot.standard_headers()
    rctype = response_content_type(bottle.request)
    rctype = 'application/json' if not is_json_type(rctype) else rctype
    bottle.response.content_type = rctype
    if is_pretty(bottle.request):
        # note that a pretty response returns non-canonical whitespace
        return json_body(json.loads(reg_schema.canonical_schema_str))
    return reg_schema.canonical_schema_str


def log_request(code=200):
    level = logging.INFO
    if code >= 400 and code < 500:
        level = logging.WARN
    elif code >= 500:
        level = logging.ERROR

    targ = bottle.request.fullpath
    if bottle.request.query_string:
        targ = "%s?%s" % (bottle.request.fullpath, bottle.request.query_string)
    logging.log(level, "[ %s ] \"%s %s\" %s",
                bottle.request.remote_addr,
                bottle.request.method,
                targ,
                code)


def abort(code=500, text='Unknown Error.'):
    log_request(code)
    if accept_json(bottle.request):
        # if we expect JSON back, give error message in JSON
        bottle.response.status = code
        bottle.response.content_type = "application/json"
        errd = {'application': 'TASR', 'version': TASR_VERSION}
        errd["method"] = bottle.request.method
        errd["uri"] = bottle.request.fullpath
        errd["query_string"] = bottle.request.query_string
        errd["status_code"] = code
        errd["message"] = text
        raise bottle.HTTPResponse(body=json_body({'error': errd}), status=code)
    else:
        bottle.abort(code, text)


def abort_if_content_type_not_json(req):
    '''Many endpoints expect to receive JSON content.'''
    c_type = str(req.content_type).split(';')[0].strip()
    if not c_type in ['application/json', 'text/json']:
        abort(406, 'Content-Type must be appliction/json or text/json.')


def abort_if_body_empty(req):
    '''A common check for PUT and POST endpoints.'''
    bod = req.body.getvalue()
    if bod == None or bod == '':
        abort(400, 'Expected a non-empty request body.')


def abort_if_value_bad(val, label='expected value'):
    '''Bail if val is None or an empty string.'''
    if val == None or val == '':
        abort(400, 'Missing %s.' % label)


def abort_if_subject_bad(val, label='subject name'):
    '''Many of the S+V endpoints require a subject name. Check that it is valid
    as well as being non-null.'''
    abort_if_value_bad(val, label)
    if not tasr.group.Group.validate_group_name(val):
        abort(400, 'Bad %s: %s.' % (label, val))


def response_content_type(req):
    jlist = ['application/json', 'text/json']
    acc_list = []
    for a_type in str(req.get_header('Accept')).split(','):
        type_name = a_type.split(';')[0].strip().lower()
        if type_name in jlist:
            acc_list.append(type_name)
    for jtype in jlist:
        if jtype in acc_list:
            return jtype
    return 'text/plain'


def is_json_type(mime_type):
    return mime_type in ['application/json', 'text/json']


def accept_json(req):
    '''Is the client expecting JSON back?'''
    return is_json_type(response_content_type(req))
