'''
Created on November 18, 2014

@author: cmills

/schema endpoints meant to be mounted by an umbrella instance of TASRApp.

'''
import bottle
from tasr.app.wsgi import TASRApp, is_json_type
from tasr.registered_schema import MasterAvroSchema
from tasr.headers import SchemaHeaderBot


##############################################################################
# /schema app - reference schemas by JSON schema text
##############################################################################
SCHEMA_APP = TASRApp()


@SCHEMA_APP.post('/')
def schema_for_schema_str():
    '''Retrieves the registered schema for the schema string posted.  As with
    schema_for_id_str, this method is not constrained by a topic.  That is,
    we convert the passed schema into a fingerprint, then do the rest as if the
    request were made with that ID string.  This means the response headers
    will be like the ones for schemas retrieved by ID string.
    '''
    c_type = str(bottle.request.content_type).split(';')[0].strip()
    if not is_json_type(c_type):
        SCHEMA_APP.abort(406, 'Content-Type not JSON.')
    bod = bottle.request.body.getvalue()
    if bod is None or bod == '':
        SCHEMA_APP.abort(400, 'Expected a non-empty request body.')
    try:
        schema_str = bottle.request.body.getvalue()
        reg_schema = SCHEMA_APP.ASR.get_schema_for_schema_str(schema_str)
        if reg_schema:
            return SCHEMA_APP.schema_response(reg_schema)

        # For unregistered schemas, the status is a 404 and the return body is
        # empty, but we add the headers with the MD5 and SHA256 IDs so the
        # client has a way to get the ID values.  We also avoid calling the
        # inherited abort() as it would discard the added ID headers.
        unreg_schema = SCHEMA_APP.ASR.instantiate_registered_schema()
        unreg_schema.schema_str = schema_str
        # back-compatible with self is the best we can check here (and it does
        # check for required defaults and so on to ensure back-compat later.
        try:
            MasterAvroSchema([unreg_schema, ]).is_compatible(unreg_schema)
        except ValueError as verr:
            # schema does not pass back-compat test with self -- likely missing
            # a required field or default
            _msg = verr.message if verr.message else 'Incompatible schema.'
            SCHEMA_APP.abort(409, _msg)
        # Schema looks valid and unknown
        SchemaHeaderBot(bottle.response, unreg_schema).set_ids()
        bottle.response.status = 404
        errd = SCHEMA_APP.error_dict(404, 'Schema not registered.')
        return SCHEMA_APP.object_response(None, errd)
    except ValueError:
        SCHEMA_APP.abort(400, 'Invalid schema.  Failed to consider.')


