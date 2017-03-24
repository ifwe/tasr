'''
Created on November 18, 2014

@author: cmills

/id endpoints meant to be mounted by an umbrella instance of TASRApp.

'''
from tasr.app.wsgi import TASRApp


##############################################################################
# /id app - reference schemas by multi-type ID
##############################################################################
ID_APP = TASRApp()


@ID_APP.get('/<base64_id_str:path>')  # IDs w/ slashes, so :path
def schema_for_id_str(base64_id_str=None):
    '''Retrieves a schema registered to one or more groups as identified by an
    ID string.  The ID string is a base64-encoded 'fingerprint' of the schema
    (a leading flag byte followed by an md5 or sha256 digest of the canonical
    form of the schema JSON doc).

    Schemas retrieved this way can be associated with multiple groups (and
    represent multiple versions for a given group if there were non-sequential
    re-registrations).  As such, the X-SCHEMA headers with the metadata can be
    more complex.  There will be current topic-version entries for each group
    the schema was registered for.  If it has multiple versions for a group,
    only the most recent will be included.

    This access method is not currently supported in the S+V API, or by the
    standard Avro (1124-type) schema repository.  It is only really possible
    with the multi-type ID.
    '''
    if base64_id_str is None or base64_id_str == '':
        ID_APP.abort(400, 'Missing base64 ID string.')
    reg_schema = ID_APP.ASR.get_schema_for_id_str(base64_id_str)
    if reg_schema:
        return ID_APP.schema_response(reg_schema)
    # return nothing if there is no schema registered for the topic name
    ID_APP.abort(404, 'No schema registered with id %s' % base64_id_str)

# TODO: Add /<id>/meta to get metadata as JSON?
