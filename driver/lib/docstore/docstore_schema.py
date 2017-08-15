import logging
import json
from copy import deepcopy
from pkg_resources import resource_filename
from builtins import str

_docstore_schema = None

# DocStore DDL (Data Deinition Language)
_DDL = {
    # List of fields which can be inserted or read from the DocStore
    u"fields": {
        # Data type of the fields list when validating against the DDL
        u"type": list,
        # Whether a fields list is required when validating against the DDL
        u"required": False,
        # Default list of fields if none are provided in the schema
        u"default": [],
        # Schema for an individual field
        u"schema": {
            # Field named used when accessing the DocStore. This field name is the default used
            # by backend databases, but the name may be different in these databases.
            # See solr_field and mongo_field
            u"name": {u"type": str, u"required": True},
            # Whether the field is required for all documents being inserted into the DocStore
            u"required": {u"type": bool, u"default": False},
            # field data type enum i.e. (string, text, int, boolean, datetime, point)
            u"type": {u"type": str, u"required": True,
                      u"enum": [u'string', u'text', u'int', u'boolean',
                                u'datetime', u'point', u'float']},
            # Whether this field is used as a unique key. If there is one field which has
            # unique_key set, then the value for this field must be unique. If there are
            # multiple unique keys set, then the key is an un-ordered tuple of all unique
            # keys and no two instances in the DocStore can contain the same un-ordered tuple.
            u"unique_key": {u"type": bool, u"default": False},
            # Whether the field is inserted into a backend reverse index for search
            u"indexed": {u"type": bool, u"default": False},
            # If this field is indexed, this property determines whether the field is also
            # stored in the search index with its original content (in addition to the reverse
            # index trie). Setting this property may allow for the backend search index to
            # provide better results, at the cost of higher memory requirements.
            u"stored": {u"type": bool, u"default": False},
            # If this field is indexed, this property determines any pre-processing analysis
            # that will be performed on the field value before indexing.
            # Valid analyzers are
            #   string: original string with no pre-processing
            #   strings: multi-valued list of original strings with no pre-processing on elements
            #   text_en: string with pre-processing for English language indexing
            #   int: integer
            #   ints: multi-valued list of integers
            #   boolean: true/false
            #   booleans: multi-valued list of booleans
            #   tdate: datetime in UTC iso-8601
            #   tdates: multi-valued list datetime in UTC iso-8601
            #   location: tuple of (x,y) coordinates
            #   other: Special case which signifies that the typing will be handled
            #     by the backend reverse index. If this property is set to "other",
            #     then the property "solr_field" must be set
            u"analyzer": {u"type": str, u"default": {
                # Default analyzer based on "type" property
                # If type of value is a string, this value is used as the default analyzer
                # If type of value is an object, the keys of this object are evaluated
                # for truthyness and the value of the truthy key is used as the default
                # analyzer. If multiple keys are truthy the default value is undefined.
                u"string": {u"multi_valued": u"strings", u"": u"string"},
                u"text": u"text_en",
                u"int": {u"multi_valued": u"ints", u"": u"int"},
                u"float": {u"multi_valued": u"doubles", u"": u"double"},
                u"boolean": {u"multi_valued": u"booleans", u"": u"boolean"},
                u"datetime": {u"multi_valued": u"tdates", u"": u"tdate"},
                u"point": u"location",
            }, u"enum": [u'string', u'strings', u'text_en', u'int', u'ints',
                         u'boolean', u'booleans', u'tdate', u'tdates', u'location',
                         u'other']},
            # Whether the value for this field is a list of "type"
            u"multi_valued": {u"type": bool, u"default": False},
            # If multi_valued is true, this property determines whether duplicates are
            # allowed within the list
            # Valid values are
            #   list: duplicates allowed
            #   set: duplicates not allowed
            u"multi_valued_type": {u"type": str, u"default": u"list",
                                   u"enum": [u'set', u'list']},
            # If multi_valued is true, this property determines whether the multi_valued
            # list should be appended upon update, or if the entire list should be replaced
            # by the new updated list
            u"multi_valued_operation": {u"type": str, u"default": u"replace",
                                        u"enum": [u'append', u'replace']},
            # If "indexed" is true and this property is set, the value of this property
            # will be used as the field name when updating the underlying reverse index.
            #
            # If "indexed" is true and this property is not set, the property name in
            # the underlying reverse index will be the value of the "name" field with
            # "solr_field.dynamic_field_suffix" appended.
            # For example:
            #   If name is "title"
            #   and type is "string"
            #   and multi_valued is false
            #   the reverse index field name will be "title_s"
            #
            #   If name is "butter_user_id"
            #   and type is "int"
            #   and multi_valued is true
            #   the reverse index field name will be "butter_user_id_is"
            u"solr_field": {u"type": str,
                            u"dynamic_field_suffix": {
                                u"string": {u"multi_valued": u"_ss", u"": u"_s"},
                                u"text": u"_en",
                                u"int": {u"multi_valued": u"_is", u"": u"_i"},
                                u"boolean": {u"multi_valued": u"_bs", u"": u"_b"},
                                u"datetime": {u"multi_valued": u"_dts", u"": u"_dt"},
                                u"point": u"_pt",
                            }},
            # Name of underlying field in the backend persistent storage.
            # If this property is not set, "name" will be used as the field name instead.
            u"mongo_field": {u"type": str},
        }
    },
    # List of fields which will be copied from a source field
    u"copy_fields": {
        # Data type of the copy_fields list when validating against the DDL
        u"type": list,
        # Whether a copy_fields list is required when validating against the DDL
        u"required": False,
        # Default list of copy fields if none are provided in the schema
        u"default": [],
        # Schema for an individual copy field
        u"schema": {
            # Field to copy from
            u"source": {u"type": str, u"required": True},
            # Field to copy into
            u"dest": {u"type": str, u"required": True},
        }
    }
}

def docstore_schema():
    global _docstore_schema
    if _docstore_schema is None:
        docstore_schema_path = resource_filename('lib', '/docstore/docstore_schema.json')
        unvalidated_schema = json.load(open(docstore_schema_path))
        _docstore_schema = _validate_schema(unvalidated_schema)
    return _docstore_schema

def _validate_schema(unvalidated_schema):
    unvalidated_fields = unvalidated_schema.get(u'fields', _DDL[u'fields'][u'default'])
    unvalidated_copy_fields = unvalidated_schema.get(u'copy_fields',
                                                     _DDL[u'copy_fields'][u'default'])
    assert isinstance(unvalidated_fields, _DDL[u'fields'][u'type'])
    assert isinstance(unvalidated_copy_fields, _DDL[u'copy_fields'][u'type'])

    return {
        "fields": _validate_schema_fields(unvalidated_fields),
        "copy_fields": _validate_schema_copy_fields(unvalidated_copy_fields, unvalidated_schema)
    }

def _validate_schema_fields(unvalidated_fields):
    fields_schema = _DDL[u'fields'][u'schema']
    field_required_props = [schema_field for schema_field in fields_schema \
                            if fields_schema[schema_field].get(u'required')]

    validated_fields = {}
    for field in unvalidated_fields:
        field = deepcopy(field)
        validated_field = {}
        _validate_properties(field, fields_schema, field_required_props)

        fname = field.pop(u'name')
        ftype = field.pop(u'type')
        assert ftype in fields_schema[u'type'][u'enum'], \
            u'Invalid type "{}" for docstore field "{}"'.format(ftype, fname)
        validated_field[u'type'] = ftype

        fmulti_valued = _validate_multi_valued(field, fields_schema, validated_field)
        findexed, fstored, fanalyzer = _validate_field_index(field, ftype, fmulti_valued,
                                                             fields_schema, validated_field)
        _validate_solr_field(field, fname, ftype, findexed, fstored, fmulti_valued,
                             fanalyzer, fields_schema, validated_field)

        # Add any remaining fields from the schema that have not already been
        # handled as special cases
        for key, val in field.iteritems():
            assert isinstance(val, fields_schema[key]['type'])
            validated_field[key] = val

        # Add a mongo field name for every validated field if one has not been
        # explicitly set already
        validated_field.setdefault(u'mongo_field', fname)

        validated_fields[fname] = validated_field

    return validated_fields

def _validate_schema_copy_fields(copy_fields, unvalidated_fields):
    copy_fields_schema = _DDL[u'copy_fields'][u'schema']
    copy_field_required_props = [schema_field for schema_field in copy_fields_schema \
                                 if copy_fields_schema[schema_field].get(u'required')]
    for copy_field in copy_fields:
        _validate_properties(copy_field, copy_fields_schema, copy_field_required_props)

        cf_source, cf_dest = copy_field['source'], copy_field['dest']
        _validate_copy_field_types(copy_fields_schema, cf_source, cf_dest)
        _validated_copy_field_solr_field(unvalidated_fields, cf_source, cf_dest)

    return copy_fields

def _validate_properties(field, schema, required_props):
    for prop in field:
        assert prop in schema, \
            u"Invalid property {} in docstore field schema".format(prop)
    for required_prop in required_props:
        assert required_prop in field, \
            u'Required field property "{}" does not exist in docstore schema' \
            .format(required_prop)

def _validate_multi_valued(field, fields_schema, validated_field):
    fmulti_valued = field.pop(u'multi_valued', fields_schema[u'multi_valued'][u'default'])
    assert isinstance(fmulti_valued, fields_schema[u'multi_valued'][u'type'])
    validated_field[u'multi_valued'] = fmulti_valued
    if fmulti_valued:
        fmulti_valued_type = field.pop(u'multi_valued_type', \
                                        fields_schema[u'multi_valued_type'][u'default'])
        assert isinstance(fmulti_valued_type, fields_schema[u'multi_valued_type'][u'type'])
        assert fmulti_valued_type in fields_schema[u'multi_valued_type'][u'enum']
        validated_field[u'multi_valued_type'] = fmulti_valued_type

        fmulti_valued_operation = field.pop(u'multi_valued_operation', \
                                            fields_schema[u'multi_valued_operation'][u'default'])
        assert isinstance(fmulti_valued_operation,
                          fields_schema[u'multi_valued_operation'][u'type'])
        assert fmulti_valued_operation in fields_schema[u'multi_valued_operation'][u'enum']
        validated_field[u'multi_valued_operation'] = fmulti_valued_operation
    return fmulti_valued

def _validate_field_index(field, ftype, fmulti_valued, fields_schema, validated_field):
    findexed = field.pop(u'indexed', fields_schema[u'indexed'][u'default'])
    assert isinstance(findexed, fields_schema[u'indexed'][u'type'])
    validated_field[u'indexed'] = findexed

    fstored = field.pop(u'stored', fields_schema[u'stored'][u'default'])
    assert isinstance(fstored, fields_schema[u'stored'][u'type'])
    validated_field[u'stored'] = fstored

    fanalyzer = None
    if findexed:
        default_analyzer = fields_schema[u'analyzer'][u'default'][ftype]
        if isinstance(default_analyzer, dict):
            default_analyzer = default_analyzer[u'multi_valued'] \
                if fmulti_valued else default_analyzer[u'']
        fanalyzer = field.pop(u'analyzer', default_analyzer)
        assert isinstance(fanalyzer, fields_schema['analyzer']['type'])
        assert fanalyzer in fields_schema['analyzer']['enum']
        validated_field[u'analyzer'] = fanalyzer
    else:
        # If the field is not indexed then the analyzer does not matter
        field.pop(u'analyzer', None)

    return findexed, fstored, fanalyzer

def _validate_solr_field(field, fname, ftype, findexed, fstored, fmulti_valued,
                         fanalyzer, fields_schema, validated_field):
    if findexed or fstored:
        dynamic_field_suffix = fields_schema[u'solr_field'][u'dynamic_field_suffix'][ftype]
        if isinstance(dynamic_field_suffix, dict):
            dynamic_field_suffix = dynamic_field_suffix[u'multi_valued'] \
                if fmulti_valued else dynamic_field_suffix[u'']
        if fanalyzer == 'other':
            assert 'solr_field' in field, u'Specifying an analyzer of "other" ' \
                'in docstore schema requires explicit "solr_field" to be defined'
        fsolr_field = field.pop(u'solr_field', fname + dynamic_field_suffix)
        assert isinstance(fsolr_field, fields_schema[u'solr_field'][u'type'])
        validated_field[u'solr_field'] = fsolr_field

def _validate_copy_field_types(copy_fields_schema, cf_source, cf_dest):
    assert isinstance(cf_source, copy_fields_schema['source']['type'])
    assert isinstance(cf_dest, copy_fields_schema['dest']['type'])

def _validated_copy_field_solr_field(unvalidated_fields, cf_source, cf_dest):
    try:
        unvalidated_source_field = next(field for field in unvalidated_fields['fields'] \
                                        if field['name'] == cf_source)
    except StopIteration:
        raise AssertionError('Copy field source "{}" in docstore schema must ' \
                             'exist is fields list'.format(cf_source))
    try:
        unvalidated_dest_field = next(field for field in unvalidated_fields['fields'] \
                                      if field['name'] == cf_dest)
    except StopIteration:
        raise AssertionError('Copy field destination "{}" in docstore schema must ' \
                             'exist is fields list'.format(cf_dest))

    assert unvalidated_source_field.get('solr_field'), 'Explicit solr ' \
        'field must be provided for copyfield source "{}"'.format(cf_source)
    assert unvalidated_dest_field.get('solr_field'), 'Explicit solr ' \
        'field must be provided for copyfield destination "{}"'.format(cf_dest)
