import logging
from collections import defaultdict
import datetime

from builtins import object

from dateutil.parser import parse as parsedate

DOCSTORE_INSTANCE = None

def memoize(function):
    memo = {}
    def wrapper(*args):
        if args in memo:
            return memo[args]
        else:
            rv = function(*args)
            memo[args] = rv
            return rv
    return wrapper

try:
    import pymongo
    from pymongo import ReplaceOne, UpdateOne
    from pymongo.errors import InvalidOperation
    has_pymongo = True
except ImportError:
    has_pymongo = False

from .docstore_schema import docstore_schema

class DocstoreLite(object):
    """
    DocstoreLite is Docstore without Solr. It's used during the development of new
    datasources for ETL.
    """
    def __init__(self):
        self._logger = logging.getLogger(__name__)

        if not has_pymongo:
            msg = "pymongo package not found. " \
                "Please install it in order to use Docstore."
            raise ImportError(msg)

        self._mongo = None
        self._mongo_collection = None

    def connect(self, *args, **kwargs):
        assert 'mongo_host' in kwargs
        assert 'mongo_port' in kwargs
        assert 'mongo_database' in kwargs
        assert 'mongo_collection' in kwargs

        if 'mongo_user' in kwargs and 'mongo_port' in kwargs:
            mongo_connection_string = 'mongodb://{}:{}@{}:{}' \
                .format(kwargs['mongo_user'], kwargs['mongo_pass'],
                        kwargs['mongo_host'], kwargs['mongo_port'])
        else:
            mongo_connection_string = 'mongodb://{}:{}' \
                .format(kwargs['mongo_host'], kwargs['mongo_port'])

        if 'mongo_database' in kwargs:
            mongo_connection_string += '/{}'.format(kwargs['mongo_database'])

        self._mongo = pymongo.MongoClient(mongo_connection_string, document_class=dict,
                                          tz_aware=True, connect=False)
        self._logger.debug("DocStore mongodb url: %s", self._mongo)

        db = self._mongo.get_default_database()
        self._mongo_collection = db[kwargs['mongo_collection']]

    def delete(self, docs):
        """
        Removes docs from mongo.

        :param doc_ids list(doc): A list (or some iterable) of docs with an
          'id' specifying the doc to delete.

        :return int: The number of deleted items.
        """

        deleted = 0
        if type(docs) is not list:
            docs = [docs]

        deleted = self._mongo_collection.delete_many({ '_id': {'$in': [doc['id'] for doc in docs]}})

        return deleted


    def update(self, butter_user_id, datasource_user_id, docs):
        """
        Insert or update (upsert) documents into DocStore.

        This method writes a list of documents to persistent storage, and indexes
        any document field which is marked as "indexed" in the DocStore schema
        into a reverse index for faster searching.

        :param butter_user_id int: The Butter user id for which to associate the
          inserted documents
        :param datasource_user_id str: The external datasource user id for which
          to associate the inserted documents
        :param docs list: List of dict, where each dict represents one document
          to insert. This document must adhere to the DocStore schema.
        """

        if not docs: # aint no work to be done !
            return

        def _assign_doc_stuffs(doc):
            doc['butter_user_id'] = butter_user_id
            doc['datasource_user_id'] = datasource_user_id
            doc['upsert_time'] = datetime.datetime.now().isoformat()

        [_assign_doc_stuffs(doc) for doc in docs]

        self.update_raw(docs)


    def update_raw(self, docs):
        """
        Takes a set of Docstore docs and applies them as-is to mongo.
        """

        mongo_update_requests = _mongo_update_requests_for_docs(docs)
        try:
            self._mongo_collection.bulk_write(mongo_update_requests)
        except InvalidOperation:
            # The update request to mongo resulted in no changes, bail out.
            return

    def select(self, limit=0, **kwargs):
        """
        Queries mongo based on ad hoc filters.

        :param kwargs dict: Filters for the query, valid options specified below.

        :returns list of docs
        """

        allowed_filters = ('butter_user_id', 'datasource_user_id',
                           'type', 'dirty', 'external_id', 'en_tag_guid',
                           'en_notebook_guid', 'parent_id',
                           'tr_board_id', 'tr_list_id', 'id', 'butter_team_id')
        unallowed_options = [k for k in kwargs.keys() if k not in allowed_filters]

        if unallowed_options:
            raise ValueError('Unallowed filters `%s`', unallowed_options)

        if 'id' in kwargs:
            kwargs['_id'] = kwargs['id']
            del kwargs['id']

        cursor = mongo_docs = self._mongo_collection.find(kwargs)
        if limit:
            cursor.limit(limit)
        return self._convert_mongo_docs_to_docstore(cursor)

    def get(self, butter_user_id, datasource_user_id, doc_id):
        """
        Return a single document by id.

        This method bypasses the reverse index and retrieves a document directly
        from persistent storage by unique id.

        :param butter_user_id int: The butter_user_id which owns this document
        :param datasource_user_id str: The datasource_user_id which owns this document
        :param doc_id str or dict: The document's unique_key. If the doc_id is a
          string, the DocStore schema will be used to determine the unique key
          field name. If the doc_id is a dict, keys of this dict are the field
          name and values are the field value. The DocStore schema will be used
          to validate that each key is part of a composite unique key in the
          underlying persistent storage. This method cannot be used to get document
          by any other field than a unique key.

        :return doc: Full document from persistent storage with this document id
        :rtype dict:
        """
        # Validate doc_id is a unique key in the DocStore schema

        # Get document from mongo by unique key

        # Validate the document includes butter_user_id and datasource_user_id
        # i.e. validate correct user

        params = {
            "id": doc_id,
            "butter_user_id": butter_user_id,
            "datasource_user_id": datasource_user_id
        }
        doc = self._mongo_collection.find_one(params)

        return doc

    def get_counts(self, butter_user_id):
        """
        This is used in the breadandbutter ETL function call to return the count
        of documents in solr and mongo.

        :return dict: Contains two keys, solr and mongo, with a dictionary of
                      datasources -> doc count.
         return example:
         {
             "solr": {
                "dropbpox": 100,
                "evernote": 500,
                "gmail": 30
             },
             "evernote": {
                "dropbpox": 100,
                "evernote": 500,
                "gmail": 30
             },
         }

        """
        mongo_groupings = self._mongo_collection.aggregate([
            {"$match": {"butter_user_id" : butter_user_id } },
            {"$group": {"_id":"$type", "count":{"$sum":1}}}
        ])

        mongo_doc_counts = {r['_id']: r['count'] for r in mongo_groupings}

        query = {
            'q': '*:*',
            'fq': 'butter_user_id_is:{}'.format(butter_user_id),
            'fl': 'type',
            'group': True,
            'group.field': 'type_s'
        }

        return {
            mongo_doc_counts
        }


    def _convert_mongo_docs_to_docstore(self, mongo_docs):
        mapper = field_mapper('mongo_field', 'name')
        docs = []
        for mongo_doc in mongo_docs:
            doc = {}
            for key in mongo_doc.keys():
                doc[mapper[key]] = mongo_doc[key]
            docs.append(doc)

        return docs

    @staticmethod
    def get_instance(host='localhost',
                     port=27017,
                     database='docstore',
                     collection='docs'):
        global DOCSTORE_INSTANCE
        if not DOCSTORE_INSTANCE:
            DOCSTORE_INSTANCE = DocstoreLite()
            params = {
                'mongo_host': host,
                'mongo_port': port,
                'mongo_database': database,
                'mongo_collection': collection
            }
            DOCSTORE_INSTANCE.connect(**params)

        return DOCSTORE_INSTANCE

def _mongo_update_requests_for_docs(docs):
    """
    Creates a list of pymongo BulkWriteOperation representing an insert or
    update (upsert) of the providede docs list, formatted for MongoDB.

    :param docs list: List of dict, where each dict represents one document
      to insert. This document must adhere to the DocStore schema.
    :return requests: A list of pymongo BulkWriteOperation
    :rtype list:
    """
    schema = docstore_schema()

    requests = []
    for doc in docs:
        unique_key = _mongo_unique_key_for_doc(schema, doc)

        # defaultdict of defaultdict
        # Setting a property at any level of the dictionary will create as
        # many nested defaultdicts as needed
        update = recursive_defaultdict()

        for field, value in doc.iteritems():
            schema_field = schema['fields'].get(field)

            if schema_field is None:
                raise ValueError('Cannot insert DocStore field "{}". '
                                 'Field does not exist in schema.'.format(field))

            # Format the field for inserting into MongoDB
            mongo_kv = _mongo_kv(schema_field, value)

            # Only update unique keys upon initial insert
            if schema_field.get('unique_key'):
                update['$setOnInsert'].update({mongo_kv[0]: mongo_kv[1]})

            elif schema_field['multi_valued']:
                # Use correct MongoDB array operator depending on append vs replace operation
                if schema_field['multi_valued_operation'] == 'append':

                    # Use correct MongoDB array operator depending on list vs set type
                    if schema_field['multi_valued_type'] == 'set':
                        update['$addToSet'].update({mongo_kv[0]: {'$each': mongo_kv[1]}})
                    elif schema_field['multi_valued_type'] == 'list':
                        update['$push'].update({mongo_kv[0]: {'$each': mongo_kv[1]}})
                elif schema_field['multi_valued_operation'] == 'replace':
                    update['$set'].update({mongo_kv[0]: mongo_kv[1]})
            # Common case (non-unique key, not multi_valued)
            else:
                update['$set'].update({mongo_kv[0]: mongo_kv[1]})

        # Create pymongo BulkWriteOperation
        # http://api.mongodb.com/python/current/api/pymongo/bulk.html#pymongo.bulk.BulkWriteOperation
        requests.append(UpdateOne(unique_key,
                                  defaultdict_to_dict(update),
                                  upsert=True))
    return requests


@memoize
def field_mapper(from_field, to_field):
    """
    Returns a dictionary with keys being `from_field` and mapping to
    `to_field` or vice versa.
    """

    valid_fields = ('mongo_field', 'name')

    if from_field not in valid_fields:
        raise ValueError("`from_field` value `%s` not in %s", from_field, valid_fields)

    if to_field not in valid_fields:
        raise ValueError("`to_field` value `%s` not in %s", to_field, valid_fields)


    schema = docstore_schema()
    field_map = {}
    for field_name, field_attrs in schema['fields'].iteritems():

        # `name` is stripped from `field_attrs`. lets repopulate it.
        if from_field == 'name':
            field_attrs[from_field] = field_name
        try:
            field_attrs[to_field] # test that the to_field is present
            field_map[field_attrs[from_field]] = field_attrs[to_field]
        except KeyError:
            field_map[field_attrs[from_field]] = field_name

    return field_map



def _mongo_unique_key_for_doc(schema, doc):
    """
    Returns a dictionary of {mongo_field_name: doc_field_value} of all mongo_field_names
    marked as "unique" in the DocStore schema.

    :param schema dict: Full DocStore schema. Must include a "fields" property
    :param doc dict: Document to get values for each unique_key field
    :return unique_key_for_doc: Dict where keys are the name of the field in MongoDB
      and values are the value for that field in the document
    :rtype dict:
    """
    unique_key = {}
    for schema_field, schema_field_properties in schema['fields'].iteritems():
        if schema_field_properties.get('unique_key', False):
            key = schema_field_properties['mongo_field']
            unique_key[key] = doc[schema_field]
    return unique_key

def _mongo_kv(schema_field, value):
    """
    Returns a tuple of the MongoDB field name and validated value

    The the schema_field is multi_valued, value will always be returned as a list.

    :param schema_field dict: The schema for the field being evaluated, taken from
      DocStore schema.fields list
    :param value: Value to insert for this field. Value must be of the same type
      as specified in the schema_field, or the value must be able to be cast
      into that type.

    :return kv: Tuple of (mongo_field_name, mongo_value)
    :rtype tuple:
    """

    if value is None:
        return schema_field['mongo_field'], None

    field_type = schema_field['type']

    # Ensure multi_valued value is a list
    multi_valued = schema_field['multi_valued']
    if multi_valued and not isinstance(value, list):
        value = [value]

    if field_type == 'int':
        value = [int(v) for v in value] if multi_valued else int(value)
    elif field_type == 'boolean':
        value = [bool(v) for v in value] if multi_valued else bool(value)
    # Datetime should be inserted into MongoDB as python datetime objects
    elif field_type == 'datetime':
        if type(value) is not datetime.datetime and not multi_valued:
            value = [parsedate(v) for v in value] if multi_valued else parsedate(value)
    # Location values should be inserted into MongoDB as two-element lists with x,y coordinates
    elif field_type == 'point':
        value = list(value)[:2]
    elif field_type == 'float':
        value = [float(v) for v in value] if multi_valued else float(value)

    # If multi_valued_type is "set", ensure no duplicates
    if multi_valued and \
            schema_field['multi_valued_operation'] == 'replace' and \
            schema_field['multi_valued_type'] == 'set':
        value = list(set(value))

    return schema_field['mongo_field'], value

def recursive_defaultdict():
    return defaultdict(recursive_defaultdict)

def defaultdict_to_dict(ddict):
    if isinstance(ddict, defaultdict):
        return {k: defaultdict_to_dict(v) for k, v in ddict.iteritems()}
    return ddict

