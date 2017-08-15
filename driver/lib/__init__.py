import attr
from attr.validators import instance_of, optional

@attr.s(frozen=True)
class RetrieveMetadataResult(object):
    """
    Result of calling an ETL driver's retrieve_metadata() method.

    Constructor must include milestone.
    Fields:
        milestone - Milestone dictionary including any values needed for future
            method invocations (e.g. cursors)
        docs - List of Solr documents (dictionaries) to add or update
        doc_ids_to_remove - List of Solr document IDs to remove
        actions - List of MetadataAction objects. For each action, caller should
            retrieve docs according to the filters and if any are found, pass
            them to perform_metadata_action() along with the MetadataAction
            instance before making any further calls to retrieve_metadata().
            Note: actions may be applied *before* updating Solr with docs from
            this object.
        retrieve_metadata_done - If True, no more calls to retrieve_metadata()
            are required at this time
    """
    milestone = attr.ib(validator=instance_of(dict))
    docs = attr.ib(default=attr.Factory(list), validator=instance_of(list))
    doc_ids_to_remove = attr.ib(default=attr.Factory(list),
                                validator=instance_of(list))
    actions = attr.ib(default=attr.Factory(list), validator=instance_of(list))
    retrieve_metadata_done = attr.ib(default=False,
                                     validator=instance_of(bool))

@attr.s(frozen=True)
class RetrieveDataResult(object):
    """
    Result of calling an ETL driver's retrieve_data() method.

    Fields:
        milestone - Milestone dictionary including any values needed for future
            method invocations (e.g. cursors)
        data - file-like object (e.g. file, StringIO, BytesIO).
            None in both data and unicode_data indicates that the file cannot
            be retrieved, and shouldn't be re-tried.
        unicode_data - unicode string (not to be passed through extraction).
            None in both data and unicode_data indicates that the file cannot
            be retrieved, and shouldn't be re-tried.
        docs - List of Solr documents (dictionaries) to add or update. Typically
            this will be empty, but for example the Evernote driver may discover
            note resources when it retrieves note data.
        should_remove_children - if True, caller should remove any children of
            the original doc that do not appear in the docs field
        should_remove_doc - if True, caller should remove the original doc
            (i.e. it was a placeholder)
    """
    data = attr.ib()
    unicode_data = attr.ib(default=None,
                           validator=optional(instance_of(unicode)))
    milestone = attr.ib(default=None, validator=optional(instance_of(dict)))
    docs = attr.ib(default=attr.Factory(list), validator=instance_of(list))
    should_remove_children = attr.ib(default=False, validator=instance_of(bool))
    should_remove_doc = attr.ib(default=False, validator=instance_of(bool))

    def __str__(self):
        milestone = "milestone={}".format(self.milestone)
        docs = "docs={}".format(self.docs)
        remove_children = "should_remove_children={}" \
            .format(self.should_remove_children)
        remove_doc = "should_remove_doc={}".format(self.should_remove_doc)
        data = "data=None" if self.data is None else "data=<obj>"
        unicode_data = "unicode_data=None" if self.unicode_data is None \
            else "unicode_data=<unicode>"
        return u"{}({}, {}, {}, {}, {}, {})".format(
            type(self).__name__, milestone, docs, remove_children, remove_doc,
            data, unicode_data
        )

class AuthRevokedError(RuntimeError):
    """
    Raised when ETL task discovers access to datasource has been revoked.
    """
    def __init__(self, error=None):
        super(AuthRevokedError, self).__init__(u"Authorization revoked error",
                                               error)
        self.error = error


class RateLimitError(RuntimeError):
    """
    Raised when a data source indicates a rate limit is being violated.
    """
    def __init__(self, error=None, duration_seconds=1800):
        super(RateLimitError, self).__init__("Rate limit error", error)
        self.error = error
        self.duration_seconds = duration_seconds


class ServiceUnavailableError(RuntimeError):
    """
    Raised when a data source throws an exception indicating diffultities
    on their end (e.g. HTTP 500 result)
    """
    def __init__(self, error=None):
        super(ServiceUnavailableError, self)\
            .__init__("Service unavailable error", error)
        self.error = error
