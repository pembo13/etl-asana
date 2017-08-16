import os

from lib import RetrieveMetadataResult, RateLimitError, \
                RetrieveDataResult

# Some dummy data mimicking a datasource.
DOCS = [
    {
        "id":"butter:sd:external_id_1",
        "external_id":"external_id_1",
        "dirty": True,
    },
    {
        "id":"butter:sd:external_id_2",
        "external_id":"external_id_2",
        "dirty": True,
    },
    {
        "id":"butter:sd:external_id_3",
        "external_id":"external_id_3",
        "dirty": True,
    },
]

# Associate sample files with the dummy data.
FILE_REFERENCE = {
    "butter:sd:external_id_1": "dictionary.pdf",
    "butter:sd:external_id_2": "invoicesample.pdf",
    "butter:sd:external_id_3": "somatosensory.pdf",
}

class SampleDriver():
    def __init__(self, butter_user_id, datasource_user_id):
        self.butter_user_id = butter_user_id
        self.datasource_user_id = datasource_user_id

    def retrieve_metadata(self, milestone):
        """
        This function gathers the file manifest from the datasource. The only
        focus here is the metadata, such as name, container, mimetype, etc. No
        content should be downloaded here, that is reserved for `retrieve_data`.

        :param milestone: A persistent dictionary unique to the butter
            user/datasource combination. Example usage is to store a cursor
            identifier that will allow the subsequent run to pick up indexing
            files where it last left off.

        :returns `RetrieveMetadataResult` with the appropriate values populated.
        """
        if not milestone:
            milestone['step'] = 'stage1'
            result = RetrieveMetadataResult(milestone=milestone,
                                            retrieve_metadata_done=True,
                                            docs=DOCS)
        elif milestone.get('step') == 'stage1':
            result = RetrieveMetadataResult(milestone=milestone,
                                            retrieve_metadata_done=True,
                                            doc_ids_to_remove=['external_id_2'])
            milestone['step'] = 'stage2'
        elif milestone.get('step') == 'stage2':
            raise RateLimitError(duration_seconds=7200) #delay next sync for 2 hours

        return result

    def retrieve_data(self, doc):
        """
        Used to download content for a single doc. Typically this involves
        passing the doc `external_id` attribute to the datasource SDK to
        initialize a download. It is not mandatory that the only function
        this performs is downloading. Gmail, for example, discovers
        all the emails in a thread using the root email within `retrieve_data`.

        :param doc: A dictionary describing a document.

        :return `RetrieveDataResult` with the appropriate values populated.
        """
        f = open(os.path.dirname(__file__) + "/lib/sample_data/"+FILE_REFERENCE[doc['id']], 'r')
        return RetrieveDataResult(data=f.read())
