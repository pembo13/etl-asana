from lib import RetrieveMetadataResult, RateLimitError, \
                RetrieveDataResult

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
        f = open("lib/sample_data/"+FILE_REFERENCE[doc['id']], 'r')
        return RetrieveDataResult(data=f.read())
