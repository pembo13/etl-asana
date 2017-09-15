# -*- coding: utf-8 -*-

# stdlib imports
import os
import pprint
# third-party imports
import asana
import dateparser
import pytz
# local imports
from lib import RetrieveMetadataResult
from lib import RateLimitError
from lib import RetrieveDataResult


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


pp = pprint.PrettyPrinter(indent=4)


def parse_date(s):
    if not s: return None
    
    dt = dateparser.parse(s)
    dt = pytz.utc.normalize(dt.astimezone(pytz.utc))
    
    return dt


class AsanaDriver(object):

    def __init__(self, butter_user_id, datasource_user_id):
        self.butter_user_id = butter_user_id
        self.datasource_user_id = datasource_user_id
        
        # pull api credentials
        personal_access_token = os.environ['ASANA_ACCESS_TOKEN']
        
        # create api client
        self.client = asana.Client.access_token(personal_access_token)
        
        # pull initial data
        self.me = self.client.users.me()
        self.workspaces = self.me['workspaces']
        self.projects = []
        for w in self.workspaces:
            for p in self.client.projects.find_all({ 'workspace': w['id'] }):
                #p['workspace'] = w
                self.projects.append(p)
                pass
            pass
       
        # configure client
        asana.Client.DEFAULTS['page_size'] = 1000
        return

    def iter_tasks(self):
        """
        Returns a list of all tasks.
        """
        
        for p in self.projects:
            for t in self.client.tasks.find_all({ 'project': p['id'] }):
                t['project'] = p
                yield t
                pass
            pass
        
        return

    def list_tasks(self):
        """
        Returns a list of all tasks.
        """
        
        items = []
        
        for p in self.projects:
            for t in self.client.tasks.find_all({ 'project': p['id'] }):
                t['project'] = p
                print t
                pass
            pass
        
        return items

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
        
        if milestone is None: milestone = { 'step':'stage1' }
        
        docs = []
        
        for t in self.iter_tasks():
            task = self.client.tasks.find_by_id(task=t['id'])
            
            # build document meta
            doc = {
                'dirty' : True,
                'id' : t['id'],
                'title' : t['name'],
                'url': 'https://app.asana.com/0/{project_id}/{task_id}'.format(project_id=t['project']['id'], task_id=t['id']),
                'created': parse_date( task['created_at'] ),
                'edited': parse_date( task['modified_at'] ),
                'tag': map(lambda tag: tag['name'], task['tags']),
                'as_assignee': [ task['assignee']['name'] , task['assignee']['id'] ] if task['assignee'] else None,
                'as_completed': task['completed'],
                'as_completed_date': parse_date( task['completed_at'] ),
                'as_hearted': task['hearted'],
            }
            #pp.pprint(doc)
            docs.append(doc)
            pass
        
        result = RetrieveMetadataResult(
            milestone=milestone,
            retrieve_metadata_done=True,
            docs=docs
        )
        
        return result

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

    pass
