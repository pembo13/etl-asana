# -*- coding: utf-8 -*-

# stdlib imports
import os
import pprint
# third-party imports
import asana
import dateparser
import pytz
import requests
# local imports
from lib import AuthRevokedError
from lib import RateLimitError
from lib import RetrieveDataResult
from lib import RetrieveMetadataResult
from lib import ServiceUnavailableError


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
        personal_access_token = os.environ['ASANA_ACCESS_TOKEN'] # TODO: handle authentication better
        
        # create api client
        self.client = asana.Client.access_token(personal_access_token)
        
        # pull initial data
        try:
            self.me = self.client.users.me()
            self.workspaces = self.me['workspaces']
            self.projects = []
            for w in self.workspaces:
                for p in self.client.projects.find_all({ 'workspace': w['id'] }):
                    #p['workspace'] = w
                    self.projects.append(p)
                    pass
                pass
        except asana.error.NoAuthorizationError, e:
            raise AuthRevokedError( e.message )
        except asana.error.RateLimitEnforcedError, e:
            raise RateLimitError( e.message )
       
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
        external_id = doc['external_id']
        subtype = doc.get('subtype')
        
        try:
            if subtype == 'attachment':
                attachment = self.client.attachments.find_by_id(attachment=external_id)
                
                url = attachment['download_url']
                
                r = requests.get(url)
                
                return RetrieveDataResult(data=r.content) # TODO: check if this okay for potentially large files
            elif subtype is not None:
                raise Exception( 'unexpected subtype: ' + repr(s) )
            else:
                task = self.client.tasks.find_by_id(task=external_id)
                
                return RetrieveDataResult(data=task['notes']) # Note: technically, a this could be empty
        except asana.error.NotFoundError, e:
            raise ServiceUnavailableError( e.message ) # TODO: doesn't seem like the best exception to raise
        except asana.error.RateLimitEnforcedError, e:
            raise RateLimitError( e.message )
        return

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
        
        if milestone is None: milestone = { 'step':'stage1' } # TODO: verify use of milestone
        
        docs = []
        
        try:
            for t in self.iter_tasks():
                task = self.client.tasks.find_by_id(task=t['id'])
                
                # build document meta for task
                doc = {
                    'id' : task['id'], # TODO: verify id to be used
                    'external_id' : task['id'], # TODO: verify id to be used
                    'dirty' : True,
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
                docs.append(doc) ; pp.pprint(doc)
                
                # get attachments for task
                attachments = self.client.attachments.find_by_task(task=t['id'])
                
                # loop through attachments, and add to document pool
                for a in attachments:
                    attachment = self.client.attachments.find_by_id(attachment=a['id'])
                    
                     # build document meta for attachment
                    doc = {
                        'id' : attachment['id'], # TODO: verify id to be used
                        'external_id' : attachment['id'], # TODO: verify id to be used
                        'subtype' : 'attachment',
                        'title' : attachment['name'],
                        'url' : attachment['download_url'],
                        'created': parse_date( attachment['created_at'] ),
                        'path': [ attachment['parent']['name'] ],
                        'container_url' : attachment['view_url'],
                    }
                    docs.append(doc) ; pp.pprint(doc)
                    pass
                pass
        except asana.error.RateLimitEnforcedError, e:
            raise RateLimitError( e.message )
            
        # build result object
        result = RetrieveMetadataResult(
            milestone=milestone,
            retrieve_metadata_done=True,
            docs=docs
        )
        
        return result

    pass
