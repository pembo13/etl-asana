# -*- coding: utf-8 -*-

# stdlib imports
import datetime
import os
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


def parse_date(s):
    if not s:
        return None

    if isinstance(s, datetime.datetime):
        return s.date()

    if isinstance(s, datetime.date):
        return s
    
    dt = dateparser.parse(s)
    
    return dt.date()

def parse_datetime(s):
    if not s:
        return None

    if isinstance(s, datetime.datetime) and s.tzinfo:
        return s

    if isinstance(s, datetime.datetime):
        dt = s
    else:
        dt = dateparser.parse(s)

    if dt.tzinfo:
        dt = pytz.utc.normalize(dt.astimezone(pytz.utc))
    else:
        dt = pytz.utc.localize(dt)

    return dt


class AsanaDriver(object):

    def __init__(self, personal_access_token):
        self.personal_access_token = personal_access_token
        
        # initialize some fields
        self.client = None
        self.me = None
        self.workspaces = None
        self.projects = None
        return
    
    def __enter__(self):
        self.setup()
        return self

    def __exit__(self, type, value, traceback):
        self.teardown()
        return

    def _iter_tasks(self, modified_since=None):
        """
        Returns a list of all tasks.
        """
        params = {}
        if modified_since is not None: params['modified_since'] = modified_since.isoformat()
        
        for p in self.projects:
            params['project'] = p['id']
            
            for t in self.client.tasks.find_all(params):
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
                
                return RetrieveDataResult(data=r.content)
            else:
                raise Exception( 'unexpected subtype: ' + repr(s) )
        except asana.error.NotFoundError, e:
            raise ServiceUnavailableError( e.message )
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
        modified_since = milestone.get('lastrun')
        modified_since = parse_datetime(modified_since)

        docs = []
        
        try:
            for t in self._iter_tasks(modified_since=modified_since):
                task = self.client.tasks.find_by_id(task=t['id'])
                task_created = parse_datetime( task['created_at'] )
                task_modified = parse_datetime( task['modified_at'] )
                
                if modified_since is None or task_modified >= modified_since:
                    # build document meta for task
                    doc = {
                        'external_id' : task['id'],
                        'dirty': False,
                        
                        'title' : task['name'],
                        'content' : task['notes'],
                        'url': 'https://app.asana.com/0/{project_id}/{task_id}'.format(project_id=t['project']['id'], task_id=t['id']),
                        'path': map(lambda p: p['name'], task['projects']),
                        'created': task_created,
                        'edited': task_modified,
                        'tag': map(lambda tag: tag['name'], task['tags']),
                        'as_assignee': [ task['assignee']['name'] , task['assignee']['id'] ] if task['assignee'] else None,
                        'as_completed': task['completed'],
                        'as_completed_date': parse_datetime( task['completed_at'] ),
                        'as_due_date': parse_datetime( task['due_at'] or task['due_on'] or None ),
                        'as_hearted': task['hearted'],
                        'parent_name' : task['parent']['name'] if task['parent'] else None,
                    }
                    docs.append(doc)
                    pass
                
                # get attachments for task
                attachments = self.client.attachments.find_by_task(task=t['id'])
                
                # loop through attachments, and add to document pool
                for a in attachments:
                    attachment = self.client.attachments.find_by_id(attachment=a['id'])
                    attachment_created = parse_datetime( attachment['created_at'] )
                    
                    if modified_since and attachment_created < modified_since:
                        continue
                    
                     # build document meta for attachment
                    doc = {
                        'external_id' : attachment['id'],
                        'subtype' : 'attachment',
                        'dirty': True,
                        
                        'title' : attachment['name'],
                        'created': attachment_created,
                        'path': [ attachment['parent']['name'] ],
                        'container_url' : None,
                        'parent_name' : attachment['parent']['name'],
                    }
                    docs.append(doc)
                    pass
                pass
            
            # update milestone
            milestone['lastrun'] = datetime.datetime.utcnow()
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

    def setup(self):
        """
        Creates connection to Asasna API, and pulls initial data.
        """
        
        if self.client is not None: return
        
        # create api client
        self.client = asana.Client.access_token(self.personal_access_token)
        
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

    def teardown(self):
        return

    pass
