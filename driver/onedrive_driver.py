# -*- coding: utf-8 -*-

# stdlib imports
from collections import deque
import datetime
import os
import pprint
import tempfile
import time
# third-party imports
import onedrivesdk
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
    if isinstance(s, datetime.date): return s
    if isinstance(s, datetime.datetime): return s.date()
    
    dt = dateparser.parse(s)
    
    return dt.date()

def parse_datetime(s):
    if not s: return None
    if isinstance(s, datetime.datetime): return s
    
    dt = dateparser.parse(s)
    if dt.tzinfo:
        dt = pytz.utc.normalize(dt.astimezone(pytz.utc))
    else:
        dt = pytz.utc.localize(dt)
    
    return dt


class OneDriveSession(onedrivesdk.session.Session):

    @staticmethod
    def load_session(**load_session_kwargs):
        session = load_session_kwargs.pop('session')
        
        return session

    def save_session(self, **save_session_kwargs):
        return

    pass

class OneDriveDriver(object):

    API_BASE_URL = 'https://api.onedrive.com/v1.0/'
    REDIRECT_URL = 'https://pembo13.net/'
    SCOPES = ['wl.signin', 'wl.offline_access', 'onedrive.readwrite']
    
    def __init__(self, **credentials):
        self.credentials = credentials
        
        # initialize some fields
        self.client = None
        self.oauth = None
        return
    
    def __enter__(self):
        self.setup()
        return self
    
    def __exit__(self, type, value, traceback):
        self.teardown()
        return

    def iter_files(self, modified_since=None):
        """
        Returns a list of all files.
        
        See: https://github.com/box/box-python-sdk/blob/1.5/boxsdk/object/file.py
        """
        
        queue = deque([
            ('root', [])
        ])
        
        fields = [
            'type',
            'id',
            'name',
            'description',
            'path_collection',
            'created_at',
            'modified_at',
            'tags',
            'parent',
        ]
        
        while len(queue) > 0:
            folder_id, parent_path = queue.popleft()
            
            collection_left = True
            limit = 100
            
            while collection_left:
                collection = self.client.item(drive='me', id=folder_id).children.request(top=limit).get()
                collection_left = len(collection) >= limit
                
                for item in collection:
                    file = item.file
                    folder = item.folder
                    
                    if folder is not None:
                        queue.append(( item.id , parent_path+[item.id] ))
                    elif file is not None:
                        yield item
                    pass
                
                if collection_left:
                    collection = onedrivesdk.ChildrenCollectionRequest.get_next_page_request(collection, self.client).get()
                    pass
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
        subtype = doc.get('subtype', 'file')
        
        if subtype == 'file':
            fd,path = tempfile.mkstemp()
            os.close(fd)
            
            self.client.item(drive='me', id=external_id).download(path)
            
            try:
                with open(path, 'rb') as fin:
                    return RetrieveDataResult(data=fin.read())
            finally:
                if os.path.isfile(path):
                    os.remove(path)
        
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
        
        if milestone is None: milestone = {  }
        
        modified_since = milestone.get('lastrun')
        modified_since = parse_datetime(modified_since)
        
        docs = []
        
        for item in self.iter_files(modified_since=modified_since):
            # build document meta for task
            path = filter(None, item.parent_reference.path.replace(u'/drive/root:', u'').split(u'/'))
            
            doc = {
                'external_id' : item.id,
                
                'title' : item.name,
                'content' : item.description or '',
                'url': item.web_url,
                'path': path,
                'created': item.created_date_time,
                'edited': item.last_modified_date_time,
                'parent_name' : path[-1] if path else '',
            }
            docs.append(doc) ; pp.pprint(doc)
            pass
        
        # update milestone
        milestone['lastrun'] = datetime.datetime.utcnow()
            
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
        
        expires = self.credentials.get('expires')
        if expires:
            expires = int(expires)
            expires_in = expires - time.time()
            pass
        else:
            expires_in = 0
            pass
        
        # pull credentials
        session = onedrivesdk.session.Session(
            token_type=self.credentials.get('token_type'),
            expires_in=expires_in,
            scope_string=self.credentials.get('scope'),
            access_token=self.credentials.get('access_token'),
            client_id=self.credentials.get('client_id'),
            auth_server_url=self.credentials.get('auth_server_url'),
            redirect_uri=self.credentials.get('redirect_uri'),
            refresh_token=self.credentials.get('refresh_token'),
            client_secret=self.credentials.get('client_secret')
        )
        
        http_provider = onedrivesdk.HttpProvider()
        self.oauth = onedrivesdk.AuthProvider(
            http_provider=http_provider,
            client_id=self.credentials.get('client_id'),
            scopes=OneDriveDriver.SCOPES,
            session_type=OneDriveSession
        )
        self.oauth.load_session(session=session)
        self.oauth.refresh_token()
        
        # create api client
        self.client = onedrivesdk.OneDriveClient(OneDriveDriver.API_BASE_URL, self.oauth, self.oauth._http_provider)
        return

    def teardown(self):
        return

    pass
