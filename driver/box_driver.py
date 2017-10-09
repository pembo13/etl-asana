# -*- coding: utf-8 -*-

# stdlib imports
from collections import deque
import datetime
import os
# third-party imports
import boxsdk
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


class BoxDriver(object):

    def __init__(self, client_id, client_secret, access_token, refresh_token):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token
        self.refresh_token = refresh_token

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

    def _iter_files(self, modified_since=None):
        """
        Returns a list of all files.

        See: https://github.com/box/box-python-sdk/blob/1.5/boxsdk/object/file.py
        """

        queue = deque([
            ('0', [])
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

            items_left = True
            limit = 100
            offset = 0

            while items_left:
                items = self.client.folder(folder_id=folder_id).get_items(fields=fields, limit=limit, offset=offset)
                items_left = len(items) >= limit
                offset += len(items)

                for item in items:
                    if item['type'] == 'folder':
                        queue.append(( item['id'] , parent_path+[item['id']] ))
                    elif item['type'] == 'file':
                        yield item
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

        try:
            if subtype == 'file':
                item = self.client.file(file_id=external_id).get()

                return RetrieveDataResult(data=item.content())
        except boxsdk.exception.BoxAPIException, e:
            if e.code == 429:
                raise RateLimitError( e.message )
            
            raise
        except boxsdk.exception.BoxOAuthException, e:
            raise AuthRevokedError( e.message )

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
            for item in self._iter_files(modified_since=modified_since):
                item_created = parse_datetime( item['created_at'] )
                
                if modified_since and item_created < modified_since:
                    continue

                # build document meta for task
                doc = {
                    'external_id' : item['id'],
                    'dirty': True,

                    'title' : item['name'],
                    'content' : item['description'],
                    'url': item.get_url(),
                    'path': map(lambda p: p['name'], item['path_collection']['entries']),
                    'created': item['created_at'],
                    'edited': item['modified_at'],
                    'tag': item['tags'],
                    'parent_name' : item['parent']['name'] if item['parent'] else None,
                }
                docs.append(doc)
                pass
        except boxsdk.exception.BoxAPIException, e:
            if e.code == 429:
                raise RateLimitError( e.message )
            
            raise
        except boxsdk.exception.BoxOAuthException, e:
            raise AuthRevokedError( e.message )

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

        # pull credentials
        self.oauth = boxsdk.OAuth2(
            client_id=self.client_id,
            client_secret=self.client_secret,
            access_token=self.access_token,
            refresh_token=self.refresh_token
        )

        # create api client
        self.client = boxsdk.Client(self.oauth)

        return

    def teardown(self):
        return

    pass
