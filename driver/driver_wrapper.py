import sys
import time

from tika import parser

from lib.docstore import DocstoreLite
from lib.db import get_milestone, upsert_milestone
from lib import RetrieveMetadataResult, AuthRevokedError, RateLimitError, \
                ServiceUnavailableError

class ETLTaskLite(object):
    def __init__(self, butter_user_id, datasource_user_id, driver):
        self.butter_user_id = butter_user_id
        self.datasource_user_id = datasource_user_id
        self.docstore = DocstoreLite.get_instance()
        self.driver = driver
        self.milestone = get_milestone(butter_user_id, datasource_user_id) or {}


    def handle_dirty_docs(self):
        dirty_docs = self.docstore.select(butter_user_id=self.butter_user_id,
                                          datasource_user_id=self.datasource_user_id,
                                          dirty=True)

        for doc in dirty_docs:
            result = self.driver.retrieve_data(doc)

            if result.data or result.unicode_data:
                doc['content'] = parser.from_buffer(result.data)

            doc['dirty'] = False
            self.docstore.update_raw([doc])
            update_docs = [doc] + result.docs

            for d in [d for d in result.docs if 'butter_user_id' not in d]:
                d['butter_user_id'] = doc['butter_user_id']
                d['datasource_user_id'] = doc['datasource_user_id']
            self.docstore.update_raw(update_docs)

            if result.should_remove_doc:
                self.docstore.delete(doc)

            if result.should_remove_children:
                # handle any docs to be deleted
                self.remove_child_docs(doc,
                                       self.butter_user_id,
                                       self.datasource_user_id,
                                       [d['id'] for d in result.docs])

    def remove_child_docs(self, doc, butter_user_id, datasource_user_id, ids_to_keep=None):
        """
        Remove unmentioned children of indicated doc.

        Remove all docs that are children of the doc with parent_id, and
        whose id value is not in ids_to_keep
        """
        to_delete = []
        params = {
            'butter_user_id': butter_user_id,
            'datasource_user_id': datasource_user_id,
            'type': doc['type'],
            'parent_id': doc['id'],
        }
        for doc in self.docstore.select(**params):
            if doc['id'] not in ids_to_keep:
                title = doc.get('title', '_Untitled Evernote Resource_')
                to_delete.append(doc)

        if to_delete:
            self.docstore.delete(to_delete)

    def start(self):
        if self.dirty_doc_count():
            self.handle_dirty_docs()
            sys.exit()

        if 'next-sync' in self.milestone and self.milestone['next-sync'] >= int(time.time()):
            delta = self.milestone['next-sync'] - int(time.time())
            print('waiting {} seconds before running again'.format(delta))
            sys.exit(0)


        result = RetrieveMetadataResult(milestone=self.milestone,
                                        retrieve_metadata_done=False)
        while not result.retrieve_metadata_done:
            try:
                result = self.driver.retrieve_metadata(self.milestone)
            except AuthRevokedError:
                print('AuthRevokoedError caught')
            except RateLimitError as rate_limit:
                next_run = int(time.time()) + rate_limit.duration_seconds
                self.milestone['next-sync'] = next_run
                result = RetrieveMetadataResult(milestone=self.milestone, retrieve_metadata_done=True)
            except ServiceUnavailableError as service_error:
                print('ServiceUnavailableError caught')

        self.process_docs(result)
        self.process_deletions(result.doc_ids_to_remove)
        upsert_milestone(self.butter_user_id,
                         self.datasource_user_id,
                         result.milestone)

    def dirty_doc_count(self):
        dirty_docs = self.docstore.select(butter_user_id=self.butter_user_id,
                                          datasource_user_id=self.datasource_user_id,
                                          dirty=True)

        return len(dirty_docs)

    def process_deletions(self, doc_ids_to_remove):
        """
        Delete docs with ids in doc_ids_to_remove
        :param list doc_ids_to_remove:
        """
        for doc_id in doc_ids_to_remove:
            params = {
                'butter_user_id': self.butter_user_id,
                'datasource_user_id': self.datasource_user_id,
                'external_id': doc_id,
            }
            docs = self.docstore.select(**params)
            self.docstore.delete(docs)

    def process_docs(self, result):
        """
        Push metadata doc updates in result to Solr.
        """
        if not result.docs:
            return

        self.docstore.update(self.butter_user_id,
                             self.datasource_user_id,
                             result.docs)
