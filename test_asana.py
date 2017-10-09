# stdlib imports
import datetime
import json
import os
import pprint
import sys
# third-party imports
# local imports
from driver.asana_driver import AsanaDriver


pp = pprint.PrettyPrinter(indent=4)


def authenticate(filename='asana.credentials', return_credentials=False):
    credentials = {
        'personal_access_token': None,
    }
    
    if os.path.isfile(filename):
        with open(filename, 'rb') as fin:
            try:
                credentials.update( json.load(fin) )
            except ValueError:
                pass

    return credentials if return_credentials else None

def test():
    credentials = authenticate(return_credentials=True)
    
    if not credentials:
        sys.exit(0)
    
    with AsanaDriver(
        personal_access_token=credentials.get('personal_access_token')
    ) as driver:
        print 'All:'
        results = driver.retrieve_metadata({})
        for doc in results.docs:
            pp.pprint(doc)
        
        print 'Last Run:'
        results = driver.retrieve_metadata({ 'lastrun': datetime.datetime.utcnow() - datetime.timedelta(days=30) })
        for doc in results.docs:
            pp.pprint(doc)
        
        print 'File:',
        result = driver.retrieve_data({ 'external_id': 431133620991715 , 'subtype': 'attachment' })
        print len(result.data), 'bytes'
        pass
    return


if __name__ == '__main__':
    test()
    pass
