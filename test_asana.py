# stdlib imports
import json
import os
import sys
# third-party imports
# local imports
from driver.asana_driver import AsanaDriver


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
        driver.retrieve_metadata(None)
        #print 'Attachment', driver.retrieve_data({ 'external_id': 431133620991715 , 'subtype': 'attachment' })
        pass
    return


if __name__ == '__main__':
    test()
    pass
