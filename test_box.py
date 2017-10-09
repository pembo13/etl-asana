# stdlib imports
import datetime
import json
import os
import pprint
import sys
import urlparse
# third-party imports
import boxsdk
# local imports
from driver.box_driver import BoxDriver


REDIRECT_URL = 'https://pembo13.net/'


pp = pprint.PrettyPrinter(indent=4)


def authenticate(filename='box.credentials', oauth_class=boxsdk.OAuth2, return_credentials=False):
    credentials = {
        'client_id': None,
        'client_secret': None,
    }
    
    if os.path.isfile(filename):
        with open(filename, 'rb') as fin:
            try:
                credentials.update( json.load(fin) )
            except ValueError:
                pass
    
    if len(sys.argv) == 2:
        url = sys.argv[1]
        parts = urlparse.urlparse(url)
        params = urlparse.parse_qs( parts.query)
        
        credentials.update({
            'auth_code': params['code'][0],
        })
        pass

    def store_tokens(access_token, refresh_token):
        credentials.update({
            'access_token': access_token,
            'refresh_token': refresh_token,
        })
        
        with open(filename, 'wb') as fout:
            json.dump(credentials, fout, indent=4)
        return
    
    oauth = oauth_class(
        client_id=credentials.get('client_id'),
        client_secret=credentials.get('client_secret'),
        access_token=credentials.get('access_token'),
        refresh_token=credentials.get('refresh_token'),
        store_tokens=store_tokens
    )
    
    if credentials.get('access_token') and credentials.get('refresh_token'):
        oauth.refresh( credentials.get('access_token') )
        pass
    elif credentials.get('auth_code'):
        try:
            access_token, refresh_token = oauth.authenticate( credentials['auth_code'] )
        except boxsdk.exception.BoxOAuthException, e:
            auth_url, csrf_token = oauth.get_authorization_url(REDIRECT_URL)
            print 'Authorization URL:', auth_url
            return
    elif not credentials.get('refresh_token'):
        auth_url, csrf_token = oauth.get_authorization_url(REDIRECT_URL)
        print 'Authorization URL:', auth_url
        return
    
    with open(filename, 'wb') as fout:
        json.dump(credentials, fout, indent=4)
    
    return credentials if return_credentials else oauth


def test():
    credentials = authenticate(return_credentials=True)
    
    if not credentials:
        sys.exit(0)
    
    with BoxDriver(
        client_id=credentials.get('client_id'),
        client_secret=credentials.get('client_secret'),
        access_token=credentials.get('access_token'),
        refresh_token=credentials.get('refresh_token')
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
        result = driver.retrieve_data({ 'external_id': 232814289172 })
        print len(result.data), 'bytes'
        pass
    return


if __name__ == '__main__':
    test()
    pass
