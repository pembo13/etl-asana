# stdlib imports
import datetime
import json
import os
import pprint
import sys
import time
import urlparse
# third-party imports
import onedrivesdk
# local imports
from driver.onedrive_driver import OneDriveDriver


API_BASE_URL = 'https://api.onedrive.com/v1.0/'
REDIRECT_URL = 'https://pembo13.net/'
SCOPES = ['wl.signin', 'wl.offline_access', 'onedrive.readwrite']


pp = pprint.PrettyPrinter(indent=4)


class OneDriveCredentials(onedrivesdk.session.Session):

    @staticmethod
    def load_session(**load_session_kwargs):
        filename = load_session_kwargs.pop('filename', 'onedrive.credentials')
        
        credentials = {}
        
        if os.path.isfile(filename):
            with open(filename, 'rb') as fin:
                try:
                    credentials.update( json.load(fin) )
                except ValueError:
                    pass
        
        expires = credentials.get('expires')
        if expires:
            expires = int(expires)
            expires_in = expires - time.time()
            pass
        else:
            expires_in = 0
            pass
        
        session = OneDriveCredentials(
            token_type=credentials.get('token_type'),
            expires_in=expires_in,
            scope_string=credentials.get('scope'),
            access_token=credentials.get('access_token'),
            client_id=credentials.get('client_id'),
            auth_server_url=credentials.get('auth_server_url'),
            redirect_uri=credentials.get('redirect_uri'),
            refresh_token=credentials.get('refresh_token'),
            client_secret=credentials.get('client_secret')
        )
        
        return session

    def save_session(self, **save_session_kwargs):
        filename = save_session_kwargs.pop('filename', 'onedrive.credentials')
        
        credentials = {}
        
        if os.path.isfile(filename):
            with open(filename, 'rb') as fin:
                try:
                    credentials.update( json.load(fin) )
                except ValueError:
                    pass
        
        credentials.update({
            'access_token': self.access_token,
            'auth_server_url': self.auth_server_url,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'expires': int(self._expires_at),
            'redirect_uri': self.redirect_uri,
            'refresh_token': self.refresh_token,
            'scope': ' '.join(self.scope),
            'token_type': self.token_type,
        })
        
        with open(filename, 'wb') as fout:
            json.dump(credentials, fout, indent=4)
        return

    pass


def authenticate(
    api_base_url=API_BASE_URL,
    filename='onedrive.credentials',
    oauth_class=onedrivesdk.AuthProvider,
    return_credentials=False
):
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
    
    http_provider = onedrivesdk.HttpProvider()
    oauth = oauth_class(
        http_provider=http_provider,
        client_id=credentials.get('client_id'),
        scopes=SCOPES,
        session_type=OneDriveCredentials
    )

    if credentials.get('access_token') and credentials.get('refresh_token'):
        oauth.load_session(filename=filename)
        oauth.refresh_token()
        oauth.save_session(filename=filename)
        return credentials if return_credentials else oauth
    elif not credentials.get('refresh_token'):
        auth_url = oauth.get_auth_url(REDIRECT_URL)
        print 'Authorization URL:', auth_url
        print
        auth_code = raw_input('Code: ')
        auth_code = auth_code.strip()
        
        oauth.authenticate(auth_code, REDIRECT_URL, credentials.get('client_secret'))
        oauth.save_session(filename=filename)
        return
    
    with open(filename, 'wb') as fout:
        json.dump(credentials, fout, indent=4)
    
    oauth.save_session(filename=filename)
    
    return credentials if return_credentials else oauth


def test():
    credentials = authenticate(return_credentials=True)
    
    if not credentials:
        sys.exit(0)
    
    with OneDriveDriver(**credentials) as driver:
        print 'All:'
        results = driver.retrieve_metadata({})
        for doc in results.docs:
            pp.pprint(doc)
        
        print 'Last Run:'
        results = driver.retrieve_metadata({ 'lastrun': datetime.datetime.utcnow() - datetime.timedelta(days=30) })
        for doc in results.docs:
            pp.pprint(doc)
            
        print 'File:',
        result = driver.retrieve_data({ 'external_id': '7794AF724FC1B738!102' })
        print len(result.data), 'bytes'
        pass
    return

def test2():
    oauth = authenticate(return_credentials=False)
    
    if not oauth:
        sys.exit(0)
    
    client = onedrivesdk.OneDriveClient(API_BASE_URL, oauth, oauth._http_provider)
    
    collection = client.item(drive='me', id='root').children.request(top=50).get()
    for c in collection:
        print repr(c.id), c.name, c._prop_dict
    print 'finished'
    return
    with BoxDriver(
        client_id=credentials.get('client_id'),
        client_secret=credentials.get('client_secret'),
        access_token=credentials.get('access_token'),
        refresh_token=credentials.get('refresh_token')
    ) as driver:
        #driver.retrieve_metadata(None)
        #driver.retrieve_metadata({ 'lastrun': datetime.datetime.utcnow() - datetime.timedelta(days=1) })
        print 'File', driver.retrieve_data({ 'external_id': 232814289172 })
        pass
    return


if __name__ == '__main__':
    test()
    pass
