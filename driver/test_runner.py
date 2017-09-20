# stdlib imports
import datetime
# local imports
from asana_driver import AsanaDriver
from driver_wrapper import ETLTaskLite


# constants
BUTTER_USER_ID = 2
DATASOURCE_USER_ID = "danial@butter.ai"


def test1():
    with AsanaDriver(BUTTER_USER_ID, DATASOURCE_USER_ID) as driver:
        #driver.retrieve_metadata(None)
        driver.retrieve_metadata({ 'lastrun': datetime.datetime.utcnow() - datetime.timedelta(days=1) })
        pass
    return

def test2():
    with AsanaDriver(BUTTER_USER_ID, DATASOURCE_USER_ID) as driver:
        #print 'Attachment', driver.retrieve_data({ 'external_id': 431133620991715 , 'subtype': 'attachment' })
        print 'Task', driver.retrieve_data({ 'external_id': 431129730173995 })
        print 'Task', driver.retrieve_data({ 'external_id': 434588403580550 })
        pass
    return


if __name__ == '__main__':
    test1()
    pass
