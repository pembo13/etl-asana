# local imports
from asana_driver import AsanaDriver
from driver_wrapper import ETLTaskLite


# constants
BUTTER_USER_ID = 2
DATASOURCE_USER_ID = "danial@butter.ai"


def test1():
    driver = AsanaDriver(BUTTER_USER_ID, DATASOURCE_USER_ID)
    
    driver.retrieve_metadata(None)
    return

def test2():
    driver = AsanaDriver(BUTTER_USER_ID, DATASOURCE_USER_ID)
    
    print 'Attachment', driver.retrieve_data({ 'external_id': 431133620991715 , 'subtype': 'attachment' })
    print 'Task', driver.retrieve_data({ 'external_id': 431129730173995 })
    return


if __name__ == '__main__':
    test1()
    pass
