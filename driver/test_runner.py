# local imports
from asana_driver import AsanaDriver
from driver_wrapper import ETLTaskLite


# constants
BUTTER_USER_ID = 2
DATASOURCE_USER_ID = "danial@butter.ai"


def test():
    driver = AsanaDriver(BUTTER_USER_ID, DATASOURCE_USER_ID)
    driver.retrieve_metadata(None)
    return


if __name__ == '__main__':
    test()
    pass
