from sample_driver import SampleDriver
from driver_wrapper import ETLTaskLite

BUTTER_USER_ID = 2
DATASOURCE_USER_ID = "danial@butter.ai"

if __name__ == '__main__':
    driver = SampleDriver(BUTTER_USER_ID, DATASOURCE_USER_ID)
    task = ETLTaskLite(BUTTER_USER_ID, DATASOURCE_USER_ID, driver)
    task.start()
