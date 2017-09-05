# Project Overview

The goal for this project is to allow developers to independently create data source connectors allowing for access to 3rd party data (think Dropbox, Google Drive, Gmail, Evernote, etc.). The end result is the implementation of two functions: `retrieve_metadata` and `retrieve_data`. `retrieve_metadata` gets executed on a per user basis, while `retrieve_data` gets executed on a per doc basis. 

#### `retrieve_metadata`
This function is used to index the details of a particular doc. For example, if a user's Dropbox has 5 files, this function would import those 5 files with details like last_updated, mimetype, name, size, among others (all of which will be specified). There's no guarantee that a single run of this function will capture all a user's data. Larger accounts will be indexed over multiple invocations. There is a persistent object, `milestone`, that will be made available to help keep track of cursors, allowing a seamless continuation of progress.

#### `retrieve_data`
This function is used to download the actual content for the docs which were captured from `retrieve_metadata`. This exists separately from `retrieve_metadata` and on a doc-by-doc basis so that the IO doesn't block anything else.


Examples of both functions exist within `driver/sample_driver.py`

# Technical Details

There are several requirements for this project:

- python 2.7.12
- pip
- JVM (used by `tika` for context extraction, see: https://pypi.python.org/pypi/tika)

Once the requirements are satisfied, you can do:
`pip install -r requirements.txt`

The sample driver was built with several stages of the ETL process in mind. You can execute `python driver/runner.py` from the root directory and see several different stages:

- After first run:
    There will be 3 docs created inside of the mongo db `docstore` and collection `docs`. This will have been the work of `retrieve_metadata` within the SampleDriver.

- After the second run:
    Content will be extracted (using the `driver/lib/sample_data` files) and associated with the docs. This will have been the work of `retrieve_data` within SampleDriver.

- After the third run:
    Doc #2 will be removed, showcasing how `RetrieveMetadataResult` options work.

- After the fourth run:
    The persistent dictionary `milestone` will come into play by raising a `RateLimitError`, which won't allow execution until the time
    specified has passed. You can manually remove the `next-sync` key from the milestone to get around this.


Majority of your work will be done in a new `Driver` class. When developing, change out the `SampleDriver` in `driver/runner.py` with your own. `runner.py` and `driver_wrapper.py` are not set in stone, though any changes made to these files should be discussed before committing to them.
