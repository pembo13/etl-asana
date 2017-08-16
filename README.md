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
