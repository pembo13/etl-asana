ETL function will connect to Box API to download the following data:

 

 


||DocStore Field||API Field||Type||Indexed||Stored||Multivalued||Example||
|title|name|string|true|false|false|"tigers.jpg"|
|alternative_title|description|string|true|false|false|"a picture of tigers"|
|path|path_collection|string|false|false|true|["All files", "Pictures", "Tigers"]|
|container_url| |string|false|false|false| |
|file_size|size|int|false|false|false|644544|
|created|created_at|datetime|true|false|false|"2012-12-12T10:55:30-08:00"|
|edited|modified_at|datetime|true|false|false|"2012-12-12T10:55:30-08:00"|
|mime| |string|true|false|false|"image/jpeg"|
|url| |string|false|false|false| |
|author|created_by|string|true|false|true|["sean rose sean@box.com"]|
|editor|modified_by|string|true|false|true|["sean rose sean@box.com"]|
|bx_author|created_by|string|false|false|true|["sean rose", "sean@box.com", "3545454"]|
|bx_editor|modified_by|string|false|false|true|["sean rose", "sean@box.com", "3545454"]|
|bx_owner|owned_by|string|false|false|true|["sean rose", "sean@box.com", "3545454"]|

h4. Notes
* {{container_url}} should be the url to the "Tigers" folder from this example
* {{mime}} does not seem to be offered by the API. It needs to be computed
* It is currently unclear how {{url}} will be retrieved

[https://developer.box.com/reference#files]

[https://community.box.com/t5/Box-Developer-Forum/Direct-download-link-to-a-BOX-file/td-p/30876]

 

 
