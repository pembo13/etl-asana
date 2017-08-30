ETL function will connect to OneDrive API to download the following data:

 
||DocStore Field||API Field||Type||Indexed||Stored||Multivalued||Example||
|subtype|folder|string|false|false|false|"folder"|
|title|name|string|true|false|false|"example.xlsx"|
|path|parentReference|string|false|false|true|"/drive/root"|
|container_url|parentReference|string|false|false|false|"http://onedrive.com/..."|
|file_size|size|int|false|false|false|1234|
|created|createdDateTime|datetime|true|false|false|"2012-12-12T10:55:30-08:00"|
|edited|lastModifiedTimeDate|datetime|true|false|false|"2012-12-12T10:55:30-08:00"|
|mime|mimeType|string|false|false|false|"application/vnd.ms-excel"|
|url|webURL|string|false|false|false|"http://onedrive.com/..."|
|author|createdBy|string|true|false|true|["Ryan Gregg"]|
|editor|lastModifiedBy|string|true|false|true|["Ryan Gregg"]|
|od_author|created_by|string|false|false|true|["Ryan Gregg", "1234"]|
|od_editor|modified_by|string|false|false|true|["Ryan Gregg", "1234"]|

h4. Notes
* {{subtype}} will be the string "folder" if this OneDrive object is a folder, otherwise the field will be left empty
* {{container_url}} should be computed from {{parentReference}} {{driveId}}
* {{container_url}} should come from {{parentReference}} {{path}}

[https://dev.onedrive.com/items/get.htm]

[https://dev.onedrive.com/items/download.htm]

 

 
