ETL function will connect to Asana API to download the following data:

h3.  For tasks:
||DocStore Field||API Field||Type||Indexed||Stored||Multivalued||Example||
|title|name|string|true|false|false|"buy catnip"|
|url| |string|false|false|false| |
|path|projects|string|false|false|true|["All files", "Pictures", "Tigers"]|
|container_url| |string|false|false|false| |
|created|created_at|datetime|true|false|false|"2012-12-12T10:55:30-08:00"|
|edited|modified_at|datetime|true|false|false|"2012-12-12T10:55:30-08:00"|
|tag|tags|string|true|false|true|["Grade A"]|
|as_assignee|assignee|string|false|false|true|["Tim Bazaro", "12345"]|
|as_completed|completed|boolean|false|false|false|false|
|as_completed_date|completed_date|datetime|false|false|false|"2012-12-12T10:55:30-08:00"|
|as_hearted|hearted|boolean|false|false|false|true|

h4. Notes
* What will we show for {{path}} when a task is part of multiple projects?
* What will be the link to the parent {{container_url}} when a task is part of multiple projects?
* Where do we get {{url}} from api?
* Where do we get {{container_url}} from api?

h3. For attachments:
||DocStore Field||API Field||Type||Indexed||Stored||Multivalued||Example||
|subtype| |string|true|false|false|"attachment"|
|mime| |string|true|false|false|"image/png"|
|file_type| |string|true|false|true|["image", "png"]|
|title|name|string|true|false|false|"screenshot.png"|
|url|download_url|string|false|false|false|"https://www.dropbox.com/s/123/Screenshot.png?dl=1"|
|created|created_at|datetime|true|false|false|"2012-12-12T10:55:30-08:00"|
|path|parent|string|false|false|true|["Bug Task"]|
|container_url|view_url|string|false|false|false|"https://www.dropbox.com/s/123/Screenshot.png"|

h4. Notes
* {{subtype}} is the literal string "attachment"

[https://asana.com/developers/api-reference/tasks]

[https://asana.com/developers/api-reference/attachments]
