MS SQL Server to Actian Vector Schema Conversion Misc Fixes Challenge
============================

Project instructions:
============================
Fix listed bugs & add features

Installation instructions:
============================
This is fixes challenge, so no additional installation steps required.
The installation guide could be found in /doc directory

Fixes summary
============================
FIXED:
1) --loadtest now works correctly (data insertion with single INSERT statement). Also fixed counter for inserted queries.
2) exclude, include are case sensitive
3) Change Default Batch size to 500
4) The loadmethod=multitable (CORE based):
	- `loadmethod=multitable` sets threads number to available CPU cores
	- `--threads` limit is 10 if cores < number_of_cores, but global limit is 32
5) dbmv_new_*.txt file should be deleted at the very start
6) `--maxrows` option set the limit for number of inserted rows (default: 100000)
7) ERROR REPORTING OR BAD buffer of data - over-run perhaps
	- skip columns which precision > charmax (default: 6400)
8) `-h`, `--help` option
	- nice looking help documentation for tool
9) `--loadata` should override any other options dealing with --cre* --loa*

Additional fixes:
============================
1) remove duplicate '--xx' option check
2) unload uses 'with' for files (safe file usage)
3) SchemaConverter.convert: uses 'with' for connections (safe connection usage)
4) format code, remove unused imports, etc.

NOT FIXED
============================
1) Grant error => reason: more time required
2) cmd + cpvwl => Driver don't support "cpvwl" query from xml file, "cmd" couldn't connect to DB [letter to @support has been written])

Video could be found here
============================
[Fix analysis video](https://youtu.be/foNqY-6B4kc)
