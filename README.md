# dbmv

A database schema conversion tool


dbmv.py helps you to move one database schema to another database if you have a 'live' connection available to both source and target databases. ODBC is used to both manage metadata about the schemas and to transfer data (if required), so Python-accessible ODBC drivers are required on the execution machine.

Source schemas can be from:

	    ORACLE Mysql
	    MICROSOFT SqlServer
	    ORACLE
	    POSTGRES
	    SAP Sybase IQ (V15+)
	    TERADATA
	
	
Schema description can be easily extended to any database in the destination list. You need to add missing description for "tbDefinition", etc, ... in file "dbmx.xml".

SQL output, field names, and SELECT descriptions must respect the pattern used for other existing databases.



Target schema can be:

	    ACTIAN VectorH, Vector
	    ACTIAN Ingres
	    EMC Greenplum
	    IBM Netezza (UDB, not sure about iseries and Zos)
            IBM Netezza (4.xx ?)
            ORACLE Mysql
	    MICROSOFT SqlServer
	    ORACLE
	    POSTGRES
            PROGRESS Database
            SAP Sybase IQ (V15+)
	    SAP Sybase ASA (Anywhere)
	    SAP Sybase ASE
	    SAP Hana
	    SAP MaxDB
	    TERADATA

	

Without parameters dbmv.py diplays a simple help about parameters

$ ./dbmv.py 

   Usage : 
     ['src=', 'dest=', 'loadata', 'cretab', 'creall', 'loaddl', 'parfile=', 'fdelim=', 
     'unload', 'translation=', 'quote=', 'cmdsep=']


   src    : Source database      (--src=URL)
   
   dest   : Destination database (--src=URL)

   URL : db_driver[-odbc]://db_host[:port]/dbname?db_login&login_password
   
          db_driver list:

	    ACTIAN VectorH, Vector -> vectorwise
            ACTIAN Ingres          -> ingres
	    EMC Greenplum          -> greenplum
	    IBM Netezza            -> netezza
	    ORACLE Mysql           -> mysql
	    MICROSOFT SqlServer    -> mssql
	    ORACLE                 -> oracle
	    POSTGRES               -> postgres
            PROGRESS Database      -> progress
	    SAP Sybase IQ (V15+)   -> iq
	    SAP Sybase ASA         -> asa
	    SAP Sybase ASE         -> ase
	    SAP Hana               -> hana
	    SAP MaxDB              -> maxdb
            TERADATA               -> teradata


            About ODBC : Linux database drivers path and configuration must be added in file 
            
            "../etc/driverTools.xml" (see in-file examples)
            
           (On Window systems, ODBC drivers and Datasource must exist. No additional configuration is required).

   cretab : Switch to Create tables only
   creall : Switch to all objects TABLES, INDEXES, PRIMARY KEYS, REFERENTIAL CONSTRAINTS
   loaddl : Load ddl into destination database. If not specified, a file containing all objects   
      	   is created in the current directory. In this latest case, you can reload 

   parfile: All parameters can be stored is a parfile. Parfiles examples can be find in the "wrk" directory. 

   translation : Schema-name translation for table, constraints, indexes
   (Ex: --translation=scname:public,vw_user,postgres,vw_user;iscname:public,vw_user,postgres,vw_user;rscname:public,vw_user,postgres,vw_user)
   
                 
   		   scname : Tables owned by "public" are moved to schema "vw_user"
                                            "postgres"                   "vw_user"
                                            
                   iscname: Indexes owner by "public" are moved to schema "vw_user"
                                             "postgres"                   "vw_user"
                                             
                   rscname: Constraints owner by "public" are moved to schema "vw_user"
                                                 "postgres"                   "vw_user"


   quote : Quote DDL (Ex: --quote='"' => CREATE TABLE "my_table")

   cmdsep: Command separator (default=';') used in DDL file (Ex: --cmdsep='\g' or --cmdsep='go') 


   unload/loadata : Very basic unload/load facilities for data. Absolutly unefficient for massive
                    data but can be useful for very small tables (< 1000 records)
                    
   fdelim : Field delimiter used for data unloads.



Supported Python drivers are:

   pyodbc                         # Module for ODBC

   or:
```
   ingresdbi                      # Module for Ingres transactional database
   cx_Oracle                      # Module for Oracle
   Sybase                         # Module for Sybase ASE
   pymssql                        # Module for MsSql
   MySQLdb                        # Module for Mysql
   psycopg2, psycopg2.extensions  # Postgres module
   import DB2                     # Module for DB2
```
