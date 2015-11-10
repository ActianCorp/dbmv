#!/usr/bin/env python
# -*- coding: utf-8 -*

# Copyright 2015 Actian Corporation

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#      http://www.apache.org/licenses/LICENSE-2.0

#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.


import re, sys, os
import xml.dom.minidom

from string import maketrans
from string import Template


try: import ingresdbi                      # Module for Ingres transactional database
except ImportError: pass

try: import cx_Oracle                      # Module for Oracle
except ImportError: pass


try: import Sybase                         # Module for Sybase ASE
except ImportError: pass

try: import pymssql                        # Module for MsSql
except ImportError: pass

try: import MySQLdb                        # Module for Mysql
except ImportError: pass

try: import psycopg2, psycopg2.extensions  # Postgres module
except ImportError: pass

try:                                       # Module for ODBC
   import pyodbc
   pyodbc.pooling = False
except ImportError: pass

try: import DB2                            # Module for DB2
except ImportError: pass


# Default databases used when no database has been specified in connect string
defdbs   = { "mysql":"mysql"  , "oracle":"sys"       , "mssql":"master", 
             "teradata":"dbc" , "postgres":"postgres", "greenplum":"postgres",
             "db2":"dsndd04"  , "ase":"master"       , "progress":"sysprogress",
             "maxdb":"sysinfo", "ingres":"iidbdb"    , "vectowise":"iidbdb",
             "asa":"sys"      , "iq":"sys"           , "hana":"sys",
             "matrix":"dev"
           }

# Default port used when no port has been specified in connect string
defports = { "mysql":"3306"   , "oracle":"1521"      , "mssql":"1433"    , 
             "teradata":"1025", "postgres":"5432"    , "greenplum":"5432",
             "db2":"446"      , "ase":"5000"         , "progress":"8104" ,
             "maxdb":"7200"   , "ingres":"II"        , "vectorwise":"VW" ,
             "asa":"2638"     , "iq":"2638"          , "hana":"00",
             "matrix":"1439"
           }

# Error table
errors   = { "wrong_db_string": "Wrong format for dbconnect. Given: %s, expected: db='dbtype[-odbc]://hostname[:port][/dbname[?user[&Pass]]]'",
             "unknown_db_type": "This type of database is unknown", "unknown_driver" : "Unknown driver" }


# Environnement variables
g_lib    = os.path.dirname(__file__)
ODBCINI  = ("%s/../etc/%s.odbc") % (g_lib, __name__)
XMLINI   = ("%s/../etc/%s.xml" ) % (g_lib, __name__)
II_DATE_FORMAT='SWEDEN'                     ## INGRESDATE datatype formated as '2006-12-15 12:30:55'


os.environ['ODBCINI']         = ODBCINI  
os.environ['II_DATE_FORMAT']  = II_DATE_FORMAT  



'''
   perror raise NameError and print message
'''
def perror ( p_error, p_value=None ):
   s = errors[p_error]
   if p_value is not None: s = "%s : %s" % (s, p_value)
   raise NameError(s)


# Extract string details
# ---------------------------------------------------------
def getDbStringDetails (p_db):
      
   db      = p_db
   pattern = re.compile(r"^(\w+)(-odbc)?://([a-zA-Z0-9_-]+[\.a-zA-Z0-9_-]*):?([a-zA-Z0-9]*)/?([a-zA-Z0-9_]?[\.a-zA-Z0-9_-]*)\??([\\\.a-zA-Z0-9_-]*)&?([\\!\.a-zA-Z0-9_-]*)$")

   # Check parameter match : <dbtype>[-odbc] '://' <hostname [.FQDN]> ':' <port> '/' <dbname> '?' <user> '&' <pwd>
   if not re.match(pattern, db): perror("wrong_db_string", db)
   

   (dbtype, driver, hostname, port, dbname, user, pwd) = pattern.search(db).groups()
   
   if dbname == '': dbname = defdbs[dbtype]            # Setup a default dbname if parameter has been omitted
   if port   == '': port   = defports[dbtype]          # Setup default port
   if user   == '': user   = 'P02Zs5vTR'
   if pwd    == '': pwd    = 'XFNsldj12xxxt'
   if driver not in [None, '-odbc']: perror("unknown_driver", driver)

   return( (dbtype, driver, hostname, port, dbname, user, pwd) )



''' 
   Get Indexed XML data from XML file. 
'''
def getXMLdata(p_key1, p_key2=None, p_key3=None):

   rc=""

   xmldoc = xml.dom.minidom.parse( XMLINI )

   if (p_key2, p_key3) == (None, None):

      node = xmldoc.getElementsByTagName(p_key1)[0]
      rc   = node.childNodes[0].data

   else:
      for node in xmldoc.getElementsByTagName(p_key1)[0].getElementsByTagName(p_key2): 
         if node.getAttribute("id") == p_key3:
            for child in node.childNodes:
              if child.nodeType == xml.dom.minidom.Node.TEXT_NODE:
                  rc = child.data

   return(rc)



'''
    Parameter db="dbtype://hostname:port/dbname?
    mysql://localhost:3306/HerongDB?user&password

'''
class dbconnector:


   def __init__(self, p_db):

      db          = p_db
      s           = ""
      self.db     = None
      self.cursor = None

     
      (self.dbtype, driver, hostname, port, dbname, user, pwd) = getDbStringDetails(db)
  
      if (self.dbtype in ["teradata", "maxdb"]) or (driver == "-odbc"):
       dsn        = self.odbc(hostname, port, dbname)
       print dsn
       self.db    = pyodbc.connect(dsn=dsn, user=user, password=pwd, ansi=True, autocommit=True)
       self.cursor=self.db.cursor()

      elif self.dbtype == "ase": 
         # hostname defined in interface file
         self.db     = Sybase.connect(dsn=hostname, user=user, passwd=pwd, database=dbname, auto_commit=True)                                 
         self.cursor = self.db.cursor()
         self.cursor.execute("set quoted_identifier on")

      elif self.dbtype in ["asa", "iq"]: 
         import sqlanydb                      # Module for Sybase ASA or IQ
         s           = "%s" % (hostname)
         print s
         self.db     = sqlanydb.connect(eng=s, userid=user, password=pwd, dbn=dbname)                   
         self.cursor = self.db.cursor()
         
      elif self.dbtype == "mssql":
         s           = "%s:%s" % (hostname, port)
         self.db     = pymssql.connect(host=s, user=user, password=pwd, database=dbname, as_dict=True)
         self.cursor = self.db.cursor()

      elif self.dbtype == "mysql":
         self.db     = MySQLdb.connect (host=hostname, port=int(port), user = user, passwd = pwd, db=dbname)
         self.cursor = self.db.cursor()

      elif self.dbtype == "db2":
         self.db     = DB2.connect (dsn=dbname, uid=user, pwd=pwd)
         self.cursor = self.db.cursor()

      elif self.dbtype in ["postgres", "greenplum"]:
         s           = "host='%s' port='%s' user='%s' password='%s' dbname='%s'" % (hostname, port, user, pwd, dbname)
         self.db     = psycopg2.connect (s)
         self.db.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
         self.cursor = self.db.cursor()

      elif self.dbtype == "oracle":
         s = "%s/%s@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%s))(CONNECT_DATA=(SERVICE_NAME=%s)))"
         s = s % (user, pwd, hostname, port, dbname) 
         self.db     = cx_Oracle.connect(s)
         self.cursor = self.db.cursor()

      elif self.dbtype == "netezza":
         # conn="DRIVER={MySQL ODBC 3.51 Driver}; SERVER=localhost; PORT=3306; DATABASE=mysql; UID=joe; 
         # PASSWORD=bloggs; OPTION=3;SOCKET=/var/run/mysqld/mysqld.sock;"   
         self.cursor = Connect (hostname, user, pwd)

      elif self.dbtype in ["hana"]:
         from hdbcli import dbapi
         self.db                       = dbapi.connect(address=hostname, port=30015+int(port), user=user, password=pwd, autocommit=True)
         self.cursor                   = self.db.cursor()

      elif self.dbtype in ["progress"]:
         dsn        = self.odbc(hostname, port, dbname) 
         self.db    = pyodbc.connect(dsn=dsn, user=user, password=pwd, autocommit=True)
         self.cursor=self.db.cursor()

      elif self.dbtype in ["ingres", "vectorwise"]:
         # vnode = @host,protocol,port[;attribute=value{;attribute=value}][[user,password]]
         s = "@%s,tcp_ip,%s;connection_type=direct" % (hostname, port)
         self.db=ingresdbi.connect(database=dbname, vnode=s, uid=user, pwd=pwd, dbms_pwd=pwd, autocommit = "Y")   # trace = (7, "dbi.log")
         self.cursor=self.db.cursor()

      else:
         perror("Unknown_db_type", self.dbtype)


  

   # Execute SQL statement
   # ---------------------------------------------------------
   def execute(self, p_sql):

      rows     = None
      pattern  = re.compile("\w", re.IGNORECASE)
      isNotnull= True if pattern.search(p_sql) else False

      if isNotnull:
         pattern  = re.compile("^ *\n* *select ", re.IGNORECASE)   # Query contains newline
                                                                   # at the beginning
         isSelect = True if pattern.search(p_sql) else False

         self.cursor.execute(p_sql.encode('utf-8'))

         if isSelect and self.dbtype in ["db2", "netezza", "teradata", "ingres", "vectorwise", "asa", "iq", "hana"]:
            rows = self.cursor.fetchall()
         else:
            rows = self.cursor

      return(rows)



   def commit(self):

      if   self.dbtype in ["db2"]: 
         self.cursor.execute("commit")

      elif self.dbtype in ["asa", "iq"]: 
         self.db.commit()


   def close(self):

      if   self.dbtype in ["netezza", "teradata", "maxdb", "progress"]: 
         pass

      else : 
         self.db.close()



   '''
       Create an odbc datasource and return dsn
   '''
   def odbc(self, p_hostname, p_port, p_dbname):
   
      if os.name == "nt": 
         dsn = p_hostname
     
      else:
         dsn = "%s_%s" % (p_hostname.split('.')[0], p_dbname)             # Create a DSN identifier
                                                                       # [hostname_dbname_port]

         odbcinfo = getXMLdata(p_key1=self.dbtype)                        # Get odbc information from Xml
         odbcinfo = odbcinfo.replace(' ','')                              # and trim whitespaces and tabs
         odbcinfo = odbcinfo.replace('\t','')   
                                                               # Create file odbc.ini 
         f   = open(ODBCINI, 'w')                                                            
         f.write( "[%s]" %(dsn) )                                                                      
         s = Template( odbcinfo )   
         f.write( s.substitute(hostname=p_hostname, port=p_port, dbname=p_dbname)+"\n" )

         f.close()
       
      return(dsn)






