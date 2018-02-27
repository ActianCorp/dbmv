  #!/usr/bin/env python
# -*- coding: utf-8 -*-
# For UTF-8 see: http://www.python.org/dev/peps/pep-0263/ 
# PYTHONIOENCODING=utf_8

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


# Add:V4
## Add test on not easily convertable datatypes (MSSQL : sql_variant, xml, byte(s), image, ...)

import sys, os, traceback
import getopt, re                 # Import basic modules
import warnings
import codecs
import time

import xml.dom.minidom
from   xml.dom.minidom import Node
from   string          import Template
from   copy            import deepcopy
from   driverTools     import dbconnector

reload(sys)
sys.setdefaultencoding('utf-8')
                                                      # Program parameters
g_pars   = [ 'src='   , 'dest=' , 'loadata' , 'cretab', 'creview', 'creall', 'loaddl', 'loadtest', 'batchsize=', 
             'truncate', 'parfile=', 'fdelim=', 'unload', 'translation=', 'quote=', 'cmdsep=', 'charmax=',
			 'creindex', 'ownsrc=', 'owntgt=', 
			 'add_drop' ] 


g_trnm   = {}                                         # Global translation table for names
g_trty   = {}                                         # Global translation table for datatypes
g_quote  = None
g_cmdsep = ''


g_bin    = os.path.dirname(__file__)
g_bin    = "." if g_bin == "" else g_bin

g_prg    = os.path.basename(__file__).split('.')[0]   # The short name of the script (__file__= sys.argv[0])

XMLINI   = "%s/../etc/%s.xml" % (g_bin, g_prg) 


## Remaining Pg datatypes:
## REM:  The postgres datatype with default precision to NULL exceeds the maximum Vector's precision.
## REM: hence, the datatype is converted to a DOUBLE PRECISION which might not fit expectations. 
## bytea 	  	binary data ("byte array")
## cidr 	  	IPv4 or IPv6 network address
## circle 	  	circle on a plane
## inet 	  	IPv4 or IPv6 host address
## line 	  	infinite line on a plane
## lseg 	  	line segment on a plane
## macaddr 	  	MAC (Media Access Control) address
## path 	  	geometric path on a plane
## point 	  	geometric point on a plane
## polygon 	  	closed geometric path on a plane
## tsquery 	  	text search query
## tsvector 	  	text search document
## txid_snapshot 	  	user-level transaction ID snapshot
## uuid 	  	universally unique identifier

pg2vw = {
         "INT"              : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer (-2^31; 2^31 - 1 (2,147,483,647))
         "INT4"             : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer (-2^31; 2^31 - 1 (2,147,483,647))
         "INTEGER"          : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer (-2^31; 2^31 - 1 (2,147,483,647))
         "SERIAL"           : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # 4 Bytes autoincrement N/A in VW
         "SERIAL4"          : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # 4 Bytes autoincrement N/A in VW
         "NUMERIC"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # 
         "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ),
         "MONEY"            : ("DECIMAL(19,4)"               , "<COLNAME>", "<VALUE>"  ), # NUMERIC(19,4) ?? To check
         "SMALLINT"         : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # 2 Bytes [-32768:32767]-
         "INT2"             : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # 2 Bytes [-32768:32767]-
         "BIGINT"           : ("BIGINT"                      , "<COLNAME>", "<VALUE>"  ), # 8 Bytes : -2^63 ;+2^63-1
         "INT8"             : ("BIGINT"                      , "<COLNAME>", "<VALUE>"  ), # 8 Bytes : -2^63 ;+2^63-1
         "BIGSERIAL"        : ("BIGINT"                      , "<COLNAME>", "<VALUE>"  ), # 8 Bytes autoincrement N/A in VW
         "SERIAL8"          : ("BIGINT"                      , "<COLNAME>", "<VALUE>"  ), # 8 Bytes autoincrement N/A in VW
         "DOUBLE PRECISION" : ("FLOAT8"                      , "<COLNAME>", "<VALUE>"  ), # 8 Bytes 
         "FLOAT8"           : ("FLOAT8"                      , "<COLNAME>", "<VALUE>"  ), # 8 Bytes 
         "FLOAT4"           : ("FLOAT4"                      , "<COLNAME>", "'<VALUE>'"), # 4 bytes                     
         "REAL"             : ("FLOAT4"                      , "<COLNAME>", "'<VALUE>'"), # 4 bytes
         "TIMESTAMP"        : ("TIMESTAMP"                   , "<COLNAME>", "'<VALUE>'"), # OR: [0-9] (def: 6) => Add precision VW [max 9]
         "TIMESTAMP WITHOUT TIME ZONE" : ("TIMESTAMP"        , "<COLNAME>", "'<VALUE>'"), # 
         "TIMESTAMP WITH TIME ZONE"    : ("TIMESTAMP WITH TIME ZONE", "<COLNAME>", "'<VALUE>'"), # 
         "TIMESTAMPTZ"      : ("TIMESTAMP WITH TIME ZONE"    , "<COLNAME>", "'<VALUE>'"), # 
         "DATE"             : ("ANSIDATE"                    , "<COLNAME>", "'<VALUE>'"), # 1999-01-08
         "TIME%"            : ("TIME(<SCALE>)"          , "<COLNAME>", "'<VALUE>'"), # OR: [0-9] (def: 6) => Add precision VW [max 6]
         "TIME % WITH TIME ZONE" : ("TIME(<SCALE>) WITH TIME ZONE", "<COLNAME>", "'<VALUE>'"), #
         "TIMETZ%"          : ("TIME(<SCALE>) WITH TIME ZONE", "<COLNAME>", "'<VALUE>'"), #
         "INTERVAL"         : ("INTERVAL"                    , "<COLNAME>", "'<VALUE>'"), # To check
         "CHAR"             : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"), #
         "CHARACTER"        : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"), #
         "VARCHAR"          : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"), #
         "CHARACTER VARYING": ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"), #
         "TEXT"             : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"), # 
         "BYTEA"            : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"), # N/A in VW / To do something, we convert to VARCHAR
         "ARRAY"            : ("ARRAY(<PRECISION>)"          , "<COLNAME>", "'<VALUE>'"), # N/A in VW / Need to converted to an equivalent table
         "BIT"              : ("BIT(<PRECISION>)"            , "<COLNAME>", "<VALUE>"  ), # N/A IN VW 
         "BIT VARYING"      : ("BIT VARYING(<PRECISION>)"    , "<COLNAME>", "<VALUE>"  ), # N/A IN VW 
         "BOOLEAN"          : ("BOOLEAN"                     , "<COLNAME>", "<VALUE>"  ), # N/A IN VW 
         "BOOL"             : ("BOOLEAN"                     , "<COLNAME>", "<VALUE>"  ), # N/A IN VW 
         "VARBINARY"        : ("VARBINARY(<PRECISION>)"      , "<COLNAME>", "<VALUE>"  ), # N/A IN VW 
         "IMAGE"            : ("BLOB"                        , "<COLNAME>", "<VALUE>"  ), # N/A IN VW 
         "BIT"              : ("BOOLEAN"                     , "<COLNAME>", "<VALUE>"  ), # 
         "XML"              : ("CLOB"                        , "CAST(<COLNAME> AS VARCHAR(8000))", "'<VALUE>'")
}



'''
  Global translation table for datatype conversion from MS*SQL to SYBASE ASE
  Sybase default values for NUMERIC and DECIMAL : 
      In Adaptive Server IQ, the default precision is 126 and the default scale is 38.
      In Adaptive Server Enterprise, the default precision is 18 and the default scale is 0.
      In Adaptive Server Anywhere, the default precision is 30 and the default scale is 6.
      IQ: MONEY, SMALLMONEY = NUMERIC(19,4), NUMERIC(10,4).
      IQ: FLOAT(p) is a synonym for REAL ou DOUBLE depending of precision
      IQ: REAL is a sigle precision floting point number stored in 4 bytes
      IQ: SMALLDATETIME = DATETIME => 8 Bytes
  Global translation table for datatype conversion from MS*SQL to INGRES VW
  VW : not compressed datatypes :  DECIMAL with precision > 18, float, float4
       Unsupported datatypes : *BYTES, INGRESDATE, BOOLEAN, UUID, TABLE_KEY, OBJECT_KEY, * WITH TIMEZONE
  
'''
iq2vw = {
         "INT"              : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer (-2^31; 2^31 - 1 (2,147,483,647))
         "INTEGER"          : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer (-2^31; 2^31 - 1 (2,147,483,647))
         "NUMERIC"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # 
         "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ),
         "MONEY"            : ("DECIMAL(19,4)"               , "<COLNAME>", "<VALUE>"  ), # IQ : NUMERIC(19,4)
         "SMALLMONEY"       : ("DECIMAL(10,4)"               , "<COLNAME>", "<VALUE>"  ), # IQ : NUMERIC(10,4)
         "TINYINT"          : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # IQ: [O:255]  VW(TINYINT): -128:+127
         "SMALLINT"         : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # 2 Bytes [-32768:32767]-
         "BIGINT"           : ("BIGINT"                      , "<COLNAME>", "<VALUE>"  ), # 8 Bytes : -2^63 ;+2^63-1
         "DOUBLE"           : ("FLOAT8"                      , "<COLNAME>", "<VALUE>"  ), # 8 Bytes 
         "FLOAT"            : ("FLOAT8"                      , "<COLNAME>", "'<VALUE>'"), # IQ: 8 Bytes/4 Bytes                      
         "REAL"             : ("FLOAT4"                      , "<COLNAME>", "'<VALUE>'"), # IQ: 4 bytes
         "TIMESTAMP"        : ("TIMESTAMP"                   , "<COLNAME>", "'<VALUE>'"), # IQ: 1999-01-08 04:05:06.xxxxxx
         "DATETIME"         : ("TIMESTAMP"                   , "<COLNAME>", "'<VALUE>'"), # IQ: 1999-01-08 04:05:06.xxxxxx 
         "SMALLDATETIME"    : ("TIMESTAMP"                   , "<COLNAME>", "'<VALUE>'"), # IQ: 1999-01-08 04:05:06.xxxxxx 
         "DATE"             : ("ANSIDATE"                    , "<COLNAME>", "'<VALUE>'"), # IQ: 1999-01-08
         "TIME"             : ("TIME WITHOUT TIMEZONE"       , "<COLNAME>", "'<VALUE>'"), # RIQ 04:05:00.xxxxxx
         "CHAR"             : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"), #
         "CHARACTER"        : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"), #
         "VARCHAR"          : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"), #
         "CHARACTER VARYING": ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"), #
         "TEXT"             : ("CLOB"                        , "<COLNAME>", "'<VALUE>'"), # N/A IN VW 
         "NTEXT"            : ("CLOB"                        , "<COLNAME>", "'<VALUE>'"), # N/A IN VW
         "UNIQUEIDENTIFIER" : ("UNIQUEIDENTIFIER"            , "<COLNAME>", "'<VALUE>'"), # N/A IN VW
         "BINARY"           : ("BINARY(<PRECISION>)"         , "<COLNAME>", "<VALUE>"  ), # N/A IN VW 
         "VARBINARY"        : ("VARBINARY(<PRECISION>)"      , "<COLNAME>", "<VALUE>"  ), # N/A IN VW 
         "IMAGE"            : ("BLOB"                        , "<COLNAME>", "<VALUE>"  ), # N/A IN VW 
         "BIT"              : ("TINYINT"                     , "<COLNAME>", "<VALUE>"  ), # IQ:0/1/NULL
         "XML"              : ("CLOB"                        , "CAST(<COLNAME> AS VARCHAR(8000))", "'<VALUE>'")
}



'''
  Global translation table for datatype conversion from MS*SQL to MYSQL
'''
ms2my = {
   "INT"              : ("INT"                                    , "<COLNAME>", "<VALUE>"  ), 
   "NUMERIC"          : ("NUMERIC(<PRECISION>,<SCALE>)"           , "<COLNAME>", "<VALUE>"  ),
   "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)"           , "<COLNAME>", "<VALUE>"  ),
   "DATETIME"         : ("TIMESTAMP"                              , "CONVERT(VARCHAR,<COLNAME>,121)", "'<VALUE>'"),
   "NVARCHAR"         : ("VARCHAR(<PRECISION>) CHARACTER SET UTF8", "CAST(<COLNAME> AS TEXT)", "'<VALUE>'"), # Should be NTEXT but pb driver MSSQL
   "VARCHAR"          : ("VARCHAR(<PRECISION>)"                   , "<COLNAME>", "'<VALUE>'"),
   "UNIQUEIDENTIFIER" : ("VARCHAR(64)"                            , "<COLNAME>", "'<VALUE>'"),
   "NCHAR"            : ("NCHAR(<PRECISION>) CHARACTER SET UTF8"  , "CAST(<COLNAME> AS TEXT)", "'<VALUE>'"),
   "CHAR"             : ("CHAR(<PRECISION>)"                      , "<COLNAME>", "'<VALUE>'"),
   "BINARY"           : ("BINARY(<PRECISION>)"                    , "<COLNAME>", "<VALUE>"  ),
   "VARBINARY"        : ("VARBINARY(<PRECISION>)"                 , "<COLNAME>", "<VALUE>"  ),
   "SMALLDATETIME"    : ("DATETIME"                               , "CONVERT(VARCHAR,<COLNAME>,120)", "'<VALUE>'"),
   "BIT"              : ("BIT(1)"                                 , "<COLNAME>", "<VALUE>"  ),
   "MONEY"            : ("DOUBLE"                                 , "<COLNAME>", "<VALUE>"  ),
   "SMALLMONEY"       : ("FLOAT"                                  , "<COLNAME>", "<VALUE>"  ),
   "TINYINT"          : ("TINYINT"                                , "<COLNAME>", "<VALUE>"  ), # [O:255]       - My: [0:255] or [-128:127]
   "SMALLINT"         : ("SMALLINT"                               , "<COLNAME>", "<VALUE>"  ), # [-32768:32767]-
   "FLOAT"            : ("FLOAT(52,10)"                           , "<COLNAME>", "'<VALUE>'"), # 8 bytes       - 8  (precision suppressed here)
   "XML"              : ("TEXT"                                   , "CAST(<COLNAME> AS VARCHAR(8000))", "'<VALUE>'")
}


'''
  Global translation table for datatype conversion from MS*SQL to SYBASE ASE
'''
ms2ase = {
   "INT"              : ("INT"                                    , "<COLNAME>", "<VALUE>"  ), 
   "NUMERIC"          : ("NUMERIC(<PRECISION>,<SCALE>)"           , "<COLNAME>", "<VALUE>"  ),
   "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)"           , "<COLNAME>", "<VALUE>"  ), 
   "DATETIME"         : ("DATETIME"                               , "<COLNAME>", "'<VALUE>'"),  # In Sybase "timestamp" is only lowercase
   "TIMESTAMP"        : ("timestamp"                              , "<COLNAME>", "'<VALUE>'"),
   "NVARCHAR"         : ("NVARCHAR(<PRECISION>)"                  , "<COLNAME>", "'<VALUE>'"),
   "VARCHAR"          : ("VARCHAR(<PRECISION>)"                   , "<COLNAME>", "'<VALUE>'"),
   "UNIQUEIDENTIFIER" : ("VARCHAR(64)"                            , "<COLNAME>", "'<VALUE>'"),
   "NCHAR"            : ("NCHAR(<PRECISION>)"                     , "<COLNAME>", "'<VALUE>'"),
   "CHAR"             : ("CHAR(<PRECISION>)"                      , "<COLNAME>", "'<VALUE>'"),
   "BINARY"           : ("BINARY(<PRECISION>)"                    , "<COLNAME>", "<VALUE>"  ),
   "VARBINARY"        : ("VARBINARY(<PRECISION>)"                 , "<COLNAME>", "<VALUE>"  ),
   "SMALLDATETIME"    : ("SMALLDATETIME"                          , "<COLNAME>", "'<VALUE>'"),
   "BIT"              : ("BIT"                                    , "<COLNAME>", "<VALUE>"  ),
   "MONEY"            : ("MONEY"                                  , "<COLNAME>", "<VALUE>"  ),
   "SMALLMONEY"       : ("SMALLMONEY"                             , "<COLNAME>", "<VALUE>"  ),
   "TINYINT"          : ("TINYINT"                                , "<COLNAME>", "<VALUE>"  ), # [O:255]       - My: [0:255] or [-128:127]
   "SMALLINT"         : ("SMALLINT"                               , "<COLNAME>", "<VALUE>"  ), # [-32768:32767]-
   "FLOAT"            : ("FLOAT"                                  , "<COLNAME>", "<VALUE>"), # 8 bytes       - 8  (precision suppressed here)
   "XML"              : ("TEXT"                                   , "CAST(<COLNAME> AS VARCHAR(8000))", "'<VALUE>'")
}



'''
  Global translation table for datatype conversion from MS*SQL to SYBASE ASE
  Sybase default values for NUMERIC and DECIMAL : 
      In Adaptive Server IQ, the default precision is 126 and the default scale is 38.
      In Adaptive Server Enterprise, the default precision is 18 and the default scale is 0.
      In Adaptive Server Anywhere, the default precision is 30 and the default scale is 6.
      IQ: MONEY, SMALLMONEY = NUMERIC(19,4), NUMERIC(10,4).
      IQ: SMALLDATETIME = DATETIME => 8 Bytes
'''
ms2iq = {
         "INT"              : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer (-2^31; 2^31 - 1 (2,147,483,647))
         "NUMERIC"          : ("NUMERIC(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # MSSQL: -10^38 +1 through 10^38 –1. 
         "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ),
         "MONEY"            : ("NUMERIC(19,4)"               , "<COLNAME>", "<VALUE>"  ), # MSSQL : NUMERIC(19,4)
         "SMALLMONEY"       : ("NUMERIC(10,4)"               , "<COLNAME>", "<VALUE>"  ), # MSSQL : NUMERIC(10,4)
         "TINYINT"          : ("TINYINT"                     , "<COLNAME>", "<VALUE>"  ), # SQL: [O:255]
         "SMALLINT"         : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # 2 Bytes [-32768:32767]-
         "BIGINT"           : ("BIGINT"                      , "<COLNAME>", "<VALUE>"  ), # 8 Bytes : -2^63 ;+2^63-1
         "FLOAT"            : ("FLOAT(<PRECISION>)"          , "<COLNAME>", "'<VALUE>'"), # MS(def) = float(53) precision(4 or 8)
         "REAL"             : ("FLOAT(<PRECISION>)"          , "<COLNAME>", "'<VALUE>'"), # MS(def) = FLOAT(24)
         "ROWVERSION"       : ("VARBINARY(8)"                , "<COLNAME>", "'<VALUE>'"), # MS : An incremental number for versionning
         "TIMESTAMP"        : ("VARBINARY(8)"                , "<COLNAME>", "'<VALUE>'"), # MS = ROWVERSION
         "DATETIME"         : ("DATETIME"                    , "CONVERT(VARCHAR,<COLNAME>,121)", "'<VALUE>'"), # MS: 1999-01-08 04:05:06.xxxx 
         "SMALLDATETIME"    : ("SMALLDATETIME"               , "CONVERT(VARCHAR,<COLNAME>,120)", "'<VALUE>'"), # MS: 1999-01-08 04:05:00
         "NCHAR"            : ("CHAR(<PRECISION>)"           , "CAST(<COLNAME> AS TEXT)", "'<VALUE>'"),
         "CHAR"             : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"),
         "NVARCHAR"         : ("VARCHAR(<PRECISION>)"        , "CAST(<COLNAME> AS TEXT)", "'<VALUE>'"),          # Should be NTEXT but pb driver MSSQL
         "VARCHAR"          : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),
         "TEXT"             : ("CLOB"                        , "<COLNAME>", "'<VALUE>'"), # MS: 2^31 Chars
         "NTEXT"            : ("CLOB"                        , "<COLNAME>", "'<VALUE>'"), # MS: 2^31/2 UTF8 Chars
         "UNIQUEIDENTIFIER" : ("UNIQUEIDENTIFIER"            , "<COLNAME>", "'<VALUE>'"),
         "BINARY"           : ("BINARY(<PRECISION>)"         , "<COLNAME>", "<VALUE>"  ), # MS: [1:8000] (Fixed size)
         "VARBINARY"        : ("VARBINARY(<PRECISION>)"      , "<COLNAME>", "<VALUE>"  ), # MS: [1:8000] (Var size)
         "IMAGE"            : ("BLOB"                        , "<COLNAME>", "<VALUE>"  ), # MS: 2^31 bytes
         "BIT"              : ("BIT"                         , "<COLNAME>", "<VALUE>"  ), # SQL:0/1; Ingres: -128:+127
         "XML"              : ("CLOB"                        , "CAST(<COLNAME> AS VARCHAR(8000))", "'<VALUE>'")
}


'''
  Global translation table for datatype conversion from MS*SQL to POSTGRES
'''
ms2pg = {
         "INT"              : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer
         "NUMERIC"          : ("NUMERIC(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ),
         "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ),      
         "DATETIME"         : ("TIMESTAMP"                   , "CONVERT(VARCHAR,<COLNAME>,121)", "'<VALUE>'"),   # MS: 1999-01-08 04:05:06.xxxx
         "NVARCHAR"         : ("VARCHAR(<PRECISION>)"        , "CAST(<COLNAME> AS TEXT)", "'<VALUE>'"),          # Should be NTEXT but pb driver MSSQL
         "VARCHAR"          : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),
         "UNIQUEIDENTIFIER" : ("VARCHAR(64)"                 , "<COLNAME>", "'<VALUE>'"),
         "NCHAR"            : ("CHAR(<PRECISION>)"           , "CAST(<COLNAME> AS TEXT)", "'<VALUE>'"),
         "CHAR"             : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"),
         "BINARY"           : ("BITEA"                       , "<COLNAME>", "<VALUE>"  ),
         "VARBINARY"        : ("BITEA"                       , "<COLNAME>", "<VALUE>"  ),
         "SMALLDATETIME"    : ("TIMESTAMP"                   , "CONVERT(VARCHAR,<COLNAME>,120)", "'<VALUE>'"),
         "BIT"              : ("BIT"                         , "<COLNAME>", "<VALUE>"  ),
         "MONEY"            : ("DOUBLE PRECISION"            , "<COLNAME>", "<VALUE>"  ), # MSSQL : NUMERIC(19,4)
         "SMALLMONEY"       : ("MONEY"                       , "<COLNAME>", "<VALUE>"  ), # MSSQL : NUMERIC(10,4)
         "TINYINT"          : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # [O:255]
         "SMALLINT"         : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # 2 Bytes [-32768:32767]-
         "FLOAT"            : ("DOUBLE PRECISION"            , "<COLNAME>", "'<VALUE>'"), # 8 bytes       - 8  (precision suppressed here)
         "XML"              : ("TEXT"                        , "CAST(<COLNAME> AS VARCHAR(8000))", "'<VALUE>'")
        }



'''
  Global translation table for datatype conversion from MS*SQL to MATRIX
'''
ms2mx = {
         "INT"              : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer
         "NUMERIC"          : ("NUMERIC(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ),
         "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ),      
         "DATETIME"         : ("TIMESTAMP"                   , "CONVERT(VARCHAR,<COLNAME>,121)", "'<VALUE>'"),   # MS: 1999-01-08 04:05:06.xxxx
         "NVARCHAR"         : ("VARCHAR(<PRECISION>)"        , "CAST(<COLNAME> AS TEXT)", "'<VALUE>'"),          # Should be NTEXT but pb driver MSSQL
         "VARCHAR"          : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),
         "UNIQUEIDENTIFIER" : ("VARCHAR(64)"                 , "<COLNAME>", "'<VALUE>'"),
         "NCHAR"            : ("CHAR(<PRECISION>)"           , "CAST(<COLNAME> AS TEXT)", "'<VALUE>'"),
         "CHAR"             : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"),
         "BINARY"           : ("BITEA"                       , "<COLNAME>", "<VALUE>"  ),
         "VARBINARY"        : ("BITEA"                       , "<COLNAME>", "<VALUE>"  ),
         "SMALLDATETIME"    : ("TIMESTAMP"                   , "CONVERT(VARCHAR,<COLNAME>,120)", "'<VALUE>'"),
         "BIT"              : ("BIT"                         , "<COLNAME>", "<VALUE>"  ),
         "MONEY"            : ("NUMERIC(19,4)"               , "<COLNAME>", "<VALUE>"  ), # MSSQL : NUMERIC(19,4)
         "SMALLMONEY"       : ("NUMERIC(10,4)"               , "<COLNAME>", "<VALUE>"  ), # MSSQL : NUMERIC(10,4)
         "TINYINT"          : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # [O:255]
         "SMALLINT"         : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # 2 Bytes [-32768:32767]-
         "FLOAT"            : ("DOUBLE PRECISION"            , "<COLNAME>", "'<VALUE>'"), # 8 bytes       - 8  (precision suppressed here)
         "XML"              : ("TEXT"                        , "CAST(<COLNAME> AS VARCHAR(8000))", "'<VALUE>'")
        }


'''
  Global translation table for datatype conversion from MS*SQL to INGRES DB
  ii : Text = VARCHAR(32000) 16000 if UTF8

'''
ms2ii = {
         "INT"              : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer (-2^31; 2^31 - 1 (2,147,483,647))
'''
	added the full INTEGER value
'''
         "INTEGER"          : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer (-2^31; 2^31 - 1 (2,147,483,647))
         "NUMERIC"          : ("NUMERIC(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # MSSQL: -10^38 +1 through 10^38 –1. 
         "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ),
         "BIT"              : ("BOOLEAN"                     , "<COLNAME>", "<VALUE>"  ), # SQL:0/1; 
         "TINYINT"          : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # SQL: [O:255] ; Ingres: -128:+127
         "SMALLINT"         : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # 2 Bytes [-32768:32767]-
         "BIGINT"           : ("BIGINT"                      , "<COLNAME>", "<VALUE>"  ), # 8 Bytes : -2^63 ;+2^63-1
         "FLOAT"            : ("FLOAT(<PRECISION>)"          , "<COLNAME>", "'<VALUE>'"), # MSSQL: Storage depends of precision(4 or 8). We take 8
         "REAL"             : ("FLOAT(<PRECISION>)"          , "<COLNAME>", "'<VALUE>'"), # MSSQL: = FLOAT
         "UNIQUEIDENTIFIER" : ("UUID"                        , "<COLNAME>", "'<VALUE>'"), # DEFAULT is automatic as UUID_CREATE()
         "ROWVERSION"       : ("BIGINT"                      , "<COLNAME>", "'<VALUE>'"), # MS : An incremental number for versionning
         "MONEY"            : ("NUMERIC(19,4)"               , "<COLNAME>", "<VALUE>"  ), # MSSQL : NUMERIC(19,4) 8 bytes
         "SMALLMONEY"       : ("NUMERIC(10,4)"               , "<COLNAME>", "<VALUE>"  ), # MSSQL : NUMERIC(10,4) 4 bytes
         "TIMESTAMP"        : ("BIGINT"                      , "<COLNAME>", "'<VALUE>'"), # MS = ROWVERSION
         "DATETIME"         : ("TIMESTAMP"                   , "CONVERT(VARCHAR,<COLNAME>,121)", "'<VALUE>'"),   # SQL: 1999-01-08 04:05:06; ii: sweden
         "SMALLDATETIME"    : ("TIMESTAMP"                   , "CONVERT(VARCHAR,<COLNAME>,120)", "'<VALUE>'"),   # depricate the INGRESDATE 
         "NCHAR"            : ("NCHAR(<PRECISION>)"          , "CAST(<COLNAME> AS TEXT)", "'<VALUE>'"),
         "CHAR"             : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"),
         "NVARCHAR"         : ("NVARCHAR(<PRECISION>)"       , "CAST(<COLNAME> AS TEXT)", "'<VALUE>'"),          # Should be NTEXT but pb driver MSSQL
         "VARCHAR"          : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),
         "TEXT"             : ("LONG VARCHAR"                , "<COLNAME>", "'<VALUE>'"), # 2^31 Chars
         "NTEXT"            : ("LONG NVARCHAR"               , "<COLNAME>", "'<VALUE>'"), # 2^31/2 UTF8 Chars
         "BINARY"           : ("BINARY(<PRECISION>)"         , "<COLNAME>", "<VALUE>"  ), # SQL: 8000 Max (Ingres: max configured row size)
         "VARBINARY"        : ("VARBINARY(<PRECISION>)"      , "<COLNAME>", "<VALUE>"  ), # SQL: 8000 Max (Ingres: max configured row size)
         "IMAGE"            : ("LONG BYTE"                   , "<COLNAME>", "<VALUE>"  ), # SQL: 2^31 Max (Ingres: max configured row size)
         "XML"              : ("LONG NVARCHAR"               , "CAST(<COLNAME> AS NVARCHAR(max))", "'<VALUE>'") # MSSQL : 2 GB 
        }


'''
  Global translation table for datatype conversion from MS*SQL to INGRES VW
  VW : not compressed datatypes :  DECIMAL with precision > 18, float, float4
       Keith Bolam - these are old definitions exclusions
       Unsupported datatypes : *BYTES, INGRESDATE, BOOLEAN, UUID, TABLE_KEY, OBJECT_KEY, * WITH TIMEZONE
	  Updated list
       Unsupported datatypes : *BYTES, INGRESDATE, TABLE_KEY, OBJECT_KEY
	   
	   The PLACEHOLDER was added due to a bug with the parsing of the arrary - perhaps introduced by the comment?? 
  
'''
ms2vw = {
         "INT"              : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer (-2^31; 2^31 - 1 (2,147,483,647))
         "INTEGER"          : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer (-2^31; 2^31 - 1 (2,147,483,647))
'''
	added the INTEGER
'''
         "PLACEHOLDER"      : ("PLACEHOLDER"               , "<COLNAME>", "<VALUE>"  ), # MSSQL : NUMERIC(10,4)
         "NUMERIC"          : ("NUMERIC(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # MSSQL: -10^38 +1 through 10^38 –1. 
         "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ),
         "SMALLMONEY"       : ("DECIMAL(10,4)"               , "<COLNAME>", "<VALUE>"  ), # MSSQL : NUMERIC(10,4)
         "MONEY"            : ("DECIMAL(19,4)"               , "<COLNAME>", "<VALUE>"  ), # MSSQL : NUMERIC(19,4)
         "TINYINT"          : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # SQL: [O:255] - VW(TINYINT): -128:+127
         "SMALLINT"         : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # 2 Bytes [-32768:32767]-
         "BIGINT"           : ("BIGINT"                      , "<COLNAME>", "<VALUE>"  ), # 8 Bytes : -2^63 ;+2^63-1
         "FLOAT"            : ("FLOAT(<PRECISION>)"          , "<COLNAME>", "'<VALUE>'"), # MS(def) = float(53) precision(4 or 8)
         "REAL"             : ("FLOAT(<PRECISION>)"          , "<COLNAME>", "'<VALUE>'"), # MS(def) = FLOAT(24)
         "ROWVERSION"       : ("BIGINT"                      , "<COLNAME>", "'<VALUE>'"), # MS : An incremental number for versionning
         "TIMESTAMP"        : ("BIGINT"                      , "<COLNAME>", "'<VALUE>'"), # MS = ROWVERSION
         "DATETIME"         : ("TIMESTAMP"                   , "CONVERT(VARCHAR,<COLNAME>,121)", "'<VALUE>'"), # MS: 1999-01-08 04:05:06.xxxx 
         "SMALLDATETIME"    : ("TIMESTAMP(0)"                , "CONVERT(VARCHAR,<COLNAME>,120)", "'<VALUE>'"), # MS: 1999-01-08 04:05:00
         "TIME"             : ("TIME"                        , "CONVERT(VARCHAR,<COLNAME>,120)", "'<VALUE>'"), # MS: 1999-01-08 04:05:00
         "DATE"             : ("ANSIDATE"                    , "CONVERT(VARCHAR,<COLNAME>,120)", "'<VALUE>'"), # MS: 1999-01-08
         "NCHAR"            : ("NCHAR(<PRECISION>)"          , "<COLNAME>", "'<VALUE>'"),
         "CHAR"             : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"),
         "NVARCHAR"         : ("NVARCHAR(<PRECISION>)"       , "<COLNAME>", "'<VALUE>'"), 
         "VARCHAR"          : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),
         "TEXT"             : ("VARCHAR(4000)"               , "CAST(<COLNAME> AS VARCHAR(4000))", "'<VALUE>'"), # MS: 2^31 Chars
         "NTEXT"            : ("NVARCHAR(4000)"              , "CAST(<COLNAME> AS NVARCHAR(4000))", "'<VALUE>'"), # MS: 2^31/2 UTF8 Chars
         "UNIQUEIDENTIFIER" : ("UUID"                        , "<COLNAME>", "'<VALUE>'"),    # ADed UUID in Vector 5.0
          #the following are not supported in any shape in Vector currently - this can change in Vector 5.1 where the BINARY columns can be put into a hash/heap table and join back the main table
         "BINARY"           : ("BINARY(<PRECISION>)"         , "CAST(<COLNAME> AS NVARCHAR(4000))", "'<VALUE>'"  ), # MS: [1:8000] (Fixed size)
         "VARBINARY"        : ("CHARACTER VARYING(4000)"      , "CASE WHEN <COLNAME> IS NOT NULL THEN '--IMAGE--' END", "'<VALUE>'"  ), # MS: [1:8000] (Var size)
         "IMAGE"            : ("LONG BYTE"                   , "<COLNAME>", "<VALUE>"  ), # MS: 2^31 bytes
         "BIT"              : ("TINYINT"                     , "CAST(<COLNAME> AS TINYINT)", "<VALUE>"  ), # SQL:0/1; 
         "XML"              : ("NVARCHAR(4000)"              , "CAST(<COLNAME> AS NVARCHAR(4000))", "'<VALUE>'"), # MSSQL : 2 GB
         "HIERARCHYID"      : ("NVARCHAR(4000)"             , "CAST(<COLNAME> AS NVARCHAR(4000))", "'<VALUE>'"),
         "GEOMETRY"         : ("NVARCHAR(4000)"             , "CAST(<COLNAME> AS NVARCHAR(4000))", "'<VALUE>'"),
         "GEOGRAPHY"        : ("NVARCHAR(4000)"             , "CAST(<COLNAME> AS NVARCHAR(4000))", "'<VALUE>'"),
         "BINARY"           : ("BINARY(<PRECISION>)"         , "CAST(<COLNAME> AS NVARCHAR(4000))", "'<VALUE>'"  ), # MS: [1:8000] (Fixed size)
         "VARBINARY"        : ("CHARACTER VARYING(4000)"      , "CASE WHEN <COLNAME> IS NOT NULL THEN '--IMAGE--' END", "'<VALUE>'"  ), # MS: [1:8000] (Var size)
#         "BIT"              : ("BOOLEAN"                    , "CAST(<COLNAME> AS TINYINT)", "<VALUE>"  ), # SQL:0/1; 
         "BIT"              : ("BOOLEAN"                    , "<COLNAME>", "'<VALUE>'") # SQL:0/1; 

}

ms2vw_default = {
  "DEFAULT newid"   : "",
  "DEFAULT getdate" : "DEFAULT CURRENT_TIMESTAMP",
  "IDENTITY(1,1)"   : "GENERATED BY DEFAULT AS IDENTITY ",
  "COMMA"           : ""","""
}

ms2vw_view = [
  ["["                 , "\""],
  ["]"                 , "\""],
  ["CONVERT(money"     , "money"],         ## poor mans awk/substitution/translation                 
  ["(money,("          ,  "((money("],     ## money and other items are available as automatic CAST operators
  ["0101"              ,  "-01-01"],       ## some rudimentory massage of the dates - shoudle  
  ["1231"              , "-12-31"],
  ["WITH SCHEMABINDING", ""],
  ["LAST_ITEM_IS_8"]
]

'''
  Global translation table for datatype conversion from MS*SQL to TERADATA
'''
ms2td = {
         "INT"              : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer
         "NUMERIC"          : ("NUMERIC(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # td <PRECISION> [1-18], <SCALE>[0-<PRECISION>]
         "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # td: id NUMERIC    
         "DATETIME"         : ("TIMESTAMP"                   , "CONVERT(VARCHAR,<COLNAME>,120)", "'<VALUE>'"),   # 1999-01-08 04:05:06
         "TIMESTAMP"        : ("TIMESTAMP"                   , "CONVERT(VARCHAR,<COLNAME>,121)", "'<VALUE>'"),   # 1999-01-08 04:05:06
         "NVARCHAR"         : ("VARCHAR(<PRECISION>)"        , "CAST(<COLNAME> AS TEXT)", "'<VALUE>'"),          # Should be NTEXT but pb driver MSSQL
         "VARCHAR"          : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),
         "UNIQUEIDENTIFIER" : ("VARCHAR(64)"                 , "<COLNAME>", "'<VALUE>'"),                 # td: 64000 or 32000 (unicode)
         "NCHAR"            : ("CHAR(<PRECISION>)"           , "CAST(<COLNAME> AS TEXT)", "'<VALUE>'"),
         "CHAR"             : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"),
         "BINARY"           : ("BITEA"                       , "<COLNAME>", "<VALUE>"  ),
         "VARBINARY"        : ("BITEA"                       , "<COLNAME>", "<VALUE>"  ),
         "SMALLDATETIME"    : ("TIMESTAMP"                   , "CONVERT(VARCHAR,<COLNAME>,120)", "'<VALUE>'"),
         "BIT"              : ("BYTEINT"                     , "<COLNAME>", "<VALUE>"  ), #               td: [-128,127]
         "MONEY"            : ("BIGINT"                      , "<COLNAME>", "<VALUE>"  ), # Signed 8 bytes integer
         "SMALLMONEY"       : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer
         "TINYINT"          : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # [O:255] 
         "SMALLINT"         : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # 2 Bytes [-32768:32767]-
         "FLOAT"            : ("FLOAT"                       , "<COLNAME>", "'<VALUE>'"), # 8 bytes
         "SQL_VARIANT"      : ("FLOAT"                       , "<COLNAME>", "'<VALUE>'"), # 8 bytes        
         "XML"              : ("CLOB"                        , "CAST(<COLNAME> AS VARCHAR(8000))", "'<VALUE>'")
        }



'''
  Global translation table for datatype conversion from MS*SQL to PROGRESS (SQL-92)
'''
ms2pr = {
         "INT"              : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer
         "NUMERIC"          : ("NUMERIC(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # (pr) Default prec=32; scale=0
         "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), #    
         "DATETIME"         : ("TIMESTAMP"                   , "CONVERT(VARCHAR,<COLNAME>,121)", "{ts '<VALUE>'}"),   # yyyy-mm-dd hh:mi:ss.mmm
         "NVARCHAR"         : ("VARCHAR(<PRECISION>)"        , "CAST(<COLNAME> AS TEXT)", "'<VALUE>'"),          # Should be NTEXT but pb driver MSSQL
         "VARCHAR"          : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"), # Max : 31995 (pr)
         "UNIQUEIDENTIFIER" : ("VARCHAR(64)"                 , "<COLNAME>", "'<VALUE>'"), # 
         "NCHAR"            : ("CHAR(<PRECISION>)"           , "CAST(<COLNAME> AS TEXT)", "'<VALUE>'"), # Comment : See char datatype
         "CHAR"             : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"), # Char(Max) = 2000 (pr); 8000 mssql=>Overflow may occured
         "BINARY"           : ("VARBINARY(<PRECISION>)"      , "<COLNAME>", "<VALUE>"  ), # binary(Max) = 2000 bytes (pr) ; 8000 mssql
         "VARBINARY"        : ("VARBINARY(<PRECISION>)"      , "<COLNAME>", "<VALUE>"  ), # Max : 31995 bytes (pr); 8000 mssql
         "SMALLDATETIME"    : ("TIMESTAMP"                   , "CONVERT(VARCHAR,<COLNAME>,120)", "{ts '<VALUE>'}"),
         "BIT"              : ("BIT"                         , "<COLNAME>", "<VALUE>"  ), # Single bit 0,1
         "MONEY"            : ("FLOAT"                       , "<COLNAME>", "<VALUE>"  ), # Signed 8 bytes integer
         "SMALLMONEY"       : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer
         "TINYINT"          : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # [O:255] (ms)  - TINYINT [-127:128] (pr)
         "SMALLINT"         : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # 2 Bytes [-32768:32767]-
         "FLOAT"            : ("FLOAT"                       , "<COLNAME>", "'<VALUE>'"), # Signed 8 bytes     
         "XML"              : ("VARCHAR(8000)"               , "CAST(<COLNAME> AS VARCHAR(8000))", "'<VALUE>'")
        }



'''
  Global translation table for datatype conversion from MS*SQL to MAX DB
  DOUBLE PRECISION = FLOAT(38)
'''
ms2md = {
         "INT"              : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer
         "NUMERIC"          : ("NUMERIC(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # td <PRECISION> [1-18], <SCALE>[0-<PRECISION>]
         "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # td: id NUMERIC 
         "BIT"              : ("BOOLEAN"                     , "<COLNAME>", "<VALUE>"  ), # 
         "MONEY"            : ("NUMERIC(19,4)"               , "<COLNAME>", "<VALUE>"  ), # Signed 8 bytes integer
         "SMALLMONEY"       : ("NUMERIC(10,4)"               , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer
         "TINYINT"          : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # [O:255] 
         "SMALLINT"         : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # 2 Bytes [-32768:32767]-
         "FLOAT"            : ("FLOAT(<PRECISION>)"          , "<COLNAME>", "'<VALUE>'"), # 8 bytes     
         "BIGINT"           : ("DOUBLE PRECISION"            , "<COLNAME>", "<VALUE>"  ), # 8 Bytes : -2^63 ;+2^63-1   
         "ROWVERSION"       : ("DOUBLE PRECISION"            , "<COLNAME>", "'<VALUE>'"), # MS : An incremental number for versionning
         "TIMESTAMP"        : ("DOUBLE PRECISION"            , "<COLNAME>", "'<VALUE>'"), # MS = ROWVERSION  
         "DATETIME"         : ("TIMESTAMP"                   , "CONVERT(VARCHAR,<COLNAME>,121)", "'<VALUE>'"),   # 1999-01-08 04:05:06
         "SMALLDATETIME"    : ("TIMESTAMP"                   , "CONVERT(VARCHAR,<COLNAME>,120)", "'<VALUE>'"),
         "NVARCHAR"         : ("NVARCHAR(<PRECISION>)"       , "CAST(<COLNAME> AS TEXT)", "'<VALUE>'"),          # Should be NTEXT but pb driver MSSQL
         "VARCHAR"          : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),
         "UNIQUEIDENTIFIER" : ("VARCHAR(64)"                 , "<COLNAME>", "'<VALUE>'"),                 # td: 64000 or 32000 (unicode)
         "NCHAR"            : ("NCHAR(<PRECISION>)"          , "<COLNAME>", "'<VALUE>'"),
         "CHAR"             : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"),
         "BINARY"           : ("BINARY(<PRECISION>)"         , "<COLNAME>", "<VALUE>"  ),
         "VARBINARY"        : ("VARBINARY(<PRECISION>)"      , "<COLNAME>", "<VALUE>"  ),
         "XML"              : ("CLOB"                        , "CAST(<COLNAME> AS NVARCHAR(max))", "'<VALUE>'")
        }



'''
  Global translation table for datatype conversion from MS*SQL to HANA NEW DB
'''
ms2ha = {
         "INT"              : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer
         "NUMERIC"          : ("NUMERIC(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # td <PRECISION> [1-18], <SCALE>[0-<PRECISION>]
         "BIT"              : ("TINYINT"                     , "<COLNAME>", "<VALUE>"  ), # 
         "MONEY"            : ("NUMERIC(19,4)"               , "<COLNAME>", "<VALUE>"  ), # Signed 8 bytes integer
         "SMALLMONEY"       : ("NUMERIC(10,4)"               , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer
         "TINYINT"          : ("TINYINT"                     , "<COLNAME>", "<VALUE>"  ), # [O:255] 
         "SMALLINT"         : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # 2 Bytes [-32768:32767]-
         "FLOAT"            : ("FLOAT(<PRECISION>)"          , "<COLNAME>", "'<VALUE>'"), # 8 bytes  
         "BIGINT"           : ("DOUBLE PRECISION"            , "<COLNAME>", "<VALUE>"  ), # 8 Bytes : -2^63 ;+2^63-1
         "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # td: id NUMERIC    
         "ROWVERSION"       : ("DOUBLE PRECISION"            , "<COLNAME>", "'<VALUE>'"), # MS : An incremental number for versionning
         "TIMESTAMP"        : ("DOUBLE PRECISION"            , "<COLNAME>", "'<VALUE>'"), # MS = ROWVERSION
         "DATETIME"         : ("TIMESTAMP"                   , "CONVERT(VARCHAR,<COLNAME>,121)", "'<VALUE>'"),   # 1999-01-08 04:05:06
         "SMALLDATETIME"    : ("TIMESTAMP"                   , "CONVERT(VARCHAR,<COLNAME>,120)", "'<VALUE>'"),
         "NVARCHAR"         : ("NVARCHAR(<PRECISION>)"       , "CAST(<COLNAME> AS TEXT)", "'<VALUE>'"),          # Should be NTEXT but pb driver MSSQL
         "VARCHAR"          : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),
         "UNIQUEIDENTIFIER" : ("VARCHAR(64)"                 , "<COLNAME>", "'<VALUE>'"),                 # td: 64000 or 32000 (unicode)
         "NCHAR"            : ("NCHAR(<PRECISION>)"          , "<COLNAME>", "'<VALUE>'"),
         "CHAR"             : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"),
         "BINARY"           : ("BINARY(<PRECISION>)"         , "<COLNAME>", "<VALUE>"  ),
         "VARBINARY"        : ("VARBINARY(<PRECISION>)"      , "<COLNAME>", "<VALUE>"  ),
         "XML"              : ("CLOB"                        , "CAST(<COLNAME> AS NVARCHAR(max))", "'<VALUE>'") # MSSQL : 2GB
        }




'''
  Global translation table for datatype conversion from MYSQL to INGRES VW
  VW : not compressed datatypes :  DECIMAL with precision > 18, float, float4
       Unsupported datatypes : *BYTES, INGRESDATE, BOOLEAN, UUID, TABLE_KEY, OBJECT_KEY, * WITH TIMEZONE

'''
my2vw = {
         "INT"              : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer (-2^31; 2^31 - 1 (2,147,483,647))
         "INTEGER"          : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer (-2^31; 2^31 - 1 (2,147,483,647))
         "NUMERIC"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # MSSQL: -10^38 +1 through 10^38 –1.
         "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # MSSQL: -10^38 +1 through 10^38 –1.
         "DEC"              : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # MSSQL: -10^38 +1 through 10^38 –1.
         "FIXED"            : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ),
         "BIT"              : ("TINYINT"                     , "<COLNAME>", "<VALUE>"  ), # MY: Bit values
         "TINYINT"          : ("TINYINT"                     , "<COLNAME>", "<VALUE>"  ), # MY: -128:127 - VW: -128:+127
         "SMALLINT"         : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # 2 Bytes [-32768:32767]-
         "MEDIUMINT"        : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # MY: 3 Signed bytes
         "BIGINT"           : ("BIGINT"                      , "<COLNAME>", "<VALUE>"  ), # 8 Bytes : -2^63 ;+2^63-1
         "DOUBLE"           : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # 8 Bytes : -2^63 ;+2^63-1
         "DOUBLE PRECISION" : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # 8 Bytes : -2^63 ;+2^63-1
         "FLOAT"            : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "'<VALUE>'"), # MS(def) = float(53) precision(4 or 8)
         "REAL"             : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "'<VALUE>'"), # MS(def) = FLOAT(24)
         "ROWVERSION"       : ("BIGINT"                      , "<COLNAME>", "'<VALUE>'"), # MS : An incremental number for versionning
         "TIMESTAMP"        : ("BIGINT"                      , "<COLNAME>", "'<VALUE>'"), # MS = ROWVERSION
         "DATE"             : ("ANSIDATE"                    , "<COLNAME>", "'<VALUE>'"), # MY: 1999-01-08
         "DATETIME"         : ("TIMESTAMP(0)"                , "<COLNAME>", "'<VALUE>'"), # MY: 1999-01-08 04:05:06
         "TIMESTAMP"        : ("TIMESTAMP"                   , "<COLNAME>", "'<VALUE>'"), # MY: 1999-01-08 04:05:00???
         "TIME"             : ("TIME WITHOUT TIME ZONE"      , "<COLNAME>", "'<VALUE>'"), # MY: HH:MI:SS
         "YEAR"             : ("TINYINT"                     , "<COLNAME>", "<VALUE>"  ), # MY: 1901:2155, 0000
         "NCHAR"            : ("NCHAR(<PRECISION>)"          , "<COLNAME>", "'<VALUE>'"),
         "CHAR"             : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"),
         "NVARCHAR"         : ("NVARCHAR(<PRECISION>)"       , "CAST(<COLNAME> AS VARCHAR(max))", "'<VALUE>'"),
         "VARCHAR"          : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),
         "TEXT"             : ("VARCHAR(16000)"              , "<COLNAME>", "'<VALUE>'"), # MS: 2^31 Chars
         "LONGTEXT"         : ("VARCHAR(16000)"              , "<COLNAME>", "'<VALUE>'"), # MS: 2^31 Chars
         "LONGBLOB"         : ("VARCHAR(16000)"              , "<COLNAME>", "'<VALUE>'"), # MS: 2^31 Chars
         "BINARY"           : ("BINARY(<PRECISION>)"         , "<COLNAME>", "<VALUE>"  ), # MS: [1:8000] (Fixed size)
         "VARBINARY"        : ("VARBINARY(<PRECISION>)"      , "<COLNAME>", "<VALUE>"  ), # MS: [1:8000] (Var size)
}


'''
  Global translation table for datatype conversion from db2 to db2

'''
d22d2 = {
         "INTEGER"          : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ),
         "BIGINT"           : ("BIGINT"                      , "<COLNAME>", "<VALUE>"  ),
         "BINARY"           : ("BINARY"                      , "<COLNAME>", "<VALUE>"  ),
         "BLOB"             : ("BLOB"                        , "<COLNAME>", "<VALUE>"  ),
         "CHAR"             : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"),
         "CLOB"             : ("CLOB"                        , "<COLNAME>", "'<VALUE>'"),
         "DATE"             : ("DATE"                        , "<COLNAME>", "'<VALUE>'"),
         "DBCLOB"           : ("DBCLOB"                      , "<COLNAME>", "<VALUE>"  ),
         "SDECIMAL"         : ("SDECIMAL"                    , "<COLNAME>", "<VALUE>"  ),
         "DOUBLE"           : ("DOUBLE"                      , "<COLNAME>", "<VALUE>"  ),
         "FLOAT"            : ("FLOAT"                       , "<COLNAME>", "<VALUE>"  ),
         "GRAPHIC"          : ("GRAPHIC"                     , "<COLNAME>", "<VALUE>"  ),
         "INTEGER"          : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ),
         "LONGVARCHAR"      : ("LONGVARCHAR"                 , "<COLNAME>", "'<VALUE>'"),
         "LONGVAR"          : ("LONGVARCHAR"                 , "<COLNAME>", "'<VALUE>'"),
         "LONGVARBINARY"    : ("LONGVARBINARY"               , "<COLNAME>", "<VALUE>"  ),
         "LONGVARGRAPHIC"   : ("LONGVARGRAPHIC"              , "<COLNAME>", "<VALUE>"  ),
         "NUMERIC"          : ("NUMERIC(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ),
         "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ),
         "REAL"             : ("REAL"                        , "<COLNAME>", "<VALUE>"  ),
         "SMALLINT"         : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ),
         "TIME"             : ("TIME"                        , "<COLNAME>", "'<VALUE>'"),
         "TIMESTAMP"        : ("TIMESTAMP"                   , "<COLNAME>", "'<VALUE>'"),
         "TIMESTMP"         : ("TIMESTAMP"                   , "<COLNAME>", "'<VALUE>'"),
         "VARCHAR"          : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),
         "VARBINARY"        : ("VARBINARY"                   , "<COLNAME>", "<VALUE>"  ),
         "VARGRAPHIC"       : ("VARGRAPHIC"                  , "<COLNAME>", "<VALUE>"  )
        }





'''
  Global translation table for datatype conversion from db2 to Vector
  VW : not compressed datatypes :  DECIMAL with precision > 18, float, float4
       Unsupported datatypes : *BYTES, INGRESDATE, BOOLEAN, UUID, TABLE_KEY, OBJECT_KEY, * WITH TIMEZONE
'''
d22vw = {
         "INT"              : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # d2: [-2,147,483,648, +2,147,483,647]
         "INTEGER"          : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # d2: [-2,147,483,648, +2,147,483,647]
         "SMALLINT"         : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # d2: [32,768, +32,767]
         "BIGINT"           : ("BIGINT"                      , "<COLNAME>", "<VALUE>"  ),
         "NUMERIC"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # d2: (31,31)
         "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ),
         "REAL"             : ("FLOAT4"                      , "<COLNAME>", "<VALUE>"  ), # d2: -7.2E+75 to 7.2E+75
         "DOUBLE"           : ("FLOAT8"                      , "<COLNAME>", "<VALUE>"  ),
         "FLOAT"            : ("FLOAT8"                      , "<COLNAME>", "<VALUE>"  ), # d2: 8 Bits
         "TIME"             : ("TIME"                        , "<COLNAME>", "'<VALUE>'"),
         "TIMESTAMP"        : ("TIMESTAMP"                   , "<COLNAME>", "'<VALUE>'"),
         "TIMESTMP"         : ("TIMESTAMP"                   , "<COLNAME>", "'<VALUE>'"),
         "VARCHAR"          : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),
         "CHAR"             : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"),
         "BINARY"           : ("BINARY"                      , "<COLNAME>", "<VALUE>"  ),
         "BLOB"             : ("BLOB"                        , "<COLNAME>", "<VALUE>"  ),
         "CLOB"             : ("CLOB"                        , "<COLNAME>", "'<VALUE>'"),
         "DATE"             : ("DATE"                        , "<COLNAME>", "'<VALUE>'"),
         "DBCLOB"           : ("DBCLOB"                      , "<COLNAME>", "<VALUE>"  ),
         "SDECIMAL"         : ("SDECIMAL"                    , "<COLNAME>", "<VALUE>"  ),
         "GRAPHIC"          : ("GRAPHIC"                     , "<COLNAME>", "<VALUE>"  ),
         "LONGVARCHAR"      : ("LONGVARCHAR"                 , "<COLNAME>", "'<VALUE>'"),
         "LONGVAR"          : ("LONGVARCHAR"                 , "<COLNAME>", "'<VALUE>'"),
         "LONGVARBINARY"    : ("LONGVARBINARY"               , "<COLNAME>", "<VALUE>"  ),
         "LONGVARGRAPHIC"   : ("LONGVARGRAPHIC"              , "<COLNAME>", "<VALUE>"  ),
         "VARBINARY"        : ("VARBINARY"                   , "<COLNAME>", "<VALUE>"  ),
         "VARGRAPHIC"       : ("VARGRAPHIC"                  , "<COLNAME>", "<VALUE>"  )
        }


'''
  Global translation table for datatype conversion from TERADATA to INGRES VW

  NEED TERADATA DATATYPE AS DEFINED IN TABLE : DBC.Columns(c.ColumnType)
  VW : not compressed datatypes :  DECIMAL with precision > 18, float, float4
       Unsupported datatypes : *BYTES, INGRESDATE, BOOLEAN, UUID, TABLE_KEY, OBJECT_KEY, * WITH TIMEZONE
  
'''
td2vw = {
         "INT"              : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer (-2^31; 2^31 - 1 (2,147,483,647))
         "NUMERIC"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # MSSQL: -10^38 +1 through 10^38 –1. 
         "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ),
         "MONEY"            : ("FLOAT8"                      , "<COLNAME>", "<VALUE>"  ), # MSSQL : NUMERIC(19,4) - VW > 18 not compressed => FLOAT8
         "SMALLMONEY"       : ("NUMERIC(10,4)"               , "<COLNAME>", "<VALUE>"  ), # MSSQL : NUMERIC(10,4)
         "TINYINT"          : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # SQL: [O:255] - VW(TINYINT): -128:+127
         "SMALLINT"         : ("SMALLINT"                    , "<COLNAME>", "<VALUE>"  ), # 2 Bytes [-32768:32767]-
         "BIGINT"           : ("BIGINT"                      , "<COLNAME>", "<VALUE>"  ), # 8 Bytes : -2^63 ;+2^63-1
         "FLOAT"            : ("FLOAT(<PRECISION>)"          , "<COLNAME>", "'<VALUE>'"), # MS(def) = float(53) precision(4 or 8)
         "REAL"             : ("FLOAT(<PRECISION>)"          , "<COLNAME>", "'<VALUE>'"), # MS(def) = FLOAT(24)
         "ROWVERSION"       : ("BIGINT"                      , "<COLNAME>", "'<VALUE>'"), # MS : An incremental number for versionning
         "TIMESTAMP"        : ("BIGINT"                      , "<COLNAME>", "'<VALUE>'"), # MS = ROWVERSION
         "DATETIME"         : ("TIMESTAMP"                   , "CONVERT(VARCHAR,<COLNAME>,121)", "'<VALUE>'"), # MS: 1999-01-08 04:05:06.xxxx 
         "SMALLDATETIME"    : ("TIMESTAMP(0)"                , "CONVERT(VARCHAR,<COLNAME>,120)", "'<VALUE>'"), # MS: 1999-01-08 04:05:00
         "NCHAR"            : ("NCHAR(<PRECISION>)"          , "CAST(<COLNAME> AS NVARCHAR(max))", "'<VALUE>'"),
         "CHAR"             : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"),
         "NVARCHAR"         : ("NVARCHAR(<PRECISION>)"       , "CAST(<COLNAME> AS VARCHAR(max))", "'<VALUE>'"), 
         "VARCHAR"          : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),
         "TEXT"             : ("LONG VARCHAR"                , "<COLNAME>", "'<VALUE>'"), # MS: 2^31 Chars
         "NTEXT"            : ("LONG NVARCHAR"               , "<COLNAME>", "'<VALUE>'"), # MS: 2^31/2 UTF8 Chars
         "UNIQUEIDENTIFIER" : ("VARCHAR(64)"                 , "<COLNAME>", "'<VALUE>'"),
         "BINARY"           : ("BINARY(<PRECISION>)"         , "<COLNAME>", "<VALUE>"  ), # MS: [1:8000] (Fixed size)
         "VARBINARY"        : ("VARBINARY(<PRECISION>)"      , "<COLNAME>", "<VALUE>"  ), # MS: [1:8000] (Var size)
         "IMAGE"            : ("LONG BYTE"                   , "<COLNAME>", "<VALUE>"  ), # MS: 2^31 bytes
         "BIT"              : ("TINYINT"                     , "<COLNAME>", "<VALUE>"  ), # SQL:0/1; 
         "XML"              : ("LONG NVARCHAR"               , "CAST(<COLNAME> AS NVARCHAR(max))", "'<VALUE>'") # MSSQL : 2 GB
}

'''
  Global translation table for datatype conversion from ORACLE to INGRES VW

  VW : not compressed datatypes :  DECIMAL with precision > 18, float, float4
       Unsupported datatypes : *BYTES, INGRESDATE, BOOLEAN, UUID, TABLE_KEY, OBJECT_KEY, * WITH TIMEZONE
  OR : NUMBER(s,p) s. The precision p can range from 1 to 38. The scale s can range from -84 to 127
       TIMESTAMP(x) : [0-9] (def: 6)
  
'''
or2vw = {
         "INT"              : ("DECIMAL(38)"                 , "<COLNAME>", "<VALUE>"  ), # OR: DECIMAL(38)   
	 "INTEGER"          : ("DECIMAL(38)"                 , "<COLNAME>", "<VALUE>"  ), # OR: DECIMAL(38)
         "SMALLINT"         : ("DECIMAL(38)"                 , "<COLNAME>", "<VALUE>"  ), # OR: DECIMAL(38)
	 "NUMBER"           : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # OR: p[1,38]; s[84,127]; 1,22 Bytes VW: ?
	 "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # OR: p[1,38]; s[84,127]; 1,22 Bytes VW: ?
	 "FLOAT"            : ("FLOAT8"                      , "<COLNAME>", "<VALUE>"  ), # OR: p[1,38]; s[84,127]; 1,22 Bytes VW: ?
         "BINARY_DOUBLE"    : ("FLOAT8"                      , "<COLNAME>", "<VALUE>"  ), # OR:8 bytes float
         "BINARY_FLOAT"     : ("FLOAT4"                      , "<COLNAME>", "'<VALUE>'"), # OR:4 bytes float      
         "TIMESTAMP%"       : ("TIMESTAMP(<SCALE>)"          , "<COLNAME>", "'<VALUE>'"), # OR: [0-9] (def: 6) => Add precision VW [max 6]
         "TIMESTAMP % WITH TIME ZONE"      : ("TIMESTAMP(<SCALE>)", "<COLNAME>", "'<VALUE>'"), # Not implemented yet
         "TIMESTAMP % WITH LOCAL TIME ZONE": ("TIMESTAMP(<SCALE>)", "<COLNAME>", "'<VALUE>'"), # Not implemented yet
         "DATE"             : ("TIMESTAMP(0)"                , "TO_CHAR(<COLNAME>,'HH24-MM-DD HH24:MI:SS')", "'<VALUE>'"), # OR: 1999-01-08 04:05:00
         "CHAR"             : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"), # OR [1:2000]
         "VARCHAR"          : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),
         "VARCHAR2"         : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),
         "CLOB"             : ("VARCHAR(<PRECISION>)"        , "CAST(<COLNAME> AS VARCHAR(max))", "'<VALUE>'"),  # OR: 4GB-1*db_block_size VW: Not implemented
         "NVARCHAR"         : ("NVARCHAR(<PRECISION>)"       , "CAST(<COLNAME> AS VARCHAR(max))", "'<VALUE>'"), 
         "NCHAR"            : ("NCHAR(<PRECISION>)"          , "CAST(<COLNAME> AS NVARCHAR(max))", "'<VALUE>'"),
         "NCLOB"            : ("NVARCHAR(<PRECISION>)"       , "CAST(<COLNAME> AS VARCHAR(max))", "'<VALUE>'"),  # OR: 4GB-1*db_block_size
         "LONG"             : ("LONG VARCHAR"                , "<COLNAME>", "'<VALUE>'"), # OR: 2^31 Chars       # Add test on unsupported datatypes
         "ROWID"            : ("VARCHAR(64)"                 , "<COLNAME>", "'<VALUE>'"), # 
         "UROWID"           : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"), # OR: [1:4000]
         "RAW"              : ("BINARY(<PRECISION>)"         , "<COLNAME>", "<VALUE>"  ), # OR [1:2000]
         "LONG RAW"         : ("LONG BYTE"                   , "<COLNAME>", "<VALUE>"  ), # OR: 2^31 bytes
         "BLOB"             : ("NVARCHAR(<PRECISION>)"       , "CAST(<COLNAME> AS VARCHAR(max))", "'<VALUE>'"),  # OR: 4GB-1*db_block_size
         "BFILE"            : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'")  # OR: 4GB
}



''' 
  Translation table for equivalent datatype
  Ex: line 1: To transfert data from "mssql" to "netezza" we can use the postgres matrix "ms2pg"
              To transfert data from "mssql" to "teradata" we use table "ms2td"
'''
g_trmxty  = { "mssql"     : {"mysql"     : ms2my,
                             "postgres"  : ms2pg,
                             "netezza"   : ms2pg,
                             "greenplum" : ms2pg,
                             "teradata"  : ms2td,
                             "progress"  : ms2pr,
                             "maxdb"     : ms2md,
                             "matrix"    : ms2mx,
                             "ase"       : ms2ase,
                             "ingres"    : ms2ii,
                             "vector"    : ms2vw,
                             "vectorh"   : ms2vw,
                             "iq"        : ms2iq,
                             "hana"      : ms2ha
                            },
              "db2"       : {"db2"       : d22d2,
                             "vector"    : d22vw,
                            },
              "oracle"    : {"vector"    : or2vw
                            },
              "mysql"     : {"vector"    : my2vw
                            },
              "iq"        : {"vector"    : iq2vw
                            },
              "postgres"  : {"vector"    : pg2vw
                            },
              "teradata"  : {"vector"    : td2vw
                            }
            }           



''' 
   Print usage and exit
'''
def usage():
   print "Usage : "
   print g_pars 
   sys.exit(1)



''' 
   Get SQL definition from XML file. SQL is retrieved by using keywords passed as function parameters
   1: Database type "mysql, mssql, ..."
   2: Sqltype : Select, Create, ...
   3: Unique Identifier 
    
'''
def getXMLdata(p_dbtype, p_sql, p_id):
   rc=""
   xmldoc = xml.dom.minidom.parse(XMLINI)
   for node in xmldoc.getElementsByTagName(p_dbtype)[0].getElementsByTagName(p_sql): 
      if node.getAttribute("id") == p_id:
         for child in node.childNodes:
           if child.nodeType == Node.TEXT_NODE:
               rc = child.data
   return(rc)



''' Populate g_trnm which is a 2 dimension array built with values of parametre --translation
    Exemple :  --translation=scname:dbo,demo,test,toto;iscname:dbo,demo2
          => g_trnm['scname']['dbo']  = demo
             g_trnm['scname']['test'] = toto
             g_trnm['iscname']['dbo'] = demo2
          => iscname, scname are the column names in the header of an object description query
'''
def init_g_trnm(p_s):

   h2 = {}

   for line in p_s.split(';'):
      t1=line.split(':') 
      key1 = t1[0].strip()
      values_key1 = t1[1]
      t2=values_key1.split(',')
      for i in range(len(t2)/2):
         key2 = t2[i*2].strip()
         value_key2 = t2[i*2+1].strip()
         h2[key2] = value_key2
         g_trnm[key1]= deepcopy(h2)



''' Function which translate tuple (key1, key2) with values inserted in dictionnary g_trnm
    Input : key1, key2
    if datum is not found key2 is returned.
'''
def tr(p_key1, p_key2):
 
   rc = p_key2
   
   if g_trnm.has_key(p_key1) and g_trnm[p_key1].has_key(p_key2): rc = g_trnm[p_key1][p_key2]

   return(rc)



'''
  Strip values of a row and try to return encoded unicode string.
  If failed return ascii format
'''
def strip_row (p_row):

   row = []
   
   for v in p_row:
   
      if type(v) == str:
         v = v.strip()
         try: v = v.decode('utf_8')
         except UnicodeDecodeError: 
            pass

      row.append(v)

   return(row)



'''  
   Add Quote or not according to value of global variable : 
'''
def quote(p_str):

   s = p_str

   if g_quote is not None:
      s = g_quote + s + g_quote

   return(s)



''' 
   Generate tables based on src database and convert table to match the destination database format.
'''
def generateTb():
   scname_src  = ""
   scname  = ""
   tbname  = ""
   isnewtb = True
   isnewsch= True
   s       = ""
   rls     = []
   tmp_tgt = ""

   drp = s.split('\n');
   ddl = s.split('\n');
   sql = getXMLdata(p_dbtype=g_srcdbtype,  p_sql="select", p_id="tbDefinition")
   s   = getXMLdata(p_dbtype=g_destdbtype, p_sql="create", p_id="tb").strip()
   ddl += s.split('\n')
   s   = getXMLdata(p_dbtype=g_destdbtype, p_sql="create", p_id="drop" ).strip()
   drp += s.split('\n')

   cur = g_srcdb.execute(sql)
   for line in cur:
      row = strip_row(line)
      '''
	  An attempt to get one schema only loaded
	  '''
      if (p_ownsrc) == (""):				
          scname_src  = row[0]
          p_ownsrc == row[0]
      else:
          scname_src  = p_ownsrc
		  
      if (row[0],row[1]) != (scname_src, tbname):
#xxxx
         if row[0] != scname: isnewsch = True;   
         if (scname, tbname) != ("", ""):
            s += ddl[3]
            rls.append(s+g_cmdsep+"\n")
            ''' 
		    an attempt to load a specific table
		    '''
            if (p_owntgt) == (""):				
                scname  = row[0]
                p_owntgt == row[0]
            else:
                scname  = p_owntgt

		 
         tbname  = row[1]
         isnewtb = True;
         
      if isnewsch:
         s        = Template(getXMLdata(p_dbtype=g_destdbtype, p_sql="create", p_id="sch").strip())
         s        = s.substitute(scname=quote(tr('scname',scname)) )
         isnewsch = False
         if ( s!= "" ):
            rls.append(s+g_cmdsep+"\n")
         else:
            rls.append(s+"\n");
      if isnewtb:
         if p_addrop:
             s = Template(drp[1])                          # The drop table statement
             s = s.substitute(scname=quote(tr('scname',scname)), tbname=quote(tbname) )
             if ( s!= "" ):
                 rls.append(s+g_cmdsep+"\n")
         s = Template(ddl[1])                          # The create table statement
         s = s.substitute(scname=quote(tr('scname',scname)), tbname=quote(tbname) )
         isnewtb = False
      else:
         s += ','

      s        += "\n"
      clname    = row[2]
      tyname    = row[3]
      precision = row[4] if row[4] > 0 else p_charmax
      scale     = 0  if row[5] is None else row[5]
      isnull    = '' if row[6] is None else row[6]
      dfval     = '' if row[7] is None else "DEFAULT " + row[7]
	  
	  
	  

      if dfval in ms2vw_default:
        dfval = ms2vw_default[dfval]
      if "NEXT VALUE FOR" in dfval: dfval = ''
      (new_type, select_cast, insert_cast) = g_trty[tyname.upper()]    # Substitute datatype by equivalent datatype
      s        += Template(ddl[2]).substitute( clname=quote(clname), tyname=new_type, isnull=isnull, dfval=dfval)
      s         = s.replace('<PRECISION>', str(precision) )
      s         = s.replace('<SCALE>'    , str(scale)     )
   s += ddl[3]
   rls.append(s+g_cmdsep+"\n")

   return(rls)

''' 
   Generate views based on src database and convert view to match the destination database format.
   SELECT s.table_schema as scname, s.table_name as viwname, s.view_definition as viwdef, s.check_option as viwchk , s.is_updateable viwupd
'''
def generateViw():
   scname   = ""
   scname_src   = ""
   viwname  = ""
   viwdef   = ""
   viwchk   = ""
   viwupd   = ""
   tbname   = ""
   isnewviw = True
   isnewsch = True
   s        = ""
   rls      = []

   
   sql = getXMLdata(p_dbtype=g_srcdbtype,  p_sql="select", p_id="viwDefinition")
   s   = getXMLdata(p_dbtype=g_destdbtype, p_sql="create", p_id="viw"          ).strip()
   ddl = s.split(' ')
#   print "VIEW NAME>>>>"
#   print sql
#   print "VIEW NAME <<<<"

   '''
      START View
   '''
   cur = g_srcdb.execute(sql)
   for line in cur:
      row = strip_row(line)     
      print line
      '''
	  An attempt to get one schema only loaded
	  '''
      if (p_ownsrc) == (""):				
          scname_src  = row[0]
          p_ownsrc == row[0]
      else:
          scname_src  = p_ownsrc

      if (row[0],row[1]) != (scname_src, viwname): 
         if row[0] != scname: 
			isnewsch =True;   
         if (scname, viwname) != ("", ""):   
            rls.append(s+g_cmdsep+"\n")
         scname  = row[0]
         viwname = row[1]
         viwdef = row[2]
		 
         if (p_owntgt) == (""):				
            scname  = row[0]
         else:
            scname  = p_owntgt
         scname_src = row[0]
#         print "VIEW DEF>>>>"
#         print p_owntgt
#         print scname_src
#         print scname
#         print viwdef
#         print "VIEW DEF updated<<<<"

         # Tried regex to swap CONVERT to CAST, but it's detecting too much of the text
         #regex = re.compile(r'CONVERT\((?P<type>[^,]*),(?P<value>[^,]*)\)', re.MULTILINE)
         #viwdef = regex.sub(r'CAST(\g<value> AS \g<type>)', viwdef)
         for i in range(0,len(ms2vw_view)-1):
            viwdef = viwdef.replace(ms2vw_view[i][0], ms2vw_view[i][1])          

         viwdef = viwdef.replace(quote(p_ownsrc), quote(p_owntgt)) 
         viwdef = viwdef.replace('['+p_ownsrc+']', quote(p_owntgt))     		 
 #        print viwdef
 #        print '['+p_ownsrc+']'
 #        print "VIEW DEF end<<<<"
		  
         isnewviw = True
 
      if isnewsch:
         s        = Template(getXMLdata(p_dbtype=g_destdbtype, p_sql="create", p_id="sch").strip())
         s        = s.substitute(scname=quote(tr('scname',scname)) )
         isnewsch = False
         if ( s!= "" ):
            rls.append(s+g_cmdsep+"\n")
         else:
            rls.append(s+"\n")
      else:
	     print "Already got"
			
      if isnewviw:
         s = viwdef                          # The create view statement         
         isnewviw= False
      else:
         s += ""  

      s        += "\n"
      viwdef    = row[4]
#      print "VIEW NAME>>>>"
#      print viwname
#      print s
#      print "VIEW NAME <<<<"
#     print "VIEW DEF>>>>"
#     print viwdef
#     print row[1]
#     print "VIEW DEF<<<<"

#      s += viwdef    
#      rls.append(s+g_cmdsep+"\n")

   return(rls)
'''
   end View
'''



''' 
   Generate unique constraints (also inclue PrimaryKeys since a PK is also a unique constraint)
   based on src database and convert unique constraint to the new database format.
'''
def generateUk():
   scname_src  = ""
   scname  = ""
   tbname  = ""
   csname  = ""
   cstype  = ""
   isnewcs = True
   s       = ""
   msk = '","'
   t       = ""
   rls     = []                      # A returned list which contains the results of the function

   sql     = getXMLdata(p_dbtype=g_srcdbtype , p_sql="select", p_id="ukDefinition")
   ddl     = getXMLdata(p_dbtype=g_destdbtype, p_sql="create", p_id="uk"    ).strip()

   cur = g_srcdb.execute(sql)

   for line in cur:
 
      row    = strip_row(line)
      '''
	  An attempt to get one schema only loaded
	  '''
      if (p_ownsrc) == (""):				
          scname_src  = row[0]
          p_ownsrc == row[0]
      else:
          scname_src  = p_ownsrc
      ''' 
	  Constraint type
      Column Names
      '''	  
      cstype = row[3]
      clname = row[4]
   
      if (row[0], row[1], row[2]) != (scname_src, tbname, csname):

         if (scname, tbname, csname) != ("", "", ""):             # We pass to the next index definition
            s = Template(s).substitute(clname='')
            rls.append(s+g_cmdsep+"\n") 

#         scname  = row[0]
         tbname  = row[1]
         csname  = row[2]+p_idxsep+row[1]
         isnewcs = True
         ''' 
		    an attempt to load a specific table
		 '''
         if (p_owntgt) == (""):				
             scname  = row[0]
             scname_src  = row[0]
         else:
             scname  = p_owntgt
             scname_src  = p_ownsrc
			 
         s = Template(ddl)
         s = s.substitute(scname=quote(tr('scname',scname)), tbname=quote(tbname), 
                          csname=quote(csname), cstype=cstype, 
                          clname=quote(clname)+r'${clname}')

      if isnewcs:
         isnewcs = False
      else:   
         s = Template(s).substitute(clname=','+quote(clname)+r'${clname}')


   s = Template(s).substitute(clname='')                          # End Uk definition
   rls.append(s+g_cmdsep+"\n")

   return(rls) 



''' 
   Generate foreign key based on src database and convert fk to the new database format.
'''
def generateFk():
   scname  = ""
   scname_src = ""
   tbname  = ""
   csname  = ""
   isnewcs = True
   s       = ""
   rls     = []

   sql     = getXMLdata(p_dbtype=g_srcdbtype , p_sql="select", p_id="fkDefinition")
   ddl     = getXMLdata(p_dbtype=g_destdbtype, p_sql="create", p_id="fk"          ).strip()

   cur     = g_srcdb.execute(sql)

   for line in cur:

      row     = strip_row(line)
      '''
	  An attempt to get one schema only loaded
	  '''
      if (p_ownsrc) == (""):				
          scname_src  = row[0]
          p_ownsrc == row[0]
      else:
          scname_src  = p_ownsrc

      clname  = row[3]
      rclname = row[6]
	  

      if (row[0], row[1], row[2]) != (scname, tbname, csname):

         if (scname, tbname, csname) != ("", "", ""):             # We pass to the next index definition
            s = Template(s).substitute(clname='', rclname='')
            rls.append(s+g_cmdsep+"\n")  

         scname  = row[0]
         tbname  = row[1]
         ''' 
		    an attempt to load a specific table
		 '''
         if (p_owntgt) == (""):				
             scname  = row[0]
##             scname_src  = row[0]
         else:
             scname  = p_owntgt
             rscname  = p_owntgt
##             scname_src  = p_ownsrc
         ''' 
         Constraint names( Objects names )  must be unique in a schema
         ''' 
         csname  = row[2]+p_idxsep+row[1]

         ''' 
         Resource owner ... 
         ALTER TABLE "dbo"."Order Details" ADD CONSTRAINT "FK_Order_Details_Orders"      FOREIGN KEY ( "OrderID" )
         REFERENCES "dbo_rsc"."Orders_rtb" ( "OrderID" )
         '''
         rscname = row[4]
         '''
         ALTER TABLE "dbo"."CustomerCustomerDemo" ADD CONSTRAINT "FK_CustomerCustomerDemo"      FOREIGN KEY ( "CustomerTypeID" )
         REFERENCES "dbo"."CustomerDemographics_rtb" ( "CustomerTypeID" )
         '''
         rtbname = row[5]
         isnewcs = True
 
         s = Template(ddl)
         s = s.substitute( scname =quote(tr('scname' ,scname)) , tbname=quote(tbname), 
                           csname =quote(csname)               , clname=quote(clname)+r'${clname}', 
                           rscname=quote(tr('rscname',scname)), rtbname=quote(rtbname), 
                           rclname=quote(rclname)+r'${rclname}')

      if isnewcs:
         isnewcs = False
      else:   
         s = Template(s).substitute(clname=','+quote(clname)+r'${clname}', rclname=','+quote(rclname)+r'${rclname}')

   s = Template(s).substitute(clname='', rclname='')                          # End constraint definition
   rls.append(s+g_cmdsep+"\n")

   return(rls) 




''' 
   Generate indexes based on src database and convert indexes to match the destination database format.
'''
def generateIx():

   scname  = ""
   scname_src = ""
   tbname  = ""
   ixname  = ""
   isnewix = True
   s       = ""
   rls     = []

   sql     = getXMLdata(p_dbtype=g_srcdbtype , p_sql="select", p_id="ixDefinition")
   ddl     = getXMLdata(p_dbtype=g_destdbtype, p_sql="create", p_id="ix"          ).strip()
#   print "INDEX NAME>>>>"
#   print sql
#   print "INDEX NAME <<<<"

   cur = g_srcdb.execute(sql)

   for line in cur:
 #     print line
      row    = strip_row(line)
      '''
	  An attempt to get one schema only loaded
      '''
      if (p_ownsrc) == (""):				
          scname_src  = row[0]
          p_ownsrc == row[0]
      else:
          scname_src  = p_ownsrc

      clname = row[6]
#      print "CLNAME >>>"
#      print clname
#      print "CLNAME >>>"
	  

      if (row[0], row[1], row[2]) != (scname, tbname, ixname):

         if (scname, tbname, ixname) != ("", "", ""):             # We print last index and we pass to the next index definition
            s = Template(s).substitute(clname='')
            rls.append(s+"\n")
            if ixtype != 'BTREE': 
               print "Warning : Unknown %s index type %s. Following command has been skipped\n%s " % (g_srcdbtype, str(ixtype), s )         

         scname  = row[0]
         tbname  = row[1]
         ''' 
		    an attempt to load a specific table
         '''
         if (p_owntgt) == (""):				
             scname  = row[0]
             scname_src  = row[0]
         else:
             scname  = p_owntgt
             rscname  = p_owntgt
             scname_src  = p_ownsrc
         ''' 
         Constraint names( Objects names )  must be unique in a schema
         ''' 

         iscname = row[2]
         ixname  = row[3]+p_idxsep+row[1]
         ixtype  = row[4]
         ixuniq  = row[5]
         isnewix = True

         s = Template(ddl)    
         s = s.substitute(ixuniq  = '' if ixuniq is None else ixuniq, 
                          iscname = quote(tr('iscname', iscname)), 
                          ixname  = quote(ixname),
                          scname  = quote(tr('scname',scname)), 
                          tbname  = quote(tbname), 
#                          clname  = quote(clname))   
                          clname  = quote(clname)+r'${clname}')   

      print s  
      if isnewix:
         isnewix = False
      else:   
         s = Template(s).substitute(clname=','+quote(clname)+r'${clname}')
         print "ADDING ONE"  
   s = Template(s).substitute(clname='')                          # End index definition
   rls.append(s+"\n") 
   print s  

   return(rls)



''' 
   Extract data from src db and load data to dest db
'''
def unloadData(p_fdelim):

   scname  = ""
   tbname  = ""
   insert  = ""
   fname   = ""
   fdelim  = p_fdelim
   counter = 0
   select  = ""
   sqls    = []
   s       = ""
   colnum  = 0

   sql = getXMLdata(p_dbtype=g_srcdbtype, p_sql="select", p_id="tbDefinition")

   cursrc = g_srcdb.execute(sql)  
                                                # Iterate to prepare statements select, insert
   for line in cursrc:

      row  = strip_row(line)
      if (row[0],row[1]) != (scname, tbname):
         if (scname, tbname) != ("", ""):
            select += selfrom
            sqls.append( (fname, colnum, select, insert))

         scname = row[0]
         tbname = row[1]

         s      = quote(tbname) if scname is None else quote(scname) + '.' + quote(tbname)
         select = 'SELECT '
         selfrom= ' FROM ' + s
         
         s      = quote(tbname) if scname is None else quote(tr('scname',scname)) + '_' + quote(tbname) + '.txt'
         fname  = s
         insert = ""
         colnum = 0
          
      if colnum > 0: 
         insert += fdelim
         select += ","

      clname  = row[2]
      tyname  = row[3]


      (new_type, select_cast, insert_cast) = g_trty[tyname.upper()]     # Translate datatypes according to translation table

      select += select_cast
      select  = select.replace('<COLNAME>', '"'+clname+'"')
      insert += insert_cast
      insert  = insert.replace('<VALUE>', '<V'+str(colnum)+'>')
     
      colnum += 1
   
   select += selfrom
   sqls.append( (fname, colnum, select, insert) )

                                                                  # Iterate to select, bind and insert data
   for (fname, colnum, select, insert) in sqls: 
      try:

         print select
         cursrc = g_srcdb.execute(select)

         counter = 0

         f = codecs.open(fname, encoding='utf-8', mode= 'w')
 
         sz = 0.0
         t1 = time.time()

         for line in cursrc:                                       ## Read source cursor (SELECT)
            row = strip_row(line)
            s   = insert
            for i in range(0,colnum):                              ## Prepare INSERT 
               value = row[i]
   
               if value is None:                                   # When Null
                  s  = s.replace("'<V" + str(i) + ">'", '' )       # Replace NULL value for strings and dates
                  s  = s.replace("<V"  + str(i) + ">" , '' )       # Replace NULL value for integers or floats
                  sz+= 1

               elif type(value) == unicode:                        # When  string (unicode)
                  value = value.replace("'", "''")                 # Replace simple ' by '' in insert command
                  s     = s.replace("<V" + unicode(str(i), 'utf-8') + ">", value)
                  sz   += len(value) 

               else:                                               # When not string 
                  try:
                     s     = s.replace("<V" + str(i) + ">", str(value) )      
                     sz   += len(str(value))                       # Not right but gives an idea of the size of numbers 
                  except UnicodeDecodeError:                       # String has an unknown character
                     print "UnicodeDecodeError for column <%d> value = " % (i),
                     print value
                     value = value.replace("'", "''")              # Replace simple ' by '' in insert command
                     s     = s.replace("<V" + str(i) + ">", unicode(value, 'utf-8',  'ignore') )
                     sz   += len(value) 
                  finally:
                     pass
                  
            try:                                                    ## Write line
               f.write(s + "\n")
               counter += 1

            except Exception:
               print "%s" % (s)
               traceback.print_exc(file=sys.stdout)

            finally:
               pass

      except Exception:
         print "%s" % (s)
         traceback.print_exc(file=sys.stdout)

      finally:
         t2 = time.time()
         f.close()
         print "Rows extracted: %d - Elapsed time(s): %f - Mean data size(MB): %f\n" % (counter, (t2-t1), sz/1024/1024)



''' 
   Loads a lot of data in a single predefined table.
'''
def loadTest():
  # Run precondition script if found (e.g. this can be used to setup session authorization)
  pres = getXMLdata(p_dbtype=g_destdbtype,  p_sql="create", p_id="pre")
  pre = Template(pres).substitute(scname=quote(tr('scname',"dbo")))
  g_destdb.execute(pre)
  # Iterate to select, bind and insert data
  s = ""
  total = 10000
  try:
      counter = 0
      currentCounter = 0
      inserts = []

      t1 = time.time()
      for j in range (0, total/p_batchsize):
        currentCounter = 0
        inserts = []
        for i in range (0, p_batchsize):
          s = "INSERT INTO dbo.Territories VALUES (%d,'%s',%d)" % (i, "Teritory%d" % i, 2*i)
          inserts.append(s)
        t2 = time.time()
        currentCounter += len(inserts) * insertSQL(g_destdb, ";\n".join(inserts))
        counter += currentCounter
        print "[%d] Batch inserted: %d - Elapsed time(s): %f" % (j, currentCounter, (t2-t1))
      
      t2 = time.time()
      print "Total rows inserted: %d - Elapsed time(s): %f" % (counter, (t2-t1))

  except Exception:
      print s
      traceback.print_exc(file=sys.stdout)

  finally:
      pass

''' 
   Extract data from src db and load data to dest db
   @param truncate: Decides if the existing data should be erased before import
'''
def loadData(truncate):

   scname  = ""
   tbname  = ""
   insert  = ""
   counter = 0
   select  = ""
   sqls    = []
   s       = ""
   colnum  = 0
   table_name = ""

   sql = getXMLdata(p_dbtype=g_srcdbtype, p_sql="select", p_id="tbDefinition")

   cursrc = g_srcdb.execute(sql)  
                                                # Iterate to prepare statements select, insert
   for line in cursrc:

      row  = strip_row(line)
      if (row[0],row[1]) != (scname, tbname):
         if (scname, tbname) != ("", ""):
            insert += ")"
            select += selfrom
            sqls.append( (colnum, select, insert, quote(tbname) if scname is None else quote(tr('scname',scname)) + '.' + quote(tbname)))

         scname = row[0]
         tbname = row[1]

         s      = quote(tbname) if scname is None else quote(scname) + '.' + quote(tbname)
         select = 'SELECT '
         selfrom= ' FROM ' + s

         s      = quote(tbname) if scname is None else quote(tr('scname',scname)) + '.' + quote(tbname)
         table_name = s
         insert = 'INSERT INTO '   + s + ' VALUES ('
         colnum = 0
          
      if colnum > 0: 
         insert += ","
         select += ","

      clname  = row[2]
      tyname  = row[3]


      (new_type, select_cast, insert_cast) = g_trty[tyname.upper()]     # Translate datatypes according to translation table

      select += select_cast
      select  = select.replace('<COLNAME>', '"'+clname+'"')
      insert += insert_cast
      insert  = insert.replace('<VALUE>', '<V'+str(colnum)+'>')
     
      colnum += 1


   insert += ")"
   select += selfrom
   sqls.append( (colnum, select, insert, table_name))

   # Run precondition script if found (e.g. this can be used to setup session authorization)
   pres = getXMLdata(p_dbtype=g_destdbtype,  p_sql="create", p_id="pre")
   pre = Template(pres).substitute(scname=quote(tr('scname',scname)))
   for pre_line in pre.split(';'):
    g_destdb.execute(pre_line)

   # Iterate to select, bind and insert data
   for (colnum, select, insert, table_name) in sqls: 
      try:
         print select
         cursrc = g_srcdb.execute(select)

         counter = 0
         currentCounter = 0
         inserts = []

         print "Loading ..."
         sz = 0.0
         t1 = time.time()

         # If truncate was specified remove existing rows from the destination table.
         if truncate and table_name !='' :
            g_destdb.execute('MODIFY %s TO TRUNCATED' % table_name)
        
         isFirstInsert = True 
         for line in cursrc:                          ## Read source cursor (SELECT)
            row = strip_row(line)
            s   = insert
            for i in range(0,colnum):                           ## Prepare INSERT 
               value = row[i]
   
               if value is None:                                  # When Null
                  s  = s.replace("'<V" + str(i) + ">'", 'NULL' )  # Replace NULL value for strings and dates
                  s  = s.replace("<V"  + str(i) + ">" , 'NULL' )  # Replace NULL value for integers or floats
                  sz+= 1

               elif type(value) == unicode:                       # When  string (unicode)
                  value = value.replace("'", "''")                # Replace simple ' by '' in insert command
                  s     = s.replace("<V" + unicode(str(i), 'utf-8') + ">", value)
                  sz   += len(value) 

               else:                                               # When not string 
                  try:
                     s     = s.replace("<V" + str(i) + ">", str(value) )      
                     sz   += len(str(value))                       # Not right but gives an idea of the size of numbers 
                  except UnicodeDecodeError:                       # String has an unknown character
                     print "UnicodeDecodeError for column <%d> value = " % (i),
                     print value
                     value = value.replace("'", "''")              # Replace simple ' by '' in insert command
                     s     = s.replace("<V" + str(i) + ">", unicode(value, 'utf-8',  'ignore') )
                     sz   += len(value) 
                     print "- Resulting DML : %s " % (s)
                  finally:
                     pass
            if isFirstInsert:
              inserts.append(s)
              isFirstInsert = False
            else:
              # Get the string after VAULES, e.g. ('<V0>','<V1>',<V2>)
              s = s.encode("utf-8")
              values = s[s.index("VALUES") + 6:]
              inserts.append(values)
              currentCounter = len(inserts)
            if currentCounter>= p_batchsize:
              counter += currentCounter * insertSQL(g_destdb, ",".join(inserts))
              inserts = []
              t2 = time.time()
              print "Batch inserted: %d - Elapsed time(s): %f - Estimated size(MB): %f\n" % (currentCounter, (t2-t1), sz/1024/1024)
              currentCounter = 0
              isFirstInsert = True
         if currentCounter > 0:
          counter += currentCounter * insertSQL(g_destdb, ",".join(inserts))

         t2 = time.time()
         print "Total Rows inserted: %d - Elapsed time(s): %f - Estimated size(MB): %f\n" % (counter, (t2-t1), sz/1024/1024)

      except Exception:
         print s
         traceback.print_exc(file=sys.stdout)

      finally:
         pass


''' 
   call db sql script with exception wrap
'''
def insertSQL(db, sql):
    try:                                                 ## Execute INSERT 
      with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        if len(w) > 0: print len(w)
        db.execute(sql)
        db.commit ()
        if len(w) > 0: 
            print sql
            print str(w[-1].message)
        return 1

    except Exception:
      print sql
      traceback.print_exc(file=sys.stdout)

    finally:
      pass
    
    return 0
# -------------------------------------------------------------------------------------------

#                                            MAIN

# -------------------------------------------------------------------------------------------


p_loadata   = False
p_cretab    = False
p_creall    = False
p_creview   = False
p_creindex  = False

p_addrop    = False
p_loaddl    = False
p_unload    = False
p_fdelim    = "\t"
p_src       = None
p_dest      = None
p_quote     = None
p_cmdsep    = None
p_batchsize = 1000
p_charmax   = 6400
p_loadtest  = False
p_truncate  = False
global p_ownsrc  
global p_owntgt  

fname       = ""
tbs         = []  
uks         = []
ixs         = []
fks         = []
viw         = []

if sys.argv[1:] == []: usage()                          ## Print usage and exit

try:
   opts, args = getopt.getopt(sys.argv[1:], '', g_pars )
except getopt.GetoptError, err:
   print str(err)
   for s in g_pars: print s
   sys.exit(2)
####generateViw
p_owntgt = ""
p_ownsrc = ""
for opt, arg in opts:
   if   opt == "--src"    : p_src    = arg.strip()
   elif opt == "--dest"   : p_dest   = arg.strip()
   elif opt == "--quote"  : p_quote  = arg.strip()
   elif opt == "--cmdsep" : p_cmdsep = arg.strip()
   elif opt == "--loadata": p_loadata= True
   elif opt == "--cretab" : p_cretab = True
   elif opt == "--creindex" : p_creindex = True
   elif opt == "--add_drop" : p_addrop = True
   elif opt == "--creall" : p_creall = True
   elif opt == "--creview" : p_creview = True
   elif opt == "--unload" : p_unload = True
   elif opt == "--fdelim" : p_fdelim = arg.strip()
   elif opt == "--loaddl" : p_loaddl = True
   elif opt == "--batchsize": p_batchsize= int(arg.strip())
   elif opt == "--charmax" : p_charmax= int(arg.strip())
   elif opt == "--ownsrc" : p_ownsrc= arg.strip()
   elif opt == "--owntgt" : p_owntgt= arg.strip()
   elif opt == "--loadtest": p_loadtest= True
   elif opt == "--truncate": p_truncate= True
   elif opt == "--parfile": 
      fname    = arg
      if fname != "":
         for line in open(fname, 'r'):
           if re.match("^--.*=.*", line): 
              (param, value) = line.split('=')
              param = param.strip()
              value = value.strip()
              if   param == "--src"   : p_src   = value
              elif param == "--dest"  : p_dest  = value
              elif param == "--quote" : p_quote = value
              elif param == "--cmdsep": p_cmdsep= value
              elif param == "--fdelim": p_fdelim= value
              elif param == "--translation": init_g_trnm(value)
              elif param == "--owntgt"  : p_owntgt  = value
              elif param == "--ownsrc"  : p_ownsrc  = value
   else:
      assert False, "unhandled option"

if p_dest == "vector": 
    p_idsep = "_x100"
elif p_dest == "vectorh": 
    p_idsep = "_x100"
else:
    p_idxsep = "_ax11_"	
	
print "Src  = %s" % (p_src)
print "Dest = %s" % (p_dest)
print "New Target Owner = %s" % (p_owntgt)
print "Single Source Owner = %s" % (p_ownsrc)

g_srcdbtype  = p_src.split(':')[0].split('-')[0]
g_destdbtype = p_dest.split(':')[0].split('-')[0]

g_trty       = g_trmxty[g_srcdbtype][g_destdbtype]  

g_srcdb      = dbconnector(p_src)

if p_loaddl or p_loadata or p_loadtest:   g_destdb = dbconnector(p_dest)

if p_quote  is not None:    g_quote  = p_quote

if p_cmdsep is not None:    g_cmdsep = p_cmdsep


if p_cretab:
   tbs = generateTb()
   fname = g_prg+'_tab.txt'
   f = codecs.open(fname, encoding='utf-8', mode= 'w')
   for s in tbs: f.write(s)
   f.close()

if p_creview:
   viw = generateViw()
   fname = g_prg+'_viw.txt'
   f = codecs.open(fname, encoding='utf-8', mode= 'w')
   for s in viw: f.write(s)
   f.close()


if p_loaddl and p_cretab:
   for s in tbs: 
      try:          
         g_destdb.execute(s)
         print s
      except Exception: 
         print s
         print sys.exc_info()

if p_loaddl and p_creview:
   for s in viw: 
      try:          
         g_destdb.execute(s)
         print s
      except Exception: 
         print s
         print sys.exc_info()



if p_loadata: loadData(p_truncate)
if p_unload : unloadData(p_fdelim)
if p_loadtest: loadTest()

if p_creall:
   uks = generateUk()
   ixs = generateIx()
   fks = generateFk()
   viw = generateViw()
   fname = g_prg+'_all.txt'
   f = codecs.open(fname, encoding='utf-8', mode= 'w')
   for s in uks: f.write(s)
   for s in ixs: f.write(s)
   for s in fks: f.write(s)
   for s in viw: f.write(s)

   f.close()
   
if p_creindex:
   uks = generateUk()
   ixs = generateIx()
   fks = generateFk()
   fname = g_prg+'_index.txt'
   f = codecs.open(fname, encoding='utf-8', mode= 'w')
   for s in uks: f.write(s)
   for s in ixs: f.write(s)
   for s in fks: f.write(s)

   f.close()

if p_loaddl and p_creall:
   for s in uks: 
      print s
      try:                 g_destdb.execute(s)
      except Exception, e: print "Exception : ", e
   for s in ixs: 
      print s
      try:                 g_destdb.execute(s)
      except Exception, e: print "Exception : ", e
   for s in fks: 
      print s
      try:                 g_destdb.execute(s)
      except Exception, e: print "Exception : ", e
   for s in viw: 
      print s
      try:                 g_destdb.execute(s)
      except Exception, e: print "Exception : ", e

if p_loaddl and p_creindex:
   for s in uks: 
      print s
      try:                 g_destdb.execute(s)
      except Exception, e: print "Exception : ", e
   for s in ixs: 
      try:                 g_destdb.execute(s)
      except Exception, e: print "Exception : ", e
   for s in fks: 
      print s
      try:                 g_destdb.execute(s)
      except Exception, e: print "Exception : ", e



if p_loaddl or p_loadata: g_destdb.close()

g_srcdb.close()

