#!/usr/bin/env python
# -*- coding: utf-8 -*

# Copyright 2018 Actian Corporation

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#      http://www.apache.org/licenses/LICENSE-2.0

#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    History
#    cooda09    28-01-19        Corrections and additions to mapping parameters



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
_pg2vw = {
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
_iq2vw = {
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
_ms2my = {
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
_ms2ase = {
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
_ms2iq = {
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
_ms2pg = {
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
_ms2mx = {
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
_ms2ii = {
    "INT"              : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer (-2^31; 2^31 - 1 (2,147,483,647))
    # added the full INTEGER value
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
_ms2vw = {
    "INT"              : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer (-2^31; 2^31 - 1 (2,147,483,647))
    "INTEGER"          : ("INTEGER"                     , "<COLNAME>", "<VALUE>"  ), # Signed 4 bytes integer (-2^31; 2^31 - 1 (2,147,483,647))
    # added the INTEGER
    "PLACEHOLDER"      : ("PLACEHOLDER"                 , "<COLNAME>", "<VALUE>"  ), # MSSQL : NUMERIC(10,4)
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
    "VARBINARY"        : ("CHARACTER VARYING(4000)"     , "CASE WHEN <COLNAME> IS NOT NULL THEN '--IMAGE--' END", "'<VALUE>'"  ), # MS: [1:8000] (Var size)
    "IMAGE"            : ("LONG BYTE"                   , "<COLNAME>", "<VALUE>"  ), # MS: 2^31 bytes
    "BIT"              : ("TINYINT"                     , "CAST(<COLNAME> AS TINYINT)", "<VALUE>"  ), # SQL:0/1; 
    "XML"              : ("NVARCHAR(4000)"              , "CAST(<COLNAME> AS NVARCHAR(4000))", "'<VALUE>'"), # MSSQL : 2 GB
    "HIERARCHYID"      : ("NVARCHAR(4000)"              , "CAST(<COLNAME> AS NVARCHAR(4000))", "'<VALUE>'"),
    "GEOMETRY"         : ("NVARCHAR(4000)"              , "CAST(<COLNAME> AS NVARCHAR(4000))", "'<VALUE>'"),
    "GEOGRAPHY"        : ("NVARCHAR(4000)"              , "CAST(<COLNAME> AS NVARCHAR(4000))", "'<VALUE>'"),
}

ms2vw_default = {
    "DEFAULT newid"   : "",
    "DEFAULT getdate" : "DEFAULT CURRENT_TIMESTAMP",
    "IDENTITY(1,1)"   : "GENERATED BY DEFAULT AS IDENTITY ",
    "COMMA"           : """, """
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
_ms2td = {
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
_ms2pr = {
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
_ms2md = {
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
_ms2ha = {
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
_my2vw = {
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
_d22d2 = {
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
_d22vw = {
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
_td2vw = {
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
_or2vw = {
    "BIGINT"           : ("BIGINT"                 , "<COLNAME>", "<VALUE>"  ), # OR: DECIMAL(38)   
    "TINYINT"          : ("TINYINT"                 , "<COLNAME>", "<VALUE>"  ), # OR: DECIMAL(38)   
    "INT"              : ("INTEGER"                 , "<COLNAME>", "<VALUE>"  ), # OR: DECIMAL(38)   
    "INTEGER"          : ("INTEGER"                 , "<COLNAME>", "<VALUE>"  ), # OR: DECIMAL(38)
    "SMALLINT"         : ("SMALLINT"                 , "<COLNAME>", "<VALUE>"  ), # OR: DECIMAL(38)
    "NUMBER"           : ("DECIMAL(<PRECISION>,0)", "<COLNAME>", "<VALUE>"  ), # OR: p[1,38]; s[84,127]; 1,22 Bytes VW: ?
    "DECIMAL"          : ("DECIMAL(<PRECISION>,0)", "<COLNAME>", "<VALUE>"  ), # OR: p[1,38]; s[84,127]; 1,22 Bytes VW: ?
    "FLOAT"            : ("FLOAT8"                      , "<COLNAME>", "<VALUE>"  ), # OR: p[1,38]; s[84,127]; 1,22 Bytes VW: ?
    "BINARY_DOUBLE"    : ("FLOAT8"                      , "<COLNAME>", "<VALUE>"  ), # OR:8 bytes float
    "BINARY_FLOAT"     : ("FLOAT4"                      , "<COLNAME>", "'<VALUE>'"), # OR:4 bytes float      
    "TIMESTAMP%"       : ("TIMESTAMP(<SCALE>)"          , "<COLNAME>", "'<VALUE>'"), # OR: [0-9] (def: 6) => Add precision VW [max 6]
    "TIMESTAMP% WITH TIME ZONE"      : ("TIMESTAMP(<SCALE>)", "<COLNAME>", "'<VALUE>'"), # Not implemented yet
    "TIMESTAMP% WITH LOCAL TIME ZONE": ("TIMESTAMP(<SCALE>)", "<COLNAME>", "'<VALUE>'"), # Not implemented yet
    "DATE"             : ("TIMESTAMP(0)"                , "TO_CHAR(<COLNAME>,'YYYY-MM-DD HH24:MI:SS')", "'<VALUE>'"), # OR: 1999-01-08 04:05:00
    "CHAR"             : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"), # OR [1:2000]
    "VARCHAR"          : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),
    "VARCHAR2"         : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),
    "CLOB"             : ("VARCHAR(<PRECISION>)"        , "CAST(<COLNAME> AS VARCHAR(max))", "'<VALUE>'"),  # OR: 4GB-1*db_block_size VW: Not implemented
    "NVARCHAR"         : ("NVARCHAR(<PRECISION>)"       , "<COLNAME>", "'<VALUE>'"), 
    "NVARCHAR2"        : ("NVARCHAR(<PRECISION>)"       , "<COLNAME>", "'<VALUE>'"), 
    "NCHAR"            : ("NCHAR(<PRECISION>)"          , "<COLNAME>", "'<VALUE>'"),
    "RAW"             : ("UUID"                        , "CAST(<COLNAME> AS VARCHAR(36))", "'<VALUE>'") # OR: p[1,38]; s[84,127]; 1,22 Bytes VW: ?
}


'''
    Global translation table for datatype conversion from ORACLE to INGRES VW

    VW : not compressed datatypes :  DECIMAL with precision > 18, float, float4
        Unsupported datatypes : *BYTES, INGRESDATE, BOOLEAN, UUID, TABLE_KEY, OBJECT_KEY, * WITH TIMEZONE
    OR : NUMBER(s,p) s. The precision p can range from 1 to 38. The scale s can range from -84 to 127
        TIMESTAMP(x) : [0-9] (def: 6)
'''
_or2ii = {
    "BIGINT"           : ("BIGINT"                 , "<COLNAME>", "<VALUE>"  ), # OR: DECIMAL(38)   
    "TINYINT"          : ("TINYINT"                 , "<COLNAME>", "<VALUE>"  ), # OR: DECIMAL(38)   
    "INT"              : ("INTEGER"                 , "<COLNAME>", "<VALUE>"  ), # OR: DECIMAL(38)   
    "INTEGER"          : ("INTEGER"                 , "<COLNAME>", "<VALUE>"  ), # OR: DECIMAL(38)
    "SMALLINT"         : ("SMALLINT"                 , "<COLNAME>", "<VALUE>"  ), # OR: DECIMAL(38)
    "NUMBER"           : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # OR: p[1,38]; s[84,127]; 1,22 Bytes VW: ?
    "DECIMAL"          : ("DECIMAL(<PRECISION>,<SCALE>)", "<COLNAME>", "<VALUE>"  ), # OR: p[1,38]; s[84,127]; 1,22 Bytes VW: ?
    "FLOAT"            : ("FLOAT8"                      , "<COLNAME>", "<VALUE>"  ), # OR: p[1,38]; s[84,127]; 1,22 Bytes VW: ?
    "BINARY_DOUBLE"    : ("FLOAT8"                      , "<COLNAME>", "<VALUE>"  ), # OR:8 bytes float
    "BINARY_FLOAT"     : ("FLOAT4"                      , "<COLNAME>", "'<VALUE>'"), # OR:4 bytes float      
    "TIMESTAMP%"       : ("TIMESTAMP(<SCALE>)"          , "<COLNAME>", "'<VALUE>'"), # OR: [0-9] (def: 6) => Add precision VW [max 6]
    "TIMESTAMP% WITH TIME ZONE"      : ("TIMESTAMP(<SCALE>)", "<COLNAME>", "'<VALUE>'"), # Not implemented yet
    "TIMESTAMP% WITH LOCAL TIME ZONE": ("TIMESTAMP(<SCALE>)", "<COLNAME>", "'<VALUE>'"), # Not implemented yet
    "DATE"             : ("TIMESTAMP(0)"                , "TO_CHAR(<COLNAME>,'YYYY-MM-DD HH24:MI:SS')", "'<VALUE>'"), # OR: 1999-01-08 04:05:00
    "CHAR"             : ("CHAR(<PRECISION>)"           , "<COLNAME>", "'<VALUE>'"), # OR [1:2000]
    "VARCHAR"          : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),
    "VARCHAR2"         : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),
    "CLOB"             : ("VARCHAR(<PRECISION>)"        , "CAST(<COLNAME> AS VARCHAR(max))", "'<VALUE>'"),  # OR: 4GB-1*db_block_size VW: Not implemented
    "NVARCHAR"         : ("NVARCHAR(<PRECISION>)"       , "<COLNAME>", "'<VALUE>'"), 
    "NVARCHAR2"        : ("NVARCHAR(<PRECISION>)"       , "<COLNAME>", "'<VALUE>'"), 
    "NCHAR"            : ("NCHAR(<PRECISION>)"          , "<COLNAME>", "'<VALUE>'"),
    "NCLOB"            : ("NVARCHAR(<PRECISION>)"       , "CAST(<COLNAME> AS VARCHAR(max))", "'<VALUE>'"),  # OR: 4GB-1*db_block_size
    "LONG"             : ("LONG VARCHAR"                , "<COLNAME>", "'<VALUE>'"), # OR: 2^31 Chars       # Add test on unsupported datatypes
    "ROWID"            : ("VARCHAR(64)"                 , "<COLNAME>", "'<VALUE>'"), # 
    "UROWID"           : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"), # OR: [1:4000]
''' 
    Make an attempt at putting RAW which is often a UUID into UUID format in Ingres/vector
    "RAW"              : ("BINARY(<PRECISION>)"         , "<COLNAME>", "<VALUE>"  ), # OR [1:2000]
'''
    "LONG RAW"         : ("LONG BYTE"                   , "<COLNAME>", "<VALUE>"  ), # OR: 2^31 bytes
    "BLOB"             : ("NVARCHAR(<PRECISION>)"       , "CAST(<COLNAME> AS VARCHAR(max))", "'<VALUE>'"),  # OR: 4GB-1*db_block_size
    "GUID"             : ("UUID"                        , "<COLNAME>", "<VALUE>"  ), # OR: p[1,38]; s[84,127]; 1,22 Bytes VW: ?
    "BFILE"            : ("VARCHAR(<PRECISION>)"        , "<COLNAME>", "'<VALUE>'"),  # OR: 4GB
	"RAW"              : ("UUID"                        , "CAST(<COLNAME> AS VARCHAR(36))", "'<VALUE>'") # OR: p[1,38]; s[84,127]; 1,22 Bytes VW: ?
}

''' 
    Translation table for equivalent datatype
    Ex: line 1: To transfert data from "mssql" to "netezza" we can use the postgres matrix "ms2pg"
                To transfert data from "mssql" to "teradata" we use table "ms2td"
'''
db_column_mapping  = { "mssql"      : {"mysql"     : _ms2my,
                                        "postgres"  : _ms2pg,
                                        "netezza"   : _ms2pg,
                                        "greenplum" : _ms2pg,
                                        "teradata"  : _ms2td,
                                        "progress"  : _ms2pr,
                                        "maxdb"     : _ms2md,
                                        "matrix"    : _ms2mx,
                                        "ase"       : _ms2ase,
                                        "ingres"    : _ms2ii,
                                        "vector"    : _ms2vw,
                                        "vectorh"   : _ms2vw,
                                        "iq"        : _ms2iq,
                                        "hana"      : _ms2ha
                                        },
                        "db2"       : {"db2"       : _d22d2,
                                        "vector"    : _d22vw,
                                        },
                        "oracle"    : {"vector"    : _or2vw,
                                        "ingres"   : _or2ii,
                                        "actianx"  : _or2ii,
                                        "vectorc"  : _or2vw,
                                        "vectorh"  : _or2vw
                                        },
                        "mysql"     : {"vector"    : _my2vw
                                        },
                        "iq"        : {"vector"    : _iq2vw
                                        },
                        "postgres"  : {"vector"    : _pg2vw
                                        },
                        "teradata"  : {"vector"    : _td2vw
                                        }
                        }

_unsupported_types_mapping = {
    "mssql" : {
        "vector": ['image', 'hierarchyid', 'geometry', 'geography', 'varbinary', 'binary', 'xml']
    },
	"oracle" : {
        "vector": ['RAW', 'CLOB', 'BLOB', 'BYTE', 'VARBYTE', 'BINARY' ],
        "vectorc": ['raw', 'image', 'hierarchyid', 'geometry', 'geography', 'varbinary', 'binary', 'xml'],
        "vectorh": ['raw', 'image', 'hierarchyid', 'geometry', 'geography', 'varbinary', 'binary', 'xml']
    }
}

_source_schema_filters = {
    "mssql" : "${source_schema_name}"
}

def get_types_mapping(source_db_type, target_db_type):
    return (db_column_mapping[source_db_type][target_db_type])

def get_unsupported_types(source_db_type, target_db_type):
    types = []
    if _unsupported_types_mapping.has_key(source_db_type) and _unsupported_types_mapping[source_db_type].has_key(target_db_type):
        types = _unsupported_types_mapping[source_db_type][target_db_type]
    return(types)

def get_unsupported_types_csv(source_db_type, target_db_type):
    types = get_unsupported_types(source_db_type, target_db_type)
    csv = "','".join(types)
    return(csv)

def get_source_schema_filter(source_db_type, schema):
    res = ''
    if schema is not None and _source_schema_filters.has_key(source_db_type):
        res = _source_schema_filters[source_db_type].replace('${source_schema_name}', schema)
    return(res)
