

## ---------------------------------------------------------------------------

   dbmv.py helps you to move one database schema to another database

## ---------------------------------------------------------------------------


Source schema can be:


	    ORACLE Mysql 
	    MICROSOFT SqlServer 
	    ORACLE
	    POSTGRES
	    SAP Sybase IQ (V15+) 
            TERADATA
	
	Schema description can be easily extended to any database in the destination list.
	You need to add missing description for "tbDefinition", etc, ... in file "dbmx.xml".
        SQL output, field names, SELECT descriptions must be respect the pattern used 
	for other existing databases



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
     ['src=', 'dest=', 'loadata', 'cretab', 'creall', 'loaddl', 'parfile=', 'fdelim=', 'unload', 'translation=', 'quote=', 'cmdsep=']


   src    : Source database (--src=URL)
   dest   : Destination database (--src=URL)

          URL : <database provider>[-odbc]://<database hostname>[:port]/<database name>?<database login>&<login password>

          profiler list:

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


            About ODBC : 

               Linux database drivers path and configuration must be added in file "../etc/driverTools.xml" (see in-file examples)
               In Window systems, ODBC drivers and Datasource must exist. No additional configuration is required.

	       Supported python ODBC => pyodbc




   cretab : Switch to Create tables only
   creall : Switch to all objects TABLES, INDEXES, PRIMARY KEYS, REFERENTIAL CONSTRAINTS
   loaddl : Load ddl into destination database. If not specified, a file containing all objects   
      	   is created in the current directory. In this latest case, you can reload 

   parfile: All parameters can be stored is a parfile. Parfiles examples can be find in the "wrk" directory. 

   translation : Schema name translation for table, constraints, indexes
                 Ex: --translation=scname:public,vw_user,postgres,vw_user;iscname:public,vw_user,postgres,vw_user;rscname:public,vw_user,postgres,vw_user
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





## ---------------------------------------------------------------------------

## Supported Python drivers are:

   pyodbc                         # Module for ODBC

   or:

   ingresdbi                      # Module for Ingres transactional database
   cx_Oracle                      # Module for Oracle
   Sybase                         # Module for Sybase ASE
   pymssql                        # Module for MsSql
   MySQLdb                        # Module for Mysql
   psycopg2, psycopg2.extensions  # Postgres module
   import DB2                     # Module for DB2




## All drivers can be installed with the Python package manager "pip". 
## Below is old information about driver setup which might be eventually helpful
## in case of troubles

## ---------------------------------------------------------------------------



## INSTALL : cx_oracle
## Rem: deb http://oss.oracle.com/debian/ unstable main non free
## ---------------------------------------------------------------------------

# useradd -Goinstall plone
# python --version
  Python 2.6.4

# apt-get install python2.6-dev

# su - plone
# cat /etc/ld.so.conf.d/oracle.conf
   /opt2/oracle/product/11.2.0/dbhome_1/lib

# export ORACLE_HOME=/opkt2/oracle/product/11.2.0/dbhome_1
# easy_install cx_oracle
  ....
  Adding cx-Oracle 5.0.3 to easy-install.pth file
  Installed /usr/local/lib/python2.6/dist-packages/cx_Oracle-5.0.3-py2.6-linux-x86_64.egg
  ....

## Err: 
    import cx_oracle
    ImportError: No module named cx_oracle

## => check if library is registered
$ python
  >>> import sys
  >>> print sys.path
## Fix : import cx_Oracle
## Other posibilities : export PYTHONPATH or sys.path.append("<My library path>")

## ORACLE 
$ . oraenv
$ my-pythonscript.py 




## INSTALL : pymssql - SqlServer Interface
## Sur Windows utiliser pyodbc
## ---------------------------------------------------------------------------

# ap-get install freetds-common 
# apt-get install libsybdb5

# easy_install pymssql

   /usr/lib/python2.6/dist-packages/setuptools/package_index.py:156: UserWarning: Unbuilt egg for setuptools

  => # rm -rf /usr/lib/python2.6/dist-packages/setuptools.egg-info
     # apt-get install --reinstall python-setuptools

  error: Couldn't find a setup script in /tmp/easy_install-kGWH_P/pymssql-1.9.908.win32.zip

# apt-get install python-pymssql



## ---------------------------------------------------------------------------
## INSTALL : mysqldb - Mysql interface
## ---------------------------------------------------------------------------

apt-get install python-mysqldb python-mysqldb-dbg



## ---------------------------------------------------------------------------
## INSTALL : PyDB2 the DB2 interface
## ---------------------------------------------------------------------------

## Get : DB2_DSClient_V95_Linux_x86-64.tar    (Db2 connect install)
## Download : libstdc++5_3.3.6-18_amd64.deb   (Missing on UBUNTU 10)
## Got it on: http://packages.debian.org/lenny/amd64/libstdc++5/download


## Download : PyDB2_1.1.1-1.tar.gz
## from     : http://sourceforge.net/projects/pydb2



$ cd /depot

$ sudo dpkg -i libstdc++5_3.3.6-18_amd64.deb
$ mkdir db2
$ cd db2 
$ tar xf /depot/DB2_DSClient_V95_Linux_x86-64.tar
$ sudo ./db2_install -b /opt2/ibm/db2client/ -p CLIENT  # Need to be root to install in a destination diff than $HOME
$ sudo /opt2/ibm/db2client/instance/db2icrt omeignan    # May be run as user omeignan            


$ cd /depot
$ tar xzf PyDB2_1.1.1-1.tar.gz
$ cd /depot/PyDB2_1.1.1
 

## Setup db2 profile
$ sudo su

## Workaround for : /usr/bin/ld: cannot find -ldb2
# cd /opt2/ibm/db2client
# ln -s lib64 lib

## Compile + Install
# . /opt2/ibm/db2client/sqllib/db2profile
# python setup.py install                          ## Or python26 (when particular python26 installed)



## ---------------------------------------------------------------------------
## INSTALL : Other Python odbc driver
## ---------------------------------------------------------------------------

$ sudo apt-get install unixodbc
$ sudo apt-get install build-essential g++
$ sudo apt-get install python-dev
$ sudo apt-get install unixodbc-dev

$ cd /depot/pyodbc

$ wget http://pyodbc.googlecode.com/files/pyodbc-2.1.8.zip

    SHA1 Checksum: 3b2a066a609f920225987ed18dfcbed95704a4bf

$ unzip pyodbc-2.1.8.zip

$ cd pyodbc-2.1.8

$ python setup.py build

$ sudo python setup.py install



## ---------------------------------------------------------------------------
## INSTALL : Netezza ODBC API
## ---------------------------------------------------------------------------

$ wget ftp://ntzftp.netezza.com/Releases/4.5/linux64cli.package.tar

  username: xxxxx
  password: xxxxx

$ cd /depot
$ tar xf linux64cli.package.tar
$ ./unpack 
   -------------------------------------------------------------------------------
   Netezza Performance Server -- NPS Linux Client 4.5
   Copyright 2002-2008 Netezza Corporation.  All rights reserved.
   -------------------------------------------------------------------------------
   Validating package checksum ... ok 
   Where should the NPS Linux Client be unpacked? [/usr/local/nz] /opt2/netezza


$ cd; vi .odbc.ini

  [nz45]
  Driver                = /opt2/netezza/lib64/libnzodbc.so
  Description           = NetezzaSQL ODBC
  Servername            = my_server
  Port                  = 5480
  Database              = demodb
  ReadOnly              = false
  ShowSystemTables      = false
  LegacySQLTables       = false
  LoginTimeout          = 0
  QueryTimeout          = 0
  DateFormat            = 1
  NumericAsChar         = false
  SQLBitOneZero         = false
  StripCRLF             = false
  securityLevel         = preferredUnSecured
  caCertFile            =


$ sudo ODBCConfig&
$ DataManagerII&                  # Test



## ---------------------------------------------------------------------------
## INSTALL : tdodbc
## ---------------------------------------------------------------------------

## Download teradata clients from teradata site
## http://www.teradata.com/DownloadCenter/
## TTU 13.10 LINUX INDEP cliv2.13.10.00.03 
## TTU 13.10 LINUX-INDEP tdodbc.13.10.00.01   

$ tar xzf /media/disk320g/odbc/cliv2_LINUX_indep.13.10.00.03.tar.gz

## rpm -e cliv2   : If previous version already exists

$ mkdir td; cd td


$ tar odbc/cliv2_LINUX_indep.13.10.00.03.tar.gz

## Convert to Debian package
$ cd /depot/td/cliv2; sudo alien --scripts cliv2-13.10.00.03-1.noarch.rpm
$ cd /depot/td/tdciu; sudo alien --scripts tdicu-13.10.00.00-1.noarch.rpm
$ cd /depot/td/x8664/TeraGSS; sudo alien --scripts TeraGSS_suselinux-x8664-13.10.00.02-1.x86_64.rpm  ## 64bits !!

  error: incorrect format: unknown tag
  => Pass : see alien --verbose .. for details

## Install Teradata global security services
$ sudo dpkg -i teragss-suselinux-x8664_13.10.00.02-2_amd64.deb
   ...
   Output has been written to Binary file "/usr/teragss/suselinux-x8664/13.10.00.02/bin/../etc/tdgssconfig.bin" 

## Install "shared ICU libraries", or "Common Components for Internacionalisation for Teradata"
$ sudo dpkg -i tdicu_13.10.00.00-2_all.deb
   ...
   Adding TD_ICU_DATA environment variable to /etc/profile file.
   Adding TD_ICU_DATA environment variable to /etc/csh.login file.

## Install "Call Level Interface"
$ sudo dpkg -i cliv2_13.10.00.03-2_all.deb
....
  Adding cliv2 COPLIB environment variable to /etc/profile file.
  Adding cliv2 COPERR environment variable to /etc/profile file.
  Adding tdmst entry to /etc/services file.


$ tar xf tdodbc__LINUX_INDEP.13.10.00.01-1.tar.gz
$ tar xzf tdodbc__linux_x64.13.10.00.01.tar.gz
$ cd /depot/td/tdodbc; sudo alien --scripts tdodbc-13.10.00.01-1.noarch.rpm
   error: incorrect format: unknown tag

$ sudo dpkg -i tdodbc_13.10.00.01-2_all.deb
  ...
  Warning : ODBC Driver for Teradata requires TeraGSS_suselinux-x8664     => Already installed
  ....


## Test
$ cd $HOME; vi .odbc.ini
   [td13r03]
   Driver=/opt/teradata/client/ODBC_64/lib/tdata.so
   Description=Teradata V13 Release 03
   DBCName=the_teradata_host

$ DataManagerII&                  # or : "isql td13r03 user pass" User/pass MUST BE parameters




## ---------------------------------------------------------------------------
## INSTALL : MAXDB Python API
## Software available at : http://www.sdn.sap.com/irj/scn/maxdb-downloads
## ---------------------------------------------------------------------------

## Install on Ubuntu 10.04.1 LTS (Linux orange 2.6.32-25-generic #45-Ubuntu SMP)
$ wget http://http.us.debian.org/debian/pool/main/g/gcc-3.3/libstdc++5_3.3.6-20_i386.deb
$ wget http://mirror.pnl.gov/ubuntu//pool/universe/w/wxwidgets2.6/libwxgtk2.6-0_2.6.3.2.2-2ubuntu4_i386.deb

$ sudo dpkg -i libstdc++5_3.3.6-20_i386.deb
$ sudo apt-cache search  libwxbase2
$ sudo dpkg -i libwxgtk2.6-0_2.6.3.2.2-2ubuntu4_i386.deb
$ sudo apt-get install libpng3

$ ls -l /lib/libpng12.so.0
   lrwxrwxrwx 1 root root 18 2010-09-28 10:34 /lib/libpng12.so.0 -> libpng12.so.0.42.0
$ file /usr/lib/libpng.so.3
    /usr/lib/libpng.so.3: broken symbolic link to `libpng12.so.0'

  => $ sudo rm /usr/lib/libpng.so.3
     $ sudo ln -s /lib/libpng12.so.0.42.0 /usr/lib/libpng.so.3

$ sudo ln -s /usr/lib/libtiff.so.4 /usr/lib/libtiff.so.3


$ cd /downloads/MAXDB_LINUX_x86
$ sudo ./SDBSETUP


## or

## extract software and lookup directories for package : SDBODBC.TGZ 
  

## Extract SDBODBC.TGZ to installation directory
$ cd <installation directory>  
$ tar xf <Extract directory>/SDBODBC.TGZ    




## Python lib cannot be loaded. Suspect a problem of python version. Driver=2.3, Python = 2.6
## Traceback (most recent call last):
##  File "./test.py", line 5, in <module>
##    import sdb.sql
## ImportError: /opt2/sapdb/programs/lib/python2.3/sdb/sqlmodule.so: undefined symbol: PyUnicodeUCS2_FromUnicode

$ ./sqlcli -n db_host -d cms77mx1 -u DEMO,PDEMO
$ cat $HOME/.odbc.ini
  [MaxDB]
  Driver          = /opt2/sapdb/programs/lib/libsdbodbc.so
  Description     = SAP DB 7.4 DATABASE
  ServerDB        = demodb
  ServerNode      = db_host


## Usernames/passwords are UPPERCASE
$ isql -v MaxDB DEMO DEMO
  ISQL>




## ---------------------------------------------------------------------------
## INSTALL : SAP DB Python API
## Can be used for HANA
## ---------------------------------------------------------------------------




## Copy the following file from an existing database installation.

/opt2/hana/python_support/libicudata.so.34
/opt2/hana/python_support/libirc.so
/opt2/hana/python_support/pyhdbcli.so
/opt2/hana/python_support/libicuuc.so.34
/opt2/hana/python_support/libicui18n.so.34
/opt2/hana/python_support/libsapcpp45.so
/opt2/hana/python_support/libSQLDBCHDB.so

/opt2/hana/python_support/hdbcli                         ## The python package
/opt2/hana/python_support/hdbcli/__init__.py
/opt2/hana/python_support/hdbcli/dbapi.py
/opt2/hana/python_support/hdbcli/resultrow.py
/opt2/hana/python_support/hdbcli/hdbcli.vcom

## The package definition
$ cat /opt2/hana/python_support/hdbcli/__init__.py
  #!/usr/bin/env python 
  #
  # Copyright 2010 SAP AG
  #
  # Python DB API v2.0
  # NewDB client library
   __all__ = [
    'dbapi',
    ]




export LD_LIBRARY_PATH=/opt2/hana/python_support           ## Where to find *.so files
export PYTHONPATH=/opt2/hana/python_support                ## Where to find hdbcli




## ---------------------------------------------------------------------------
## INSTALL : SYBASE SqlAnywhere API
## Python "ctypes" module is required
## CTypes - A package for calling the functions of dlls/shared libraries. Now included with Python 2.5 and up. 
## easy_install or http://sourceforge.net/projects/ctypes/files/ctypes/
## ---------------------------------------------------------------------------

## Install SqlAnywhere Api
$ wget http://download.sybase.com/eval/saclient/sa12_client_linux_x86.x64.1201_3152_l10n.tar.gz
$ tar xzf sa12_client_linux_x86.x64.1201_3152_l10n.tar.gz
$ cd client1201
$ sudo mkdir -p /opt2/sqlanywhere/12
$ sudo chown omeignan:omeignan /opt2/sqlanywhere/12
$ ./setup -silent -I_accept_the_license_agreement -install sqlanyclnt_standalone64 -sqlany-dir /opt2/sqlanywhere/12
$ . /opt2/sqlanywhere/12/bin64/sa_config.sh


## Install python interface
$ wget http://sqlanydb.googlecode.com/files/sqlanydb-1.0.2.tar.gz
$ tar xzf sqlanydb-1.0.2.tar.gz
$ cd sqlanydb/
$ python setup.py build
$ sudo python setup.py install

$ python
  >>> import sqlanydb




## ---------------------------------------------------------------------------
## INSTALL : SYBASE Ase API
## ---------------------------------------------------------------------------

## Download: http://python-sybase.sourceforge.net/python-sybase-0.40pre1.tar.gz  (latest realease)
## To: /depot/sybase
## Install sybase client to /opt2/sybase/15.0.3 (open connectivity)

$ cd /opt2/sybase/15.0.3
$ . ./ASE150.sh

$ sudo apt-get install python-distutils-extra

##$ sudo apt-get install devscripts (not needed)

$ cd /depot/sybase 
$ tar xf python-sybase-0.40pre1.tar.gz
$ cd python-sybase-0.40pre1/


## Other possibilities : 
##    python setup.py build_ext -D HAVE_FREETDS -U WANT_BULKCOPY                      (With Threads and bulk copy)
##    python setup.py build_ext -D WANT_THREADS -D HAVE_FREETDS -U WANT_BULKCOPY      (With Thread, Freetds, Bulk copy)
                       
$ python setup.py build_ext -D WANT_THREADS


     /usr/bin/ld: skipping incompatible /opt2/sybase/15.0.3/OCS-15_0/lib/libsybunic.so when searching for -lsybunic
     /usr/bin/ld: cannot find -lsybunic
     collect2: ld returned 1 exit status
     error: command 'gcc' failed with exit status 1

   => 
      $ mv /opt2/sybase/15.0.3/OCS-15_0/lib/libsybunic.so /opt2/sybase/15.0.3/OCS-15_0/lib/libsybunic.so.old
      $ ls /opt2/sybase/15.0.3/OCS-15_0/lib/libsybunic*
             /opt2/sybase/15.0.3/OCS-15_0/lib/libsybunic64.a  
             /opt2/sybase/15.0.3/OCS-15_0/lib/libsybunic64.so  
             /opt2/sybase/15.0.3/OCS-15_0/lib/libsybunic.so.old
      $ ln -s /opt2/sybase/15.0.3/OCS-15_0/lib/libsybunic64.so /opt2/sybase/15.0.3/OCS-15_0/lib/libsybunic.so

   => $ python setup.py build_ext -D WANT_THREADS   ## = OK

$ sudo -E python setup.py install

## Test$ 

$ .  /opt2/sybase/15.0.3/ASE150.sh

$ vi $SYBASE/interfaces
    ase15
        query tcp ether db_host 65000


$ python
  >>> import Sybase
  ...
   Using locale name "en_US.utf8" defined in environment variable LANG
   Locale name "en_US.utf8" doesn't exist in your /opt2/sybase/15.0.3/locales/locales.dat file
   ....
   ## => Add en_US.utf8 into locales.dat file at linux location.

  >>> import Sybase 
  >>>db=Sybase.connect(dsn='ase15', user='club', passwd='CLUB')

   


## ---------------------------------------------------------------------------
## INSTALL : INGRES Python API	
## Download python driver : ingresdbi-2.0.0.zip from www.ingres.com
## ---------------------------------------------------------------------------

## Install python Net.


$ cd /depot/Ingres
$ tar xf ingres-9.2.0-143-NPTL-com-linux-x86_64.tgz
$ cd /depot/Ingres/ingres-9.2.0-143-NPTL-com-linux-ingbuild-x86_64


## Installation fails conversion to debian package
## $ fakeroot alien --verbose --scripts ingres-net-9.2.0-143.x86_64.rpm


$ export TMPDIR=/tmp
$ export II_DISTRIBUTION=/depot/Ingres/ingres-9.2.0-143-NPTL-com-linux-ingbuild-x86_64/ingres.tar
$ export TERM_INGRES=vt100fx
$ export II_INSTALLATION=I0
$ export II_SYSTEM=/opt2/ingres/9.2.0-143
$ export LD_LIBRARY_PATH=$II_SYSTEM/ingres/lib:$LD_LIBRARY_PATH

   ## If library path not configured

$ PATH=$II_SYSTEM/ingres/bin:$II_SYSTEM/ingres/utility:$PATH

$ sudo mkdir -p $II_SYSTEM/ingres
$ sudo chown -R omeignan:omeignan $II_SYSTEM

$ cd $II_SYSTEM/ingres
$ tar xf $II_DISTRIBUTION install
$ cd install 

## List products
$ ./ingbuild -products
	....       
	dbmsnet           Ingres Networked DBMS server  
	dbmsstar          Ingres Networked DBMS server w
	standalone        Ingres Stand-alone DBMS server
	net               Ingres Networking 
        ....
     
$ ./ingbuild -version
  II 9.2.0 (a64.lnx/143)NPTL


## Create default file : ingrsp.rsp
## Can be use to check or to add install parameters
./ingbuild -install="net" -acceptlicense -mkresponse

$ cat ingrsp.rsp

  II_SYSTEM=/opt2/ingres/9.2.0-143
  II_DISTRIBUTION=/depot/Ingres/ingres-9.2.0-143-NPTL-com-linux-ingbuild-x86_64/ingres.tar
  II_MSGDIR=/opt2/ingres/9.2.0-143/ingres/install
  II_CONFIG=/opt2/ingres/9.2.0-143/ingres/install
  TERM_INGRES=linux


$ ./ingbuild  -acceptlicense  -express -install="net,odbc"  $II_DISTRIBUTION 

$ ingsetenv II_INSTALLATION VT        ## Setup environnement
$ ingunset  II_CHARSETII              ## Unset unncessary variables


## Setup variables (ex: II_INSTALLATION=VT)
$ ingsetenv II_CHARSETVT      UTF8             ## If Vectorwise
$ ingsetenv II_TIMEZONE_NAME EUROPE-CENTRAL

$ ingstart


## Usefull if system identification is required ??
## Need to be verified
# $II_SYSTEM/ingres/bin/mkvalidpw
  ....
  chown: invalid user: `ingres'   ## => See bug below

## Secure script
# chmod 4711 $II_SYSTEM/ingres/bin/ingvalidpw

## 
## BUG :
## tail $II_SYSTEM/ingres/bin/mkvalidpw 
##   ...
##   chown ingres $II_SYSTEM/ingres/files/symbol.tbl


## Start name server (gcn)
$ ingstart -iigcn

    Starting the Name Server...

## REM : 
## ingstop -iigcn          => Does not clearly clean up symbol.tbl
## strace ingstart -iigcn  => Hang
## stat("/opt2/ingres/9.2.0-143/ingres/files/symbol.tbl", {st_mode=S_IFREG|0644, st_size=256, ...}) = 0
## futex(0x7f38bd5e4c18, FUTEX_WAIT_PRIVATE, 2, NULL
## => rm /opt2/ingres/9.2.0-143/ingres/files/symbol.tbl

## Port used by be GCN 
$ cat $II_SYSTEM/files/symbol.tbl | grep -i port
 II_GCNI0_PORT	51252                              


## Configure and start global communication server (gcc)
$ netutil -file-
  create global connection <mycomputername> 127.0.0.1 tcp_ip I0
  
$ ingstart -iigcc

  Starting Net Server (default)...
  GCC Server = 38750
    TCP_IP port = I0 (21376)

$ ps -ef 
  ...
  /opt2/ingres/9.2.0-143/ingres/bin/iigcn I0 gcn
  /opt2/ingres/9.2.0-143/ingres/bin/iigcc I0 gcc
  ... 



## Test
$ sql "@db_host,tcp_ip,II;connection_type=direct[csdev1,csdev1]::csdev1"


## Install python interface
$ unzip ingresdbi-2.0.0.zip      ## Rpm version
$ cd ingresdbi-2.0.0/


## Note for Microsoft Windows: Make sure the Microsoft Visual Tools environment is set up before this step. 
$ python setup.py build

## Test
# python setup.py install


$ python 
  >>> import ingresdbi
  >>> dc=ingresdbi.connect(database='csdev1',vnode='@db_host,tcp_ip,II',uid='csdev1',pwd='csdev1')



## Automatic installation
$ wget http://esd.ingres.com/product/drivers/Python_DBI/Source_Code/Ingres_Python_DBI_Driver/ingresdbi-2.0.0.zip
$ easy_install http://esd.ingres.com/product/drivers/Python_DBI/Source_Code/Ingres_Python_DBI_Driver/ingresdbi-2.0.0.zip






