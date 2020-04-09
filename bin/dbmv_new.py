#!/usr/bin/env python
# -*- coding: utf-8 -*

# Copyright 2018 - 2019 Actian Corporation

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#      http://www.apache.org/licenses/LICENSE-2.0

#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##
##    History
##    cooda09	28-01-19	Modifications for --mapping option
##    bolke01/  03-09-19    Update to manage structure/partition counts
##    cooda09               Added Netezza Source
##                          Added MySQL Source
##                          Added Zen Source
##    bolke01  05-09-19     Added --pagesize option for HEAP tables
##                          (ingres and actianx only dest)
##                          Added parameters vwload and cpvwl for improved
##                          data load performance.
##    bolke01  08-09-19     reverted True to False for required setting of 
##                          src and dest parameters. they are required either
##                          in a prameter file or on the command line.
##    bolke01  10-09-19     Added quiet option (default)
##    bolke01  10-09-19     Added verbose option (default)
##    cooda09  31-03-20     Added filetag option 
##

import getopt
import os
import re
import sys
import argparse
import logging
import logging.config
import json

from conversionParams import ConversionParameters
from schemaConvertor import SchemaConvertor

class ErrorFilter(logging.Filter):
    """Filters log messages and decides whether to allow and stop ones"""
    def __init__(self, param=None):
        super(ErrorFilter, self).__init__()
        self.param = param

    def filter(self, record):
        if self.param is None:
            allow = True
        else:
            if self.param in record.msg:
                record.msg = record.msg.replace('noshow', '', 1)
                allow = False
            else:
                allow = True
        return allow


def setup_logging(default_path='logging.json', env_key='LOG_CFG'):
    """Setup logging configuration"""
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        raise ValueError("No logging configuration file is found in {0}".format(path))


def main():
    reload(sys)
    sys.setdefaultencoding('utf-8')
    logging_conf_file = "{0}/../etc/logging.json".format(os.path.dirname(__file__))
    setup_logging(default_path=logging_conf_file)

    # Program parameters
    g_pars = ['src=', 'dest=', 'loadata', 'cretab', 'creview', 'dmpobj', 'creall', 'crecmnt', 'loaddl', 
              'loadtest', 'batchsize=', 'maxrows=',
              'truncate', 'parfile=', 'fdelim=', 'unload', 'translation=', 'quote=', 'cmdsep=', 'charmax=',
              'creindex', 'ownsrc=', 'owntgt=', 'add_drop',
              'on_error=', 'source_schema=', 'target_schema=', 'unsupported=', 'exclude=',
              'include=', 'tables=', 'insertmode=', 'trial', 'loadmethod=', 'threads=', 'mapping', '--help',
              'partition=', 'structure=', 'pagesize=', 'cpvwl', 'vwload' , 'quiet', 'verbose', 'filetag=']

    parser = argparse.ArgumentParser()
    parser.add_argument('--src', required=False, action="store",
                        help="source DB used as a data source for migration, copy, etc.")
    parser.add_argument('--dest', required=False, action="store",
                        help="destination DB used as a destination for migration, copy, etc.")
    parser.add_argument('--loadata', required=False, action="store_true",
                        help="indicates whether to load or not the actual tables data from @source to @dest")
    parser.add_argument('--cretab', required=False, action="store_true",
                        help="create tables in the @dest from @source schema")
    parser.add_argument('--creview', required=False, action="store_true",
                        help="create views in a @dest from @source schema")
    parser.add_argument('--dmpobj', required=False, action="store_true",
                        help="create procedures in a @dest from @source schema")
    parser.add_argument('--creall', required=False, action="store_true",
                        help="create tables, views, constrains in a @dest from @source")
    parser.add_argument('--crecmnt', required=False, action="store_true",
                        help="create column comment details in a @dest from @source")
    parser.add_argument('--loaddl', required=False, action="store_true",
                        help="load DDL from @source to @dest")
    parser.add_argument('--loadtest', required=False, action="store_true",
                        help="perform predefined INSERT queries to @dest (used for performance testing)")
    parser.add_argument('--batchsize', required=False, action="store",
                        help="set custom batchsize for INSERT queries (default: 500, min: 1, max: 10000)")
    parser.add_argument('--maxrows', required=False, action="store",
                        help="set TOTAL limit for INSERT queries (default: 100000, min: 1, max: 100000)")
    parser.add_argument('--truncate', required=False, action="store_true",
                        help="remove existing rows from @dest table")
    parser.add_argument('--parfile', required=False, action="store",
                        help="file with all parameters inside can be stored is a parfile. Parfiles examples can be" +
                             " find in the 'wrk' directory.")
    parser.add_argument('--fdelim', required=False, action="store",
                        help="sets field delimiter for data/stmts written to file (default: \\t)")
    parser.add_argument('--unload', required=False, action="store_true",
                        help="unload tables structure from @src to file as CREATE statements")
    parser.add_argument('--translation', required=False, action="store",
                        help="use translation table/constrains/indexes rules in @dest")
    parser.add_argument('--quote', required=False, action="store",
                        help="use this symbol as quotation symbol for DDL")
    parser.add_argument('--cmdsep', required=False, action="store",
                        help="Command separator (default=';') used in DDL file")
    parser.add_argument('--charmax', required=False, action="store",
                        help="Max columns precision that is acceptable for translation (default: 6400)")
    parser.add_argument('--creindex', required=False, action="store_true",
                        help="create indexes in @dest", default=False)
    parser.add_argument('--ownsrc', required=False, action="store",
                        help="single source owner")
    parser.add_argument('--owntgt', required=False, action="store",
                        help="new target owner")
    parser.add_argument('--add_drop', required=False, action="store_true",
                        help="drop table at the beginning")
    parser.add_argument('--on_error', required=False, action="store",
                        help="<continue> continue or abort when error occurs")
    parser.add_argument('--source_schema', required=False, action="store",
                        help="specify @src schema to process")
    parser.add_argument('--target_schema', required=False, action="store",
                        help="convert @source_schema to this schema")
    parser.add_argument('--unsupported', required=False, action="store_true",
                        help="whether to skip unsupported types or not")
    parser.add_argument('--exclude', required=False, action="store",
                        help="exclude specific table/columns from processing")
    parser.add_argument('--include', required=False, action="store",
                        help="include only specific tables/columns for processing")
    parser.add_argument('--insertmode', required=False, action="store",
                        help="([default]bulk, row) row - insert data row-by-row, ignoring batchsize, "
                             "bulk - insert data in chunks (buffered insertion)")
    parser.add_argument('--trial', required=False, action="store_true",
                        help="useful for testing (trial) mode", default=False)
    parser.add_argument('--loadmethod', required=False, action="store",
                        help="use multiple threads for data loading ([default]serial=1, parallel=4,"
                             " multitable=CPU dependent)")
    parser.add_argument('--threads', required=False, action="store",
                        help="specifies custom number of thread to use (default: 1, min: 1,"
                             " max: CPU dependent but <= 32)")

    parser.add_argument('--mapping', required=False, action="store_true",
                        help="Display SRC to DEST variable assignment mapping")

    parser.add_argument('--partition', required=False, action="store",
                        help="For Partitioned table storage use this partition count. Note avalanche always uses 'DEFAULT'")

    parser.add_argument('--structure', required=False, action="store",
                        help="Target Storage structure of tables - HEAP or X100")

    parser.add_argument('--pagesize', required=False, action="store",
                        help="Target Storage size of HEAP tables (8192,16384,32768,65536) Def=8192 or installation default")

    parser.add_argument('--vwload', required=False, action="store_true",
                        help="Use vwload at command line to load CSV files using --cmdsep - vwload -t tab [<opts>] dbname file_list" )

    parser.add_argument('--cpvwl', required=False, action="store_true",
                        help="Use vwload at SQL level to load CSV files using --cmdsep - COPY tab() VWLOAD FROM 'csvlist <with opts>'")
    parser.add_argument('--quiet', required=False, action="store_true",
                        help="No output to console")
    parser.add_argument('--verbose', required=False, action="store_true",
                        help="Output to console - may cause embedded usage to show errors or warnings.")
    parser.add_argument('--filetag', required=False, action="store",
                        help="Filetag to be included in output file name, default is date and time.")

    if not sys.argv[1:]:
        # Print usage and exit
        parser.print_help()
        sys.exit(1)

    # check if args are correct
    parser.parse_args()

    try:
        opts, args = getopt.getopt(sys.argv[1:], '', g_pars)
    except getopt.GetoptError as err:
        print str(err)
        for s in g_pars:
            print s
        sys.exit(2)
##
##
    print 'Version 0.9f'
    print '# Copyright 2018 - 2020 Actian Corporation'
##

    params = ConversionParameters()
    params.parse_arguments(opts)
    set_paths(params)

    # delete files from previous run
    purge(".", params.program_name + '_\w+.txt')

    convertor = SchemaConvertor(params)
    convertor.convert()


def set_paths(params):
    bin_dir = os.path.dirname(__file__)
    bin_dir = "." if bin_dir == "" else bin_dir
    params.bin_dir_path = bin_dir

    # The short name of the script (__file__= sys.argv[0])
    params.program_name = os.path.basename(__file__).split('.')[0]
    # The path to application xml configuration file
    params.xml_path = "%s/../etc/%s.xml" % (bin_dir, params.program_name)


def purge(dir, pattern):
    for f in os.listdir(dir):
        if re.search(pattern, f):
            os.remove(os.path.join(dir, f))


if __name__ == "__main__":
    main()
