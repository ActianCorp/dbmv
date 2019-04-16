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
#
#    History
#    cooda09    28-01-19        Use of --mapping flag and default option


import multiprocessing
import re
import sys
import logging
from copy import deepcopy


class ConversionParameters:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._bin_dir_path = ""
        self._program_name = ""
        self._xml_path = ""

        # translation table for names
        self.translation_table_names = {}

        # translation table for datatypes
        self.translation_table_datatypes = {}

        self.loadata = False
        self.cretab = False
        self.creall = False
        self.creview = False
        self.creindex = False

        self.add_drop = False
        self.loaddl = False
        self.unload = False
        self.fdelim = "\t"
        self.src = None
        self.dest = None
        self.quote = None
        self.command_separator = ';'
        self.index_separator = ''
        self.maxrows = 100000
        self.batchsize = 500
        self.charmax = 6400
        self.loadtest = False
        self.truncate = False
        self.translation = None
        self.mapping = False

        # Run the script through ignoring all errors
        self.continue_on_error = False
        # only select the tables for a given schema
        self.source_schema = ''
        # change the schema name to this one
        # self.target_schema = None
        self.target_schema = ''
        # do not migrate columns of data we cannot handle
        self.skip_unsupported = False
        # warn about columns of data we cannot handle
        self.warn_unsupported = True
        # do not migrate tables listed
        self.exclude_tables = []
        # do not migrate columns listed
        self.exclude_columns = []
        # migrate tables listed
        self.include_tables = []
        # migrate columns listed
        self.include_columns = []
        # values - 'bulk'/'row'
        self.insert_mode = 'BULK'
        self.trial = False
        # values (serial|parallel|multitable)
        self.loadmethod = 'serial'
        # number of threads for data loading
        self.threads = 1
        # regex to match the table
        self.table_regexp = re.compile(r"^\w+$")
        # regex to match the table.column
        self.table_column_regexp = re.compile(r"^\w+\.\w+$")

    @property
    def bin_dir_path(self):
        return self._bin_dir_path

    @property
    def program_name(self):
        return self._program_name

    @property
    def xml_path(self):
        return self._xml_path

    def parse_arguments(self, arguments):
        for opt, arg in arguments:
            if opt == "--src":
                val = arg.strip()
                if not val:
                    self.logger.error("--src value can't be empty")
                    sys.exit(1)
                self.src = val
            elif opt == "--dest":
                val = arg.strip()
                if not val:
                    self.logger.error("--dest value can't be empty")
                    sys.exit(1)
                self.dest = val
            elif opt == "--quote": self.quote = arg.strip()
            elif opt == "--cmdsep": self.command_separator = arg.strip()
            elif opt == "--loadata":
                self.loadata = True
            elif opt == "--cretab": self.cretab = True
            elif opt == "--creindex": self.creindex = True
            elif opt == "--add_drop": self.add_drop = True
            elif opt == "--creall": self.creall = True
            elif opt == "--creview": self.creview = True
            elif opt == "--unload":
                # unload and loadtest are exclusive
                self.loadtest = False
                self.loaddl = False
                self.unload = True
            elif opt == "--fdelim": self.fdelim = arg.strip()
            elif opt == "--loaddl": self.loaddl = True
            elif opt == "--maxrows":
                val = arg.strip()
                self.maxrows = int(val) if val.isdigit else -1
                if self.maxrows < 1 or self.maxrows > 1000000:
                    self.logger.error("'{0}' is not a valid 'maxrows' value. Valid values are [1..1000000].".format(val))
                    sys.exit(1)
            elif opt == "--batchsize":
                val = arg.strip()
                self.batchsize = int(val) if val.isdigit else -1
                if self.batchsize < 1 or self.batchsize > 10000:
                    self.logger.error("'{0}' is not a valid 'batchsize' value. Valid values are [1..10000]."
                                      .format(val))
                    sys.exit(1)
            elif opt == "--charmax":
                val = arg.strip()
                self.charmax = int(val) if val.isdigit else -1
                if self.charmax < 1:
                    self.logger.error("'{0}' is not a valid 'charmax' value. Positive decimal value is valid."
                                      .format(val))
                    sys.exit(1)
            elif opt == "--ownsrc": self.ownsrc = arg.strip()
            elif opt == "--owntgt": self.owntgt = arg.strip()
            elif opt == "--loadtest":
                # loadtest and unload are exclusive
                self.unload = False
                self.loadtest = True
            elif opt == "--truncate": self.truncate = True
            elif opt == "--on_error":
                val = arg.strip()
                if val == 'continue':
                    self.continue_on_error = True
                else:
                    self.logger.warn("--on_error value is not valid. Ignoring --on_error setting")
            elif opt == "--source_schema":
                val = arg.strip()
                if not val:
                    self.logger.error("--source_schema value can't be empty")
                    sys.exit(1)
                self.source_schema = val
            elif opt == "--target_schema":
                val = arg.strip()
                if not val:
                    self.logger.error("--target_schema value can't be empty")
                    sys.exit(1)
                self.target_schema = val
            elif opt == "--unsupported": self.skip_unsupported = arg.strip() == 'skip'
            elif opt == "--exclude":
                val = arg.strip()
                if not val:
                    self.logger.error("--exclude value can't be empty")
                    sys.exit(1)
                for i in map(str.strip, val.split(',')):
                    if self.table_regexp.match(i):
                        self.logger.debug("--exclude: {0} detected as table specification".format(i))
                        self.exclude_tables.append(i)
                    elif self.table_column_regexp.match(i):
                        self.logger.debug("--exclude: {0} detected as table.column specification".format(i))
                        self.exclude_columns.append(i)
                    else:
                        self.logger.warn("The --exclude value: '{0}' doesn't match nether table nor table.column"
                                         " regexp. Skipping...".format(i))
            elif opt == "--include":
                val = arg.strip()
                if not val:
                    self.logger.error("--include value can't be empty")
                    sys.exit(1)
                for i in map(str.strip, val.split(',')):
                    if self.table_regexp.match(i):
                        self.logger.debug("--include: {0} detected as table specification".format(i))
                        self.include_tables.append(i)
                    elif self.table_column_regexp.match(i):
                        self.logger.debug("--include: {0} detected as table.column specification".format(i))
                        self.include_columns.append(i)
                    else:
                        self.logger.warn("The --include value: '{0}' doesn't match nether table nor table.column"
                                         " regexp. Skipping...".format(i))
            elif opt == "--insertmode":
                val = arg.strip()
                self.insert_mode = val.upper()
                if self.insert_mode not in ("BULK", "ROW"):
                    self.logger.error("'{0}' is not a valid 'insert_mode' value. Valid values are [BULK, ROW]."
                                      .format(val))
                    sys.exit(1)
            elif opt == "--trial": self.trial = True
            elif opt == "--loadmethod":
                self.loadmethod = arg.strip().lower()
                if self.loadmethod == 'serial':
                    self.threads = 1
                elif self.loadmethod == 'parallel':
                    self.threads = 4
                elif self.loadmethod == 'multitable':
                    self.threads = multiprocessing.cpu_count()
                else:
                    self.logger.error("'{0}' is not a valid '--loadmethod' value. Valid values are "
                                      "[serial, parallel, multitable].".format(self.loadmethod))
                    sys.exit(1)
            elif opt == "--threads":
                val = arg.strip()
                # 10 if cores < number_of_cores, but upper bound is 32
                thread_limit = min(max(multiprocessing.cpu_count(), 10), 32)
                self.threads = int(val) if val.isdigit else -1
                if self.threads < 1 or self.threads > thread_limit:
                    self.logger.error("'{0}' is not a valid '--threads' value. Valid values are [1..{1}]"
                                      .format(val, thread_limit))
                    sys.exit(1)
            elif opt == "--xx": self.xx = arg.strip()
            elif opt == "--mapping" :self.mapping = True
            elif opt == "--parfile":
                fname = arg
                if fname != "":
                    for line in open(fname, 'r'):
                        if re.match("^--.*=.*", line):
                            (param, value) = line.split('=')
                            param = param.strip()
                            value = value.strip()
                            if param == "--src":
                                self.src = value
                            elif param == "--dest":
                                self.dest = value
                            elif param == "--quote":
                                self.quote = value
                            elif param == "--cmdsep":
                                self.command_separator = value
                            elif param == "--fdelim":
                                self.fdelim = value
                            elif param == "--translation":
                                self.translation = value
                                self.init_translation_table_names(value)
                            elif param == "--owntgt":
                                self.owntgt = value
                            elif param == "--ownsrc":
                                self.ownsrc = value
            else:
                assert False, "unhandled option"

        if self.__is_include_exclude_conflict():
            self.logger.error("--include&--exclude values conflict detected. exit...")
            sys.exit(1)
        self.set_index_separator()

    def __is_include_exclude_conflict(self):
        """find conflict between --include&--exclude values"""
        if self.include_tables and self.exclude_tables:
            conflicting_tables = set(self.include_tables).intersection(self.exclude_tables)
            if conflicting_tables:
                self.logger.error("Ambiguous values for --include & --exclude tables. Tables are: {0}"
                                  .format(conflicting_tables))
                return True
        if self.include_columns and self.exclude_columns:
            conflicting_columns = set(self.include_columns).intersection(self.exclude_columns)
            if conflicting_columns:
                self.logger.error("Ambiguous values for --include & --exclude table.columns. Columns are: {0}"
                                  .format(conflicting_columns))
                return True
        if self.exclude_tables and self.include_columns:
            conflicting_values = list(filter(lambda e: any(i.startswith(e) for i in self.include_columns),
                                             self.exclude_tables))
            self.logger.error("Ambiguous values for --exclude table & --include table.column. Tables are: {0}"
                              .format(conflicting_values))
            return True

    # ex: init_g_trnm
    def init_translation_table_names(self, p_s):
        """ Populate _translation_table_names which is a 2 dimension array built with values of parameter --translation
            Example :  --translation=scname:dbo,demo,test,toto;iscname:dbo,demo2
                => _translation_table_names['scname']['dbo']  = demo
                    _translation_table_names['scname']['test'] = toto
                    _translation_table_names['iscname']['dbo'] = demo2
                => iscname, scname are the column names in the header of an object description query
        """
        for line in p_s.split(';'):
            h2 = {}
            t1 = line.split(':')
            key1 = t1[0].strip()
            values_key1 = t1[1]
            t2 = values_key1.split(',')
            for i in range(len(t2)/2):
                key2 = t2[i*2].strip()
                value_key2 = t2[i*2+1].strip()
                h2[key2] = value_key2
            self.translation_table_names[key1] = deepcopy(h2)

    # ex: tr
    def get_translation_name(self, p_key1, p_key2):
        """ Function which translate tuple (key1, key2) with values inserted in dictionnary _translation_table_names
            Input : key1, key2
            if datum is not found key2 is returned.
        """
        rc = p_key2
        if self.translation_table_names.has_key(p_key1) and self.translation_table_names[p_key1].has_key(p_key2):
            rc = self.translation_table_names[p_key1][p_key2]
        return rc

    def get_target_schema(self, schema):
        if self.target_schema is None:
            result = self.get_translation_name('scname', schema)
        else:
            result = self.target_schema
        return result

    def set_index_separator(self):
        dest_dbtype = self.dest.split(':')[0].split('-')[0]
        if dest_dbtype == "vector": 
            self.index_separator = "_x100_"
        if dest_dbtype == "ingres": 
            self.index_separator = "_ii_"
        elif dest_dbtype == "vectorh": 
            self.index_separator = "_x100_"
        else:
            self.index_separator = "_ax11_"
