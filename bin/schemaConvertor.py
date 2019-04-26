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
#    cooda09    28-01-19       Use of --mapping flag and change to precision processing
#    cooda09    11-02-19       Schema owner of 'ontime' was hardcoded.
#                              Substitute processing was giving an error
#    cooda09    17-04-19       Further fixes in definitions created for 
#                              creating of vector views

import codecs
import sys
import time
import logging
import warnings
import xml.dom.minidom
from string import Template
from xml.dom.minidom import Node

from concurrent.futures import ThreadPoolExecutor, wait
from multiprocessing import RawValue
from threading import Lock
import typesMapping
from driverTools import dbconnector


class Counter(object):
    def __init__(self, value=0):
        # RawValue because we don't need it to create a Lock:
        self.val = RawValue('i', value)
        self.lock = Lock()

    def add(self, value):
        with self.lock:
            self.val.value += value

    def get(self):
        with self.lock:
            return self.val.value


class ConvertorUtil:
    def __init__(self, params, xml_path):
        self.params = params
        self.xml_path = xml_path
        self.inserted_queries_number = Counter()
        self.logger = logging.getLogger(__name__)
        self.connectors = []

    def get_xml_data(self, dbtype, sql, identifier):
        """
            Get SQL definition from XML file. SQL is retrieved by using keywords passed as function parameters
            @:param dbtype Database type "mysql, mssql, ..."
            @:param sql Sqltype : Select, Create, ...
            @:param identifier Unique Identifier
        """
        result = ""
        xmldoc = xml.dom.minidom.parse(self.xml_path)
        for node in xmldoc.getElementsByTagName(dbtype)[0].getElementsByTagName(sql):
            if node.getAttribute("id") == identifier:
                for child in node.childNodes:
                    if child.nodeType == Node.TEXT_NODE:
                        result = child.data
                        break
        return result

    def strip_row(self, row):
        """
            Strip values of a row and try to return encoded unicode string.
            If failed return ascii format
            @:param row
        """
        result = []
        for v in row:
            if type(v) == str:
                v = v.strip()
                try:
                    v = v.decode('utf_8')
                except UnicodeDecodeError:
                    self.handle_error()
            result.append(v)
        return result

    def quote(self, param):
        """
            Add Quote or not according to value of quote parameter.
            @:param str
        """
        result = param
        if self.params.quote is not None:
            result = self.params.quote + result + self.params.quote
        return result

    def write_txt_file(self, file_suffix, data):
        filename = self.params.program_name + '_' + file_suffix + '.txt'
        with codecs.open(filename, encoding='utf-8', mode='w+') as f:
            f.writelines(data)

    def handle_error(self, ex=None):
        self.logger.error(ex)
        self.logger.exception('noshow' + repr(ex))  # do not show exceptions (see logging conf file)
        if not self.params.continue_on_error:
            sys.exit(2)

    def is_included(self, table, col):
        """
        :param table: table to check
        :param col: column tot check
        :return: true if table.column should be processed False otherwise
        """
        result = False
        full_col_name = "{0}.{1}".format(table, col).upper()
        table = table.upper()
        if self.params.include_tables or self.params.include_columns:
            if table in [x.upper() for x in self.params.include_tables]:
                if full_col_name in [x.upper() for x in self.params.exclude_columns]:
                    self.logger.debug("{0} is in exclude columns. Skipping...".format(full_col_name))
                else:
                    self.logger.debug("{0} is not in exclude list. Take it...".format(full_col_name))
                    result = True
            elif self.params.include_columns:
                if full_col_name in [x.upper() for x in self.params.include_columns]:
                    self.logger.debug("{0} is in include columns list. Take it...".format(full_col_name))
                    result = True
                else:
                    self.logger.debug("{0} is not in include columns list. Skipping...".format(full_col_name))
            else:
                self.logger.debug("{0} is not in include tables. Skipping...".format(table))
        elif self.params.exclude_tables or self.params.exclude_columns:
            if table in [x.upper() for x in self.params.exclude_tables]:
                self.logger.debug("{0} is in exclude tables. Skipping...".format(table))
            elif self.params.exclude_columns:
                if full_col_name in [x.upper() for x in self.params.exclude_columns]:
                    self.logger.debug("{0} is in exclude columns. Skipping...".format(full_col_name))
                else:
                    self.logger.debug("{0} is not in exclude columns. Take it...".format(full_col_name))
                    result = True
            else:
                self.logger.debug("{0} is not in exclude list. Take it...".format(table))
                result = True
        else:
            self.logger.debug("Column '{0}' exclude&include rules not set. Take it...".format(full_col_name))
            result = True
        return result

    def generate_tb(self, source_connector, target_db_type):
        """
            Generate tables based on src database and convert table to match the destination database format.
            @:param source_connector
            @:param target_db_type
        """
        source_db_type = source_connector.dbtype
        global types_mapping
        types_mapping = typesMapping.get_types_mapping(source_db_type, target_db_type)
        source_schema = ""
        target_schema = ""
        target_schemas = set()
        held_structure = ""
        table_name = ""
        is_new_table = True
        is_new_schema = True
        s = ""
        rls = []

        drp = s.split('\n')
        ddl = s.split('\n')

        types_to_skip = ''
        types_to_warn = []
        if self.params.skip_unsupported:
            types_to_skip = typesMapping.get_unsupported_types_csv(source_db_type, target_db_type)
        else:
            types_to_warn = typesMapping.get_unsupported_types(source_db_type, target_db_type)

        sql = Template(self.get_xml_data(dbtype=source_db_type, sql="select", identifier="tbDefinition").strip())
        sql = sql.substitute(types_to_skip=types_to_skip, schema_filter=self.params.source_schema)

        s = self.get_xml_data(dbtype=target_db_type, sql="create", identifier="tb").strip()
        ddl += s.split('\n')
        s = self.get_xml_data(dbtype=target_db_type, sql="create", identifier="drop").strip()
        drp += s.split('\n')

        cur = source_connector.execute(sql)
        for line in cur:
            row = self.strip_row(line)

            # skip not included cols and tables
            if not self.is_included(row[1], row[2]):
                continue

            if row[3] in types_to_warn:
                self.logger.warn("Data type '" + row[3] + "' of column '" + row[1] + "." + row[
                    2] + "' is not supported. Skipping...")
                continue
            if row[4] > self.params.charmax:
                print self.params.charmax 
                self.logger.warn(row[1] + '.' + row[2] + ' of type ' + row[3] +
                                 ' : ' + str(
                    row[4]) + ' is not acceptable (should be <= ' + str(
                    self.params.charmax) + '). Skipping...')
                continue

            source_schema = row[0]
            target_schema = self.params.get_target_schema(source_schema)
            if target_schema not in target_schemas:
                is_new_schema = True
                target_schemas.add(target_schema)

            if row[1] != table_name:
                table_name = row[1]
                is_new_table = True

            if is_new_table:
                s += held_structure + " "
                held_structure = ""

            if (is_new_schema or is_new_table) and len(rls) > 0:
                # s += ddl[3]
                rls.append(s + self.params.command_separator + "\n")

            if is_new_schema:
                s = Template(self.get_xml_data(dbtype=target_db_type, sql="create", identifier="sch").strip())
                s = s.substitute(scname=self.quote(target_schema))
                is_new_schema = False
                if s != "":
                    rls.append(s + self.params.command_separator + "\n")
                else:
                    rls.append(s + "\n")
            if is_new_table:
                if self.params.add_drop:
                    # The drop table statement
                    s = Template(drp[1])
                    s = s.substitute(scname=self.quote(target_schema), tbname=self.quote(table_name))
                    if s != "":
                        rls.append(s + self.params.command_separator + "\n")
                # The create table statement
                s = Template(ddl[1])
                s = s.substitute(scname=self.quote(target_schema), tbname=self.quote(table_name))
                is_new_table = False
            else:
                s += ','

            s += "\n"
            clname = row[2]
            tyname = row[3]
            precision = row[4] if row[4] > 0 else self.params.charmax
            scale = 0 if row[5] is None else row[5]
            isnull = '' if row[6] is None else row[6]
            dfval = '' if row[7] is None else "DEFAULT " + row[7]

            if dfval in typesMapping.db2vw_default:
                dfval = typesMapping.db2vw_default[dfval]
            if "NEXT VALUE FOR" in dfval:
                dfval = ''
            # Substitute datatype by equivalent datatype
            target_type = types_mapping[tyname.upper()][0]
            s += Template(ddl[2]).substitute(clname=self.quote(clname), tyname=target_type, isnull=isnull, dfval=dfval)
	    precisionstring = str(precision)
	    if precisionstring.endswith('.0'):
	        precisionstring = precisionstring[:-2]
            s = s.replace('<PRECISION>', str(precisionstring))
			
	    scalestring = str(scale)
	    if scalestring.endswith('.0'):
	        scalestring = scalestring[:-2]
            s = s.replace('<SCALE>', str(scalestring))
			
            if held_structure == "":
                held_structure = Template(ddl[3]).substitute(clname=self.quote(clname))

        if held_structure != "":
            #held_structure = Template(ddl[3]).substitute(clname=self.quote(clname))
            s += held_structure

        if len(rls) > 0:
            # s += ddl[3]
            rls.append(s + self.params.command_separator + "\n")

        return rls

    def generate_views(self, source_connector, target_db_type):
        """
            Generate views based on src database and convert view to match the destination database format.
            SELECT s.table_schema as scname, s.table_name as viwname, s.view_definition as viwdef, s.check_option as viwchk , s.is_updateable viwupd
            @:param source_connector
            @:param target_db_type
        """
        self.logger.debug("Running generate_views processing")
        target_schema = ""
        source_schema = ""
        target_schemas = set()
        viewname = ""
        viwdef = ""
        is_new_view = True
        is_new_schema = True
        s = ""
        rls = []

        sql = Template(self.get_xml_data(dbtype=source_connector.dbtype, sql="select", identifier="viwDefinition"))
        sql = sql.safe_substitute(schema_filter=self.params.source_schema)
        s = self.get_xml_data(dbtype=target_db_type, sql="create", identifier="viw").strip()

        # START View
        cur = source_connector.execute(sql)
        for line in cur:
            row = self.strip_row(line)

            # An attempt to get one schema only loaded
            source_schema = row[0]
            target_schema = self.params.get_target_schema(source_schema)
            if target_schema not in target_schemas:
                is_new_schema = True
                target_schemas.add(target_schema)

            if row[1] != viewname:
                viewname = row[1]
                is_new_view = True

            if (is_new_schema or is_new_view) and len(rls) > 0:
                rls.append(s + self.params.command_separator + "\n")

            viwdef = row[2]
            # Tried regex to swap CONVERT to CAST, but it's detecting too much of the text
            # regex = re.compile(r'CONVERT\((?P<type>[^,]*),(?P<value>[^,]*)\)', re.MULTILINE)
            # viwdef = regex.sub(r'CAST(\g<value> AS \g<type>)', viwdef)

            if viwdef is None : # when NULL 
                print "viwdef is NONE"
            else :
                for i in range(0, len(typesMapping.db2vw_view) - 1):
                
                    viwdef = viwdef.replace(typesMapping.db2vw_view[i][0], typesMapping.db2vw_view[i][1])

                viwdef = viwdef.replace(self.quote(source_schema), self.quote(target_schema))
                viwdef = viwdef.replace('[' + source_schema + ']', self.quote(target_schema))
            ##viwdef = viwdef.replace('WITH READ ONLY','')
            ## viwdef = viwdef.replace('with read only','')

                is_new_view = True

                if is_new_schema:
                    s = Template(self.get_xml_data(dbtype=target_db_type, sql="create", identifier="sch").strip())
                    s = s.substitute(scname=self.quote(target_schema))
                    is_new_schema = False
                    if s != "":
                        rls.append(s + self.params.command_separator + "\n")
                    else:
                        rls.append(["\n", ''])

                if is_new_view:
                # The create view statement
                # s = viwdef
                    s = "drop view if exists "+viewname+self.params.command_separator+"\n"+viwdef
                    is_new_view = False

                s += "\n"
                viwdef = row[4]

                rls.append(s + self.params.command_separator + "\n")
        return rls

    def generate_uk(self, source_connector, target_db_type):
        """
            Generate unique constraints (also inclue PrimaryKeys since a PK is also a unique constraint)
            based on src database and convert unique constraint to the new database format.
            @:param source_connector
            @:param target_db_type
        """
        self.logger.debug("Running generate_uk processing")
        source_schema = ""
        source_schema_prev = ""
        table_name = ""
        table_name_prev = ""
        uk_name = ""
        uk_name_prev = ""
        uk_type = ""
        s = ""
        rls = []  # A returned list which contains the results of the function
        self.logger.debug(self.params.source_schema)
        self.logger.debug(source_connector.dbtype)
        sql = Template(self.get_xml_data(dbtype=source_connector.dbtype, sql="select", identifier="ukDefinition"))
        sql = sql.safe_substitute(schema_filter=self.params.source_schema)
        #line above was sql = sql.substitute(schema_filter=self.params.source_schema)
        #sql = sql.safe_substitute(schema_filter='ontime')
        #line above was sql = sql.substitute(schema_filter='ontime')
        self.logger.debug(sql)
        ddl = self.get_xml_data(dbtype=target_db_type, sql="create", identifier="uk").strip()

        cur = source_connector.execute(sql)
        for line in cur:
            row = self.strip_row(line)

            # skip not included cols and tables
            if not self.is_included(row[1], row[4]):
                continue

            source_schema = row[0]
            table_name = row[1]
            # uk_name = row[2] + self.params.command_separator + row[1]
            uk_name = row[2] 
            uk_type = row[3]  # Constraint type
            col_names = row[4]  # Column Names
            del_rule = row[5]  # Deletion Rule if provided else blank
            if del_rule is None:
                del_rule = ""


            if (source_schema_prev, table_name_prev, uk_name_prev) != (source_schema, table_name, uk_name):
                source_schema_prev = source_schema
                table_name_prev = table_name
                uk_name_prev = uk_name

                if len(s):
                    # We pass to the next index definition
                    s = Template(s).substitute(clname='')
                    rls.append(s + self.params.command_separator + "\n")

                target_schema = self.params.get_target_schema(source_schema)
                s = Template(ddl)
                s = s.substitute(scname=self.quote(target_schema), 
                                 tbname=self.quote(table_name),
                                 csname=self.quote(uk_name), 
                                 cstype=uk_type,
                                 delname=del_rule,
                                 clname=self.quote(col_names) + r'${clname}')
            else:
                s = Template(s).substitute(clname=',' + self.quote(col_names) + r'${clname}')

        s = Template(s).substitute(clname='')
##                                   delname=del_rule)
        rls.append(s + self.params.command_separator + "\n")

        return rls

    def generate_fk(self, source_connector, target_db_type):
        """
            Generate foreign key based on src database and convert fk to the new database format.
            @:param source_connector
            @:param """
        self.logger.debug("Running generate_fk processing")
        source_schema = ""
        source_schema_prev = ""
        table_name = ""
        table_name_prev = ""
        fk_name = ""
        fk_name_prev = ""
        s = ""
        rls = []

        sql = Template(self.get_xml_data(dbtype=source_connector.dbtype, sql="select", identifier="fkDefinition"))
        sql = sql.safe_substitute(schema_filter=self.params.source_schema)

        ddl = self.get_xml_data(dbtype=target_db_type, sql="create", identifier="fk").strip()

        cur = source_connector.execute(sql)
        for line in cur:
            row = self.strip_row(line)

            # skip not included cols and tables (both for source and referenced tables)
            if not self.is_included(row[1], row[3]) or not self.is_included(row[5], row[6]):
                continue

            # skip self-referencing FKs
            if row[1] == row[5]:
                continue

            source_schema = row[0]
            table_name = row[1]
            fk_name = row[2] + self.params.index_separator + row[
                1]  # Constraint names( Objects names )  must be unique in a schema
            col_name = row[3]
            ref_col_name = row[6]
            del_rule = row[7]
            if del_rule is None:
                del_rule=""

            if (source_schema_prev, table_name_prev, fk_name_prev) != (source_schema, table_name, fk_name):
                source_schema_prev = source_schema
                table_name_prev = table_name
                fk_name_prev = fk_name

                # We pass to the next index definition
                if len(s) > 0:
                    s = Template(s).substitute(clname='', rclname='')
                    rls.append(s + self.params.command_separator + "\n")

                # Resource owner ... 
                # ALTER TABLE "dbo"."Order Details" ADD CONSTRAINT "FK_Order_Details_Orders"      FOREIGN KEY ( "OrderID" )
                # REFERENCES "dbo_rsc"."Orders_rtb" ( "OrderID" )
                #
                # ALTER TABLE "dbo"."CustomerCustomerDemo" ADD CONSTRAINT "FK_CustomerCustomerDemo"      FOREIGN KEY ( "CustomerTypeID" )
                # REFERENCES "dbo"."CustomerDemographics_rtb" ( "CustomerTypeID" )
                source_ref_schema = row[4]
                ref_table_name = row[5]
                target_schema = self.params.get_target_schema(source_schema)
                target_ref_schema = self.params.get_target_schema(source_ref_schema)

                s = Template(ddl)
                s = s.substitute(scname=self.quote(target_schema), tbname=self.quote(table_name),
                                 csname=self.quote(fk_name), clname=self.quote(col_name) + r'${clname}',
                                 rscname=self.quote(target_ref_schema), rtbname=self.quote(ref_table_name),
                                 delname=self.quote(del_rule),
                                 rclname=self.quote(ref_col_name) + r'${rclname}')
            else:
                s = Template(s).substitute(clname=',' + self.quote(col_name) + r'${clname}',
                                           rclname=',' + self.quote(ref_col_name) + r'${rclname}')

        s = Template(s).substitute(clname='', rclname='')  # End constraint definition
        rls.append(s + self.params.command_separator + "\n")

        return rls

    def generate_ix(self, source_connector, target_db_type):
        """
            Generate indexes based on src database and convert indexes to match the destination database format.
            @:param source_connector
            @:param target_db_type
        """
        self.logger.debug("Running generate_ix processing")
        
        source_schema = ""
        source_schema_prev = ""
        table_name = ""
        table_name_prev = ""
        ix_name = ""
        ix_name_prev = ""
        ix_type = ""
        s = ""
        rls = []

        sql = Template(self.get_xml_data(dbtype=source_connector.dbtype, sql="select", identifier="ixDefinition"))
        sql = sql.safe_substitute(schema_filter=self.params.source_schema)

        ddl = self.get_xml_data(dbtype=target_db_type, sql="create", identifier="ix").strip()

        cur = source_connector.execute(sql)
        for line in cur:
            row = self.strip_row(line)

            # skip not included cols and tables
            if not self.is_included(row[1], row[6]):
                continue

            source_schema = row[0]
            table_name = row[1]
            ix_name = row[3] + self.params.index_separator + row[1]
            col_name = row[6]

            if (source_schema_prev, table_name_prev, ix_name_prev) != (source_schema, table_name, ix_name):
                source_schema_prev = source_schema
                table_name_prev = table_name
                ix_name_prev = ix_name

                if len(s):  # We print last index and we pass to the next index definition
                    s = Template(s).substitute(clname='')
                    rls.append(s + "\n")
                    if ix_type != 'BTREE':
                        self.logger.warn("Unknown %s index type %s. Following command has been skipped\n%s " % (
                            source_connector.dbtype, str(ix_type), s))

                # Constraint names( Objects names )  must be unique in a schema
                ix_type = row[4]
                ixuniq = row[5]
                target_schema = self.params.get_target_schema(source_schema)
                target_index_schema = self.params.get_target_schema(row[2])

                s = Template(ddl)
                s = s.substitute(ixuniq='' if ixuniq is None else ixuniq,
                                 iscname=self.quote(target_index_schema),
                                 ixname=self.quote(ix_name),
                                 scname=self.quote(target_schema),
                                 tbname=self.quote(table_name),
                                 clname=self.quote(col_name) + r'${clname}')
            else:
                s = Template(s).substitute(clname=',' + self.quote(col_name) + r'${clname}')

        s = Template(s).substitute(clname='')  # End index definition
        rls.append(s + "\n")

        return rls

    def unload_data(self, source_connector, target_db_type):
        """
            Extract data from src db and load data to dest db
            @:param source_connector
            @:param target_db_type
        """
        self.logger.debug("Running unload_data processing")
        source_schema = ""
        source_schema_prev = ""
        table_name = ""
        table_name_prev = ""
        insert = ""
        fname = ""
        counter = 0
        select = ""
        sqls = []
        s = ""
        colnum = 0
        selfrom = ""

        types_mapping = typesMapping.get_types_mapping(source_connector.dbtype, target_db_type)
        types_to_skip = typesMapping.get_unsupported_types_csv(source_connector.dbtype, target_db_type)

        sql = Template(self.get_xml_data(dbtype=source_connector.dbtype, sql="select", identifier="tbDefinition").strip())
        sql = sql.substitute(types_to_skip=types_to_skip, schema_filter=self.params.source_schema)

        cursrc = source_connector.execute(sql)

        # Iterate to prepare statements select, insert
        for line in cursrc:
            row = self.strip_row(line)

            # skip not included cols and tables
            if not self.is_included(row[1], row[2]):
                continue

            source_schema = row[0]
            table_name = row[1]

            if (source_schema_prev, table_name_prev) != (source_schema, table_name):
                source_schema_prev = source_schema
                table_name_prev = table_name

                if len(s) > 0:
                    select += selfrom
                    sqls.append((fname, colnum, select, insert))

                s = self.quote(table_name) if source_schema is None else self.quote(source_schema) + '.' + self.quote(
                    table_name)
                select = 'SELECT '
                selfrom = ' FROM ' + s

                target_schema = self.params.get_target_schema(source_schema)
                s = table_name if target_schema is None else target_schema + '_' + table_name + '.txt'
                fname = s
                insert = ""
                colnum = 0

            if colnum > 0:
                insert += self.params.fdelim
                select += ","

            clname = row[2]
            tyname = row[3]

            # Translate datatypes according to translation table
            (_, select_cast, insert_cast) = types_mapping[tyname.upper()]

            select += select_cast
            select = select.replace('<COLNAME>', '"' + clname + '"')
            insert += insert_cast
            insert = insert.replace('<VALUE>', '<V' + str(colnum) + '>')

            colnum += 1

        select += selfrom
        sqls.append((fname, colnum, select, insert))

        # Iterate to select, bind and insert data
        for (fname, colnum, select, insert) in sqls:
            try:
                self.logger.debug(select)

                sz = 0.0
                counter = 0
                t1 = time.time()
                with codecs.open(fname, encoding='utf-8', mode='w') as f:
                    cursrc = source_connector.execute(select)
                    for line in cursrc:  # Read source cursor (SELECT)
                        row = self.strip_row(line)
                        s = insert
                        for i in range(0, colnum):  # Prepare INSERT
                            value = row[i]
                            if value is None:  # When Null
                                s = s.replace("'<V" + str(i) + ">'", '')  # Replace NULL value for strings and dates
                                s = s.replace("<V" + str(i) + ">", '')  # Replace NULL value for integers or floats
                                sz += 1
                            elif type(value) == unicode:  # When  string (unicode)
                                value = value.replace("'", "''")  # Replace simple ' by '' in insert command
                                s = s.replace("<V" + unicode(str(i), 'utf-8') + ">", value)
                                sz += len(value)
                            else:  # When not string
                                try:
                                    s = s.replace("<V" + str(i) + ">", str(value))
                                    sz += len(str(value))  # Not right but gives an idea of the size of numbers
                                except UnicodeDecodeError:  # String has an unknown character
                                    self.logger.error("UnicodeDecodeError for column <%d> value = %s" % (i, value))
                                    value = value.replace("'", "''")  # Replace simple ' by '' in insert command
                                    s = s.replace("<V" + str(i) + ">", unicode(value, 'utf-8', 'ignore'))
                                    sz += len(value)
                        try:  # Write line
                            f.write(s + "\n")
                            counter += 1
                        except Exception as ex:
                            self.logger.debug(s)
                            self.logger.exception(ex)
            except Exception as ex:
                self.logger.debug(s)
                self.logger.exception(ex)
            finally:
                t2 = time.time()
                self.logger.info("Rows extracted: %d - Elapsed time(s): %f - Mean data size(MB): %f\n" % (
                    counter, (t2 - t1), sz / 1024 / 1024))

    def load_test(self, source_connector, target_connector):
        """
            Loads a lot of data in a single predefined table.
            @:param source_connector
            @:param target_connector
        """
        # Run precondition script if found (e.g. this can be used to setup session authorization)
        pres = self.get_xml_data(dbtype=target_connector.dbtype, sql="create", identifier="sch")
        sch = Template(pres).substitute(scname=self.quote(self.params.get_target_schema("dbo")),
                                        insert_mode=self.params.insert_mode).strip()

        # do not alter DB in trial mode
        if not self.params.trial:
            target_connector.execute(sch)

        # Iterate to select, bind and insert data
        s = ""
        total = 10000
        try:
            counter = 0

            t1 = time.time()
            for j in range(0, total / self.params.batchsize):
                currentCounter = 0
                inserts = []
                s = "INSERT INTO dbo.Territories VALUES (%d,'%s',%d)" % (j, "Territory%d" % j, 2 * j)
                inserts.append(s)
                for i in range(0, self.params.batchsize - 1):
                    s = ", (%d,'%s',%d)" % (i, "Territory%d" % i, 2 * i)
                    inserts.append(s)
                t2 = time.time()
                currentCounter += len(inserts) * self.insert_sql(target_connector, ";\n".join(inserts))
                counter += currentCounter
                self.logger.info("[%d] Batch inserted: %d - Elapsed time(s): %f" % (j, currentCounter, (t2 - t1)))

            t2 = time.time()
            self.logger.info("Total rows inserted: %d - Elapsed time(s): %f" % (counter, (t2 - t1)))

        except Exception as ex:
            self.logger.debug(s)
            self.logger.exception(ex)
        finally:
            pass

    def load_data(self, source_connector, target_connector):
        """
            Extract data from src db and load data to dest db
            @:param source_connector
            @:param target_connector
        """
        source_schema = ""
        source_schema_prev = ""
        table_name = ""
        table_name_prev = ""
        count_loaded = 0
        insert = ""
        select = ""
        sqls = []
        s = ""
        colnum = 0
        table_name = ""
        selfrom = ""
        types_mapping = typesMapping.get_types_mapping(source_connector.dbtype, target_connector.dbtype)
        types_to_skip = typesMapping.get_unsupported_types_csv(source_connector.dbtype, target_connector.dbtype)

        sql = Template(self.get_xml_data(dbtype=source_connector.dbtype, sql="select", identifier="tbDefinition").strip())
        sql = sql.substitute(types_to_skip=types_to_skip, schema_filter=self.params.source_schema)

        cursrc = source_connector.execute(sql)
        # Iterate to prepare statements select, insert
        for line in cursrc:
            row = self.strip_row(line)

            # skip not included cols and tables
            if not self.is_included(row[1], row[2]):
                continue

            source_schema = row[0]
            table_name = row[1]

            if (source_schema_prev, table_name_prev) != (source_schema, table_name):
                source_schema_prev = source_schema
                table_name_prev = table_name
                target_schema = self.params.get_target_schema(source_schema)

                if len(s) > 0:
                    insert += ")"
                    select += selfrom
                    sqls.append((colnum, select, insert,
                                 self.quote(table_name.strip()) if target_schema is None else self.quote(
                                     target_schema.strip()) + '.' + self.quote(table_name.strip())))

                s = self.quote(table_name.strip()) if source_schema is None else self.quote(source_schema.strip()) + '.' + self.quote(
                    table_name.strip())
                select = 'SELECT '
                selfrom = ' FROM ' + s

                s = self.quote(table_name.strip()) if target_schema is None else self.quote(target_schema.strip()) + '.' + self.quote(
                    table_name.strip())
                table_name = s
                insert = 'INSERT INTO ' + s + ' VALUES ('
                colnum = 0

            if colnum > 0:
                insert += ","
                select += ","

            clname = row[2]
            tyname = row[3]

            (_, select_cast, insert_cast) = types_mapping[
                tyname.upper()]  # Translate datatypes according to translation table

            select += select_cast
            select = select.replace('<COLNAME>', '"' + clname.strip() + '"')
            insert += insert_cast
            insert = insert.replace('<VALUE>', '<V' + str(colnum) + '>')

            colnum += 1

        insert += ")"
        select += selfrom
        sqls.append((colnum, select, insert, table_name))

        # Run precondition script if found (e.g. this can be used to setup session authorization)
        target_schema = self.params.get_target_schema(source_schema)
        pres = self.get_xml_data(dbtype=target_connector.dbtype, sql="create", identifier="pre")
        pre = Template(pres).substitute(scname=self.quote(target_schema), insert_mode=self.params.insert_mode)
        for pre_line in pre.split(';'):
            # do not alter DB in trial mode
            if not self.params.trial:
                target_connector.execute(pre_line)

        self.connectors = [[source_connector, target_connector, False, 0]]
        for index in range(1, self.params.threads):
            source_db = dbconnector(self.params.src, True)
            target_db = dbconnector(self.params.dest, True)
            self.connectors.append([source_db, target_db, False, index])

        self.logger.info('Started loading data from tables. Thread count: ' + str(self.params.threads))
        start_time = time.time()
        futures = []
        pool = ThreadPoolExecutor(self.params.threads)
        for (colnum, select, insert, table_name) in sqls:
            if table_name == "":
                self.logger.warn("No table_name specified")
            else:
                count_loaded += 1
                futures.append(pool.submit(self.copy_table_data, table_name, colnum, select, insert))
        wait(futures)
        for i in range(1, self.params.threads):
            self.connectors[i][0].close()
            self.connectors[i][1].close()
        if count_loaded > 0:
            self.logger.info("Data from all tables (%d) was loaded. Total Elapsed time: %f" %
                             (count_loaded, time.time() - start_time))
        else:
            self.logger.warn("No tables loaded. Total Elapsed time: %f" % (time.time() - start_time))

    def insert_sql(self, db, sql):
        """
            call db sql script with exception wrap
        """
        try:  # Execute INSERT
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                if len(w) > 0: self.logger.debug(len(w))

                # do not alter DB in trial mode
                if not self.params.trial:
                    db.execute(sql)
                    db.commit()

                if len(w) > 0:
                    self.logger.debug(sql)
                    self.logger.debug(str(w[-1].message))
                return 1
        except Exception as ex:
            self.logger.warn('Error: Failed to insert data into target DB')
            self.handle_error(ex)
        finally:
            pass
        return 0

    def copy_table_data(self, table_name, column_count, select, insert):
        try:
            connector = None
            for conn in self.connectors:
                if not conn[2]:
                    conn[2] = True
                    connector = conn
                    break
            source_connector = connector[0]
            target_connector = connector[1]
            if table_name == '':
                return 9
            else:
                self.logger.debug(table_name)

            self.logger.debug(select)
            cursrc = source_connector.execute(select)

            counter = 0
            currentCounter = 0
            inserts = []

            ## self.logger.debug("[Thread #%d] Loading..." % (connector[3]))
            self.logger.debug("[Thread #%d] Loading... %s" % (connector[3] , table_name))
            sz = 0.0
            t1 = time.time()

            # If truncate was specified remove existing rows from the destination table.
            if self.params.truncate and table_name != '':
                # do not alter DB in trial mode
                if not self.params.trial:
                    print "Running in Trial Mode for %s" % table_name
                    target_connector.execute('MODIFY %s TO TRUNCATED' % table_name)

            insert_time = 0
            batch_start = time.time()
            is_first_insert = True
            for line in cursrc:  # Read source cursor (SELECT)
                row = self.strip_row(line)
                s = insert
                for i in range(0, column_count):  # Prepare INSERT
                    value = row[i]
                    if value is None:  # When Null
                        s = s.replace("'<V" + str(i) + ">'", 'NULL')  # Replace NULL value for strings and dates
                        s = s.replace("<V" + str(i) + ">", 'NULL')  # Replace NULL value for integers or floats
                        sz += 1
                    elif type(value) == unicode:  # When  string (unicode)
                        value = value.replace("'", "''")  # Replace simple ' by '' in insert command
                        s = s.replace("<V" + unicode(str(i), 'utf-8') + ">", value)
                        sz += len(value)
                    else:  # When not string
                        try:
                            s = s.replace("<V" + str(i) + ">", str(value))
                            sz += len(str(value))  # Not right but gives an idea of the size of numbers
                        except UnicodeDecodeError:  # String has an unknown character
                            self.logger.error("UnicodeDecodeError for column <%d> value = %s" % (i, value))
                            value = value.replace("'", "''")  # Replace simple ' by '' in insert command
                            s = s.replace("<V" + str(i) + ">", unicode(value, 'utf-8', 'ignore'))
                            sz += len(value)
                            self.logger.debug("- Resulting DML : %s " % s)
                        finally:
                            pass
                if is_first_insert:
                    inserts.append(s)
                    is_first_insert = False
                else:
                    # Get the string after VAULES, e.g. ('<V0>','<V1>',<V2>)
                    s = s.encode("utf-8")
                    values = s[s.index("VALUES") + 6:]
                    inserts.append(values)
                    currentCounter = len(inserts)

                if (currentCounter >= self.params.batchsize) or \
                        (currentCounter + self.inserted_queries_number.get()) >= self.params.maxrows:
                    if self.__is_reached_insertion_limit(connector[3]):
                        # prevent any inserts
                        currentCounter = 0
                        inserts = []
                        break

                    insert_start = time.time()
                    currentCounter *= self.insert_sql(target_connector, ",".join(inserts))
                    counter += currentCounter
                    inserts = []
                    t2 = time.time()
                    insert_time += t2 - insert_start
                    self.logger.debug(
                        "[Thread #%d] Batch inserted: %d - Elapsed time(s): %f (insert time: %f), - Estimated size(MB): %f\n" % (
                            connector[3], currentCounter, t2 - batch_start, t2 - insert_start, sz / 1024 / 1024))
                    self.inserted_queries_number.add(currentCounter)
                    currentCounter = 0
                    is_first_insert = True

                    if self.__is_reached_insertion_limit(connector[3]):
                        break

            if currentCounter > 0:
                insert_start = time.time()
                counter += currentCounter * self.insert_sql(target_connector, ",".join(inserts))
                insert_time += time.time() - insert_start

            t2 = time.time()
            self.logger.debug(
                "[Thread #%d] Total Rows inserted into %s: %d - Elapsed time(s): %f (total insert time: %f), - Estimated size(MB): %f\n" % (
                    connector[3],table_name, counter, t2 - t1, insert_time, sz / 1024 / 1024))
        except Exception as ex:
            self.logger.error("Failed to copy data for table'" + table_name + "'.")
            self.handle_error(ex)
        finally:
            connector[2] = False
            pass

    def __is_reached_insertion_limit(self, thread_id):
        if self.inserted_queries_number.get() >= self.params.maxrows:
            self.logger.debug("[Thread #%d] Global insertion maximum %d already reached -> stop inserting" % (
            thread_id, self.params.maxrows))
            return True
        return False

    def create_views(self, connector, sql_statements):
        """
            Creates Views in target DB.
        """
        failed_views = []
        for s in sql_statements:
            try:
                # do not alter DB in trial mode
                if not self.params.trial:
                    connector.execute(s)
            except Exception, ex:
                failed_views.append([s, str(ex)])
        if len(failed_views) > 0:
            self.logger.warn(
                'Some Views were not created. This can happen when excluded or unsupported tables/columns are present in View declaration.')
            self.logger.warn('Please review following View(s) that were skipped:')
            for error in failed_views:
                self.logger.warn('Reason: ' + error[1])
                self.logger.warn('View Declaration:\n' + error[0].rstrip('\n'))


class SchemaConvertor:
    def __init__(self, params):
        self.params = params
        self.util = ConvertorUtil(params, params.xml_path)
        self.logger = logging.getLogger(__name__)

    def convert(self):
        with dbconnector(self.params.src) as source_connector:
            connect = self.params.loaddl or self.params.loadata or self.params.loadtest
            with dbconnector(self.params.dest, connect) as target_connector:
                if self.params.cretab:
                    try:
                        tbs = self.util.generate_tb(source_connector, target_connector.dbtype)
                        self.util.write_txt_file('tab', tbs)
                        if self.params.mapping :
                            ## create a nice readable output
                            print "\n"
                            print "%-*s  %-*s " % (32,"Source Column",40,"Target Column")
                            print "=============================================="
                            for prt in types_mapping:
                                print "%-*s  %-*s" % (32,prt,40,types_mapping[prt])
                            print "\n"
                    except Exception as ex:
                        self.util.handle_error(ex)

                if self.params.creview:
                    try:
                        views = self.util.generate_views(source_connector, target_connector.dbtype)
                        self.util.write_txt_file('viw', views)
                    except Exception as ex:
                        self.util.handle_error(ex)

                if self.params.loaddl and self.params.cretab:
                    for s in tbs:
                        try:
                            self.logger.debug(s)
                            # do not alter DB in trial mode
                            if not self.params.trial:
                                target_connector.execute(s)
                        except Exception as ex:
                            self.util.handle_error(ex)

                if self.params.loaddl and self.params.creview:
                    self.util.create_views(target_connector, views)

                if self.params.loadata:
                    try:
                        self.util.load_data(source_connector, target_connector)
                    except Exception as ex:
                        self.util.handle_error(ex)

                if self.params.unload:
                    try:
                        self.util.unload_data(source_connector, target_connector.dbtype)
                    except Exception as ex:
                        self.util.handle_error(ex)

                if self.params.loadtest:
                    try:
                        self.util.load_test(source_connector, target_connector)
                    except Exception as ex:
                        self.util.handle_error(ex)

                if self.params.creall:
                    try:
                        uks = self.util.generate_uk(source_connector, target_connector.dbtype)
                        ixs = self.util.generate_ix(source_connector, target_connector.dbtype)
                        fks = self.util.generate_fk(source_connector, target_connector.dbtype)
                        views = self.util.generate_views(source_connector, target_connector.dbtype)
                        self.util.write_txt_file('all', uks + ixs + fks + views)
                    except Exception as ex:
                        self.util.handle_error(ex)

                if self.params.creindex:
                    try:
                        uks = self.util.generate_uk(source_connector, target_connector.dbtype)
                        ixs = self.util.generate_ix(source_connector, target_connector.dbtype)
                        fks = self.util.generate_fk(source_connector, target_connector.dbtype)
                        self.util.write_txt_file('index', uks + ixs + fks)
                    except Exception as ex:
                        self.util.handle_error(ex)

                if self.params.loaddl and self.params.creall:
                    queries = uks + ixs + fks
                    for s in queries:
                        self.logger.debug(s)
                        try:
                            # do not alter DB in trial mode
                            if not self.params.trial:
                                target_connector.execute(s)
                        except Exception as ex:
                            self.util.handle_error(ex)
                    self.util.create_views(target_connector, views)

                if self.params.loaddl and self.params.creindex:
                    queries = uks + ixs + fks
                    for s in queries:
                        self.logger.debug(s)
                        try:
                            # do not alter DB in trial mode
                            if not self.params.trial:
                                target_connector.execute(s)
                        except Exception as ex:
                            self.util.handle_error(ex)
