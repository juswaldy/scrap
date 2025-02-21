"""
Copyright (c) 2022-2023 J.Jusman

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from typing import Tuple
import os
import glob
import re
import csv
import json
import argparse
from unicodedata import category
import numpy as np
import pyodbc
import dbconfig

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
""" Configs """
class Configs:
    def __init__(self, **kwargs):

        """ SQL Agent """
        self.newline = '\n'
    
        """ Specific params """
        self.delete_only = [
            'x_archive_trans_hist_2013_fa',
            'TWU_x_TRANS_HIST_FF_4_20140129',
            'TWU_x_TRANS_HIST_FF_112_20150602',
            'TWU_TMP_TRANS_HIST',
            'TWU_TMP_STG_EMPLOYEEDISCOUNT',
            'twu_tmp_SP2014_USD_ADJ',
            'TWU_tmp_Kantech_orig',
            'TWU_tmp_Kantech_new',
            'TWU_tmp_Kantech_load',
            'TWU_TMP_DEP_DATES_AWM',
            'TWU_TEMP_TORCH_STUDENTS',
            'TWU_TEMP_TORCH_BEFORE_RESET_20140308',
            'TWU_TEMP_STUDENTS_FA2014_STS_C_20140316',
            'TWU_TEMP_STUDENT_PAID_FLG_SET_20140316',
            'TWU_TEMP_STUDENT_CRS_HIST_HKIN_FIX_20140422',
            'TWU_TEMP_SECTION_MASTER_HKIN_FIX_20140422',
            'TWU_TEMP_CATALOG_UPDATE05',
            'TWU_STUDENT_MASTER_PRE_TELWEB_202210',
            'TWU_STUDENT_MASTER_PRE_TELWEB_202202',
            'TWU_STUDENT_MASTER_PRE_TELWEB_202111',
            'TWU_STUDENT_MASTER_PRE_TELWEB_202110',
            'TWU_STUDENT_MASTER_PRE_TELWEB_202102',
            'TWU_STUDENT_MASTER_PRE_TELWEB_202010',
            'TWU_STUDENT_MASTER_PRE_TELWEB_202002',
            'TWU_STUDENT_MASTER_PRE_TELWEB_201911',
            'TWU_STUDENT_MASTER_PRE_TELWEB_201905_v1',
            'TWU_STUDENT_MASTER_PRE_TELWEB_201905',
            'TWU_STUDENT_MASTER_PRE_TELWEB_201902',
            'tmp_TEST_TRANS_HIST_IMPORT',
            'temp_vendors',
            'temp_subsids',
            'temp_students',
            'temp_rules',
            'temp_rule',
            'temp_org',
            'temp_not_vendors',
            'temp_not_subsids',
            'temp_not_students',
            'temp_faculty',
            'TEL_WEB_GRP_CDE_asOf_20141015',
            'STUDENT_MASTER_TELWEBSP14',
            'STUDENT_MASTER_PRE_TELWEBFA15',
            'STUDENT_DIV_MAST_20140306',
            'STUDENT_CRS_HIST_cr_22612_20140829',
            'STUD_LIFE_CHGS_20141031',
            'STUD_AIMS_LABELS_20140306',
            'STUD_AIMS_20140306',
            'SectionMasterGradePeriod_20230413',
            'HousingFA19_AWM',
            'bkp_VIOL_INCID_DTL',
            'bkp_VIOL_INCID',
            'bkp_TXRPT_FADB_DATA',
            'bkp_TIMCRDS',
            'bkp_Thread',
            'bkp_TE_TABLES',
            'bkp_SUBMISSION',
            'bkp_STUDENT_CRS_HIST',
            'bkp_STUD_INSUR_MASTER',
            'bkp_STUD_HLTH_PROFILE',
            'bkp_STRPT_OK_TRS_EMPL_YEAR_DATA',
            'bkp_STRPT_AR_PERKINS_II_DATA',
            'bkp_ST_STUDENT_CRS_HIST',
            'bkp_ST_OK_ORG_POS_MAST',
            'bkp_SPORTS_TRACKING',
            'bkp_SettingLogicalOption',
            'bkp_Setting',
            'bkp_SECTION_VAR_GRADE_TYPE',
            'bkp_SECTION_MSTR_WEB_REG',
            'bkp_SECTION_MSTR_ATTRB',
            'bkp_SECTION_MASTER',
            'bkp_ScheduledEventDetails',
            'bkp_ProviderConfiguration',
            'bkp_PREREQ_ADV_TABLE_20140320',
            'bkp_PREREQ_ADV_CONTROL_20140320',
            'bkp_PAYROLL_CONFIG',
            'bkp_PATimecardDetailHours',
            'bkp_PATimecardDetail',
            'bkp_PART_DEF',
            'bkp_MINOR_MAJOR',
            'bkp_LOGIN_PURPOSE_CDE_DEF',
            'bkp_LOGIN_ID_XREF',
            'bkp_LICENSE_INFO',
            'bkp_IR_OK_EMPL_POS_CONTR',
            'bkp_INVOLVE_TYPE_DEF',
            'bkp_IND_POS_HIST',
            'bkp_IND_PAY_ACC_RATE',
            'bkp_HR_CONFIG',
            'bkp_HONOR_DEFINITION',
            'bkp_HEALTH_GRP_MASTER',
            'bkp_HEALTH_CDE_GRPS',
            'bkp_HEALTH_CDE_DEF',
            'bkp_GROUP_MEMBERSHIP',
            'bkp_GRADE_TABLE',
            'bkp_GPA_TIER_HIST',
            'bkp_GPA_TIER_DEF',
            'bkp_GL_MASTER',
            'bkp_FacilitySpace',
            'bkp_FacilityRelationshipType',
            'bkp_Facility',
            'bkp_EventType',
            'bkp_DATABASE_LEVEL_HISTORY',
            'bkp_COURSE_AUTHORIZATION',
            'bkp_COREQ_TABLE_OLR_20140320',
            'bkp_COREQ_TABLE_20140319',
            'bkp_COREQ_CONTROL_20140320',
            'bkp_COREQ_ADV_TABLE_20140320',
            'bkp_COREQ_ADV_CONTROL_20140320',
            'bkp_CONCENTRATION_MAJOR',
            'bkp_CM_SESSION_MSTR',
            'bkp_CHK_HIST_TIMCARD',
            'bkp_CATALOG_MASTER',
            'bkp_CalendarTimeslotExternal',
            'bkp_BIOGRAPH_MASTER',
            'bkp_ATTRIBUTE_TRANS',
            'bkp_ATTRIBUTE_DEF',
            'bkp_ATTRIB_GROUP_ACS',
            'bkp_ATTACHMENT',
            'bkp_ApplicationUser',
            'bkp_APP_USER_LDAP',
            'bkp_APP_USER',
            'bkp_APP_GROUP',
            'bkp_ACT_PART',
            'bkp_ACCR_STDS_TABLE',
        ]

        self.backup_first = [
            'acct_cmp_1_def_bk',
            'acct_cmp_2_def_bk',
            'acct_cmp_3_def_bk',
            'acct_cmp_4_def_bk',
            'acct_cmp_5_def_bk',
            'acct_cmp_6_def_bk',
            'DEGREE_HISTORY_tmpbak_20230721',
            'due_tofrom_accts_bkpost',
            'due_tofrom_accts_cu_bkpost',
            'generalledger_bk',
            'gl_master_bk',
            'rpt_cell_def_bk',
            'TWU_STUDENT_MASTER_PRE_TELWEB_202302',
            'x_comp_1',
            'x_comp_2',
            'x_comp_3',
            'x_comp_4',
            'x_comp_5',
            'x_comp_6',
            'x_gl_master',
            'x_gl_master_bk',
            'x_new_coa',
        ]

        """ Meta """
        self.enumerate_sourcetables = False
        self.debugging = False

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
""" Argument parsing and checking """
def parse_args() -> argparse.Namespace:
    desc = 'Miscellaneous tools for working with MS SQL'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--fn', type=str, default=None, help='sphelp, whereincode', required=True)
    parser.add_argument('--dsn', type=str, default=None, help='ODBC DSN', required=False)
    parser.add_argument('--string', type=str, default=None, help='String parameter', required=False)
    parser.add_argument('--int', type=int, default=None, help='Integer parameter', required=False)
    parser.add_argument('--inputfile', type=str, default=None, help='Input file', required=False)
    parser.add_argument('--outputfile', type=str, default=None, help='Output file', required=False)
    return check_args(parser.parse_args())
def check_args(args: argparse.Namespace) -> argparse.Namespace:
    """ No checks for now. """
    return args

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
""" Helper functions """
def enumerate_params(thedict: dict) -> list:
    result = [ f"\t\t@{k}={thedict[k]}, " for k in list(thedict.keys())[1:-1] ]
    result.extend([f"\t\t@{k}={thedict[k]}" for k in [list(thedict.keys())[-1]]])
    return result

def strip_nvarchar(line: str) -> str:
    return line[2:-1]

def parse_targets(proc_str: str, keyword: str) -> list:
    return list(set(re.findall(r'{}\s+([^@#][^\s;]*)'.format(keyword), proc_str, re.IGNORECASE)))

def get_results(cursor: pyodbc.Cursor) -> (list, list):
    columns, values = [], []
    if cursor.description:
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        for row in rows:
            vals = [str(val) for val in row]
            values.append(vals)
    return columns, values

def columns2header(columns: list) -> str:
    return ','.join(columns)

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
""" Available functions """

""" Run sp_help on the supplied objects """
def sphelp(args: argparse.Namespace) -> None:
    dsn = getattr(dbconfig, args.dsn)
    conn = pyodbc.connect(dsn, autocommit=True)
    cursor = conn.cursor()

    outfields = ['Name', 'Owner', 'Type', 'Created_datetime']
    rstypes = []

    with open(args.inputfile, 'r') as infile, open(args.outputfile, 'w') as outfile:
        lines = infile.readlines()
        writer = csv.DictWriter(outfile, outfields)
        writer.writeheader()
        for obj in lines:
            sql = f"EXEC sp_help '{obj.strip()}'"
            cursor.execute(sql)
            columns, values = get_results(cursor)
            if columns2header(columns) == columns2header(outfields):
                row = dict(zip(columns, values[0]))
                writer.writerow(row)
            while (cursor.nextset()):
                columns, values = get_results(cursor)
                if columns2header(columns) == columns2header(outfields):
                    row = dict(zip(columns, values[0]))
                    writer.writerow(row)

    cursor.close()
    print(rstypes)

    return

""" Find the supplied objects in our sql codebase """
def whereincode(args: argparse.Namespace) -> None:

    candidates = [ args.string.split(',') ] if args.string else config.delete_only + config.backup_first

    if args.int == 1:
        # Where in Aqueduct Reports?
        dsn = getattr(dbconfig, 'aq_PROD')
        conn = pyodbc.connect(dsn, autocommit=True)
        cursor = conn.cursor()
        print('AQ Reports')
        for candidate in candidates:
            for obj in candidate:
                print('.', end='', flush=True) if config.debugging else None
                sql = f"SELECT Name FROM Reporting.Report WHERE Definition LIKE '%{obj}%' ORDER BY Name"
                cursor.execute(sql)
                rows = cursor.fetchall()
                print('aq_PROD', obj, rows) if config.debugging and rows else None
                for row in rows:
                    print(row[0])
        cursor.close()
        print()
    else:
        # Where in TmsEPrd, ICS_NET, FinancialReporting, and Aqueduct?
        for dbconn in [ 'jz_PROD', 'ics_PROD', 'fr_PROD', 'aq_PROD' ]:
            dsn = getattr(dbconfig, dbconn)
            conn = pyodbc.connect(dsn, autocommit=True)
            cursor = conn.cursor()
            print(dbconn)
            for candidate in candidates:
                subsql = [ f"x{i} AS ( SELECT schema_name, object_name, type, '{obj}' ref FROM base WHERE text LIKE '%{obj}%' )" for i, obj in enumerate(candidate) ]
                combined = "combined AS ( " + " UNION ".join([ f"SELECT * FROM x{i}" for i, obj in enumerate(candidate) ]) + " )"
                # print('.', end='', flush=True)
                sql = "WITH\nbase AS ( SELECT s.name schema_name, o.name object_name, o.type, c.text FROM sys.syscomments c JOIN sys.objects o ON c.id = o.object_id JOIN sys.schemas s ON o.schema_id = s.schema_id ),\n"
                sql += ",\n".join(subsql) + ",\n"
                sql += combined
                sql += """
                    SELECT DISTINCT schema_name, object_name, type, 
                    STUFF(
                        (
                            SELECT CONCAT(', ', ref)
                            FROM combined c1
                            WHERE c.schema_name = c1.schema_name AND c.object_name = c1.object_name
                            FOR XML PATH ('')
                        ),
                        1, 2, ''
                    ) ref
                    FROM combined c
                """
                print(sql) if config.debugging else None
                cursor.execute(sql)
                rows = cursor.fetchall()
                print(dbconn, obj, rows) if config.debugging and rows else None
                for row in rows:
                    print(row)
            cursor.close()
            print()

""" Create clover Dump graph from the source sql """
def sql2graphdump(args: argparse.Namespace) -> None:
    import xml.etree.cElementTree as ET

    for obj in config.backup_first:
        print(obj)

        root = ET.Element('root')
        graph = ET.SubElement(root, 'Graph')

        # Global settings and configs.
        _global = ET.SubElement(graph, 'Global')
        _metadata = ET.SubElement(_global, 'Metadata')
        _connection = ET.SubElement(_global, 'Connection', attrib={'dbConfig': '${CONN_DIR}/jus_DEV.cfg'})
        _graphparameters = ET.SubElement(_global, 'GraphParameters')
        _dictionary = ET.SubElement(_global, 'Dictionary')

        # Phase 0.
        _phase = ET.SubElement(graph, 'Phase')

        tree = ET.ElementTree(root)
        tree.write(f'C:\\github\\Integration\\Everything\\graph\\tmseprd\\{obj}.xml', xml_declaration=True, encoding='unicode')
    return


""" Throwaway scrap function """
def scrap(args: argparse.Namespace) -> None:

    # https://stackoverflow.com/questions/14693701/how-can-i-remove-the-ansi-escape-sequences-from-a-string-in-python
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

    # Read in the text file.
    with open(args.inputfile, 'r', encoding='utf8') as infile, open(args.outputfile, 'w', errors='ignore') as outfile:
        lines = infile.readlines()
        # Print the line if it starts with a '│ ' or contains 'Animation frame:'.
        for line in lines:
            if 'Animation frame:' in line:
                print(line.strip(), end='')
                outfile.write(ansi_escape.sub('', line.strip()))
            if line.startswith('│ '):
                print(line.strip())
                outfile.write(line.strip() + '\n')

    return

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
""" Main """
def main():
    args = parse_args()
    if args is None:
        print('Problem!')
        exit()
    globals()[args.fn](args)
    return True

if __name__ == '__main__':
    config = Configs()
    main()
