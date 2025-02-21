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
import pythoncom
import win32com.client

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
""" Configs """
class Configs:
    def __init__(self, **kwargs):

        """ SQL Agent """
        self.newline = '\n'
        self.header_stop = 'DECLARE @jobId BINARY(16)'
        self.jobstep_stop = 'EXEC @ReturnCode = msdb.dbo.sp_update_job'
        self.jobschedule_stop = 'EXEC @ReturnCode = msdb.dbo.sp_add_jobserver'
        self.step_delimiter = 'IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback'

        """ SQL Exporter/Importer """
        self.batchsize = 100 # Number of rows per INSERT statement.

        self.enumerate_sourcetables = False
        self.debugging = True

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
""" Argument parsing and checking """
def parse_args() -> argparse.Namespace:
    desc = 'Tools for working with SQL and SQL Agent files'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--fn', type=str, default=None, help='exportagents, agent2json, json2agent, json2mermaid, metanotebook2initsql, metanotebook2json, allagents', required=True)
    parser.add_argument('--str', type=str, default=None, help='String parameter', required=False)
    parser.add_argument('--inputfolder', type=str, default=None, help='Input folder', required=False)
    parser.add_argument('--outputfolder', type=str, default=None, help='Output folder', required=False)
    parser.add_argument('--inputfile', type=str, default=None, help='Input file', required=False)
    parser.add_argument('--outputfile', type=str, default=None, help='Output file', required=False)
    parser.add_argument('--istrue', action='store_true', help='Flag as true?', required=False)
    return check_args(parser.parse_args())
def check_args(args: argparse.Namespace) -> argparse.Namespace:
    """ No checks for now. """
    return args

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
""" Helper functions """
def parse_keyvalue(line: str) -> Tuple[str, str]:
    key, value = line.split('=')[-2].split('@')[-1], line.split('=')[-1].split(',')[0]
    return key, value

# Parse a multi-line key-value pair.
def parse_multiperline(r: str, thedict: dict) -> Tuple[dict, str]:
    items = [ item for item in r.split(', ') if len(item) > 0 ]
    for item in items:
        key, value = parse_keyvalue(line=item)
        if key != 'job_id':
            thedict[key] = value

def parse_3parts(row: object, anchor1: str, anchor2: str) -> Tuple[dict, str]:
    thedict = {}
    r = next(row)
    if not r.startswith(config.jobstep_stop):
        key, value = parse_keyvalue(line=r)
        thedict[key] = value
        
        # Parse until the first anchor.
        r = next(row)
        while not re.search(anchor1, r):
            parse_multiperline(r=r, thedict=thedict)
            r = next(row)

        # Parse the first anchor, which could be multiple lines, until the second anchor.
        if anchor1 == r'\s*@command':
            key, value = r.split('=', maxsplit=1)[-2].split('@')[-1], r.split('=', maxsplit=1)[-1].split(r',$')[0]
        else:
            key, value = parse_keyvalue(r)
        r = next(row)
        
        # If schedule_uid, we're done.
        if anchor1 == r'\s*@schedule_uid':
            thedict[key] = value
            r = next(row)
        else:

            # Only search for the second anchor if the command doesn't begin with these exceptions.
            endofcommand = r'.*, $' if re.search(r"N'(\/Server|sqlcmd|\$Error|cmd|C:\\Scripts|copy|del)", value, flags=re.IGNORECASE) else anchor2
            isErrorhandler = re.search(r"N'\$Error", value)

            # If looking for anchor2, continue collecting the command.
            if endofcommand == anchor2:
                # Search till the end of the command.
                while not re.search(endofcommand, r):
                    value += config.newline + r
                    r = next(row)
                thedict[key] = value[:-2] if re.search(r'.*, $', value) else value
                key, value = parse_keyvalue(r)
                thedict[key] = value
                r = next(row)
            else: # Otherwise, we're done.
                if isErrorhandler:
                    while not re.search(endofcommand, r):
                        value += config.newline + r
                        r = next(row)
                    value += config.newline + r
                if re.search(endofcommand, value):
                    thedict[key] = value[:-2] if re.search(r'.*, $', value) else value
                    if not isErrorhandler:
                        key, value = parse_keyvalue(r)
                        thedict[key] = value
                    r = next(row)

            # Parse the rest.
            while not r == config.step_delimiter:
                parse_multiperline(r=r, thedict=thedict)
                r = next(row)

    return thedict, r

def cleanup(inputlines: list) -> list:
    cleanedlines = []
    for line in inputlines:
        if re.search(r'@notify_email_operator_name=.*, @job_id', line) or re.search(r'@os_run_priority=.*, @subsystem', line):
            items = [ item.strip(' \t') for item in line.split(', ') ]
            cleanedlines.append(f'\t\t{items[0]}, ')
            cleanedlines.append(f'\t\t{items[1]}' + (', ' if len(items) > 2 else ''))
        else:
            cleanedlines.append(line)
    return cleanedlines

def enumerate_params(thedict: dict) -> list:
    result = [ f"\t\t@{k}={thedict[k]}, " for k in list(thedict.keys())[1:-1] ]
    result.extend([f"\t\t@{k}={thedict[k]}" for k in [list(thedict.keys())[-1]]])
    return result

def strip_nvarchar(line: str) -> str:
    return line[2:-1]

def parse_targets(proc_str: str, keyword: str) -> list:
    return list(set(re.findall(r'{}\s+([^@#][^\s;]*)'.format(keyword), proc_str, re.IGNORECASE)))

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
""" Export functions """
def exportagents(args: argparse.Namespace) -> None:
    server_instance = args.str
    output_dir = args.outputfolder

    pythoncom.CoInitialize()
    server = win32com.client.Dispatch("Microsoft.SqlServer.Management.Smo.Server")
    server.ConnectionContext.ServerInstance = server_instance
    server.ConnectionContext.LoginSecure = True  # Use Windows Authentication

    os.makedirs(output_dir, exist_ok=True)

    for job in server.JobServer.Jobs:
        job_name = job.Name.replace(" ", "_")
        file_path = os.path.join(output_dir, f"{job_name}.sql")
        
        with open(file_path, "w", encoding="utf-8") as f:
            script = job.Script()
            f.write(script)
        
        print(f"Exported job '{job.Name}' to {file_path}")

    pythoncom.CoUninitialize()

    return

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
""" Conversion functions """
def agent2json(inputfile: str, outputfile: str = None) -> str:

    # Figure out filenames and components.
    if outputfile is None:
        folder = os.path.dirname(inputfile)
        filename = os.path.basename(inputfile)
    else:
        folder = os.path.dirname(outputfile)
        filename = os.path.basename(outputfile)
    components = filename.split('.')
    basename = '.'.join(components[:-1])
    cleanedfile = os.path.join(folder, f'{basename}.sql.cleaned')
    jsonfile = os.path.join(folder, f'{basename}.json') if outputfile is None else outputfile

    # Read and cleanup the input file.
    with open(inputfile, 'r') as infile, open(cleanedfile, 'w') as cleanedfile:
        cleanedlines = cleanup(infile.read().split(config.newline))
        cleanedfile.write(config.newline.join(cleanedlines))
        row = iter(cleanedlines)

    # Read in the agent file.
    with open(jsonfile, 'w') as outfile:

        # Parse the rows.
        r = ''

        # Set up the job dictionary.
        thejob = {}

        # Header.
        header = []
        while not r.startswith(config.header_stop):
            r = next(row)
            header.append(r)

        ########################################
        # Job metadata.
        jobmeta, r = parse_3parts(row=row, anchor1=r'\s*@description', anchor2=r'\s*@category_name')

        ########################################
        # Job steps
        jobsteps = []
        while not r.startswith(config.jobstep_stop):
            jobstep, r = parse_3parts(row=row, anchor1=r'\s*@command', anchor2=r'\s*@database_name')
            jobsteps.append(jobstep) if jobstep else None

        ########################################
        # Start step id.
        start_step_id = r.split('=')[3].strip()
        r = next(row)
        r = next(row)

        ########################################
        # Job schedule.
        jobschedules = []
        while not r.startswith(config.jobschedule_stop):
            jobschedule, r = parse_3parts(row=row, anchor1=r'\s*@schedule_uid', anchor2=r'.*')
            jobschedules.append(jobschedule) if jobschedule else None

        ########################################
        # Server name.
        if r.startswith('EXEC @ReturnCode = msdb.dbo.sp_add_jobserver'):
            server_name = r.split('=')[3].strip()
            r = next(row)

        ########################################
        # Footer.
        footer = []
        while r:
            footer.append(r)
            r = next(row)

        thejob['header'] = header
        thejob['jobmeta'] = jobmeta
        thejob['jobsteps'] = jobsteps
        thejob['start_step_id'] = start_step_id
        thejob['jobschedules'] = jobschedules
        thejob['server_name'] = server_name
        thejob['footer'] = footer

        json.dump(thejob, outfile, indent=4)

    return jsonfile

def json2agent(inputfile: str, outputfile: str) -> None:
    with open(inputfile, 'r') as infile, open(outputfile, 'w') as outfile:
        thejob = json.load(infile)
        jobsql = []
        jobheader = thejob['header']
        anchor = jobheader.index('SELECT @ReturnCode = 0')
        jobheader.insert(anchor, '')
        jobheader.insert(anchor, 'IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback')
        jobheader.insert(anchor, f"EXEC @ReturnCode = msdb.dbo.sp_delete_job @job_name={thejob['jobmeta']['job_name']}")
        jobheader.insert(anchor, '')
        jobsql.extend(jobheader)
        jobsql.extend([f"EXEC @ReturnCode = msdb.dbo.sp_add_job @job_name={thejob['jobmeta']['job_name']}, "])
        jobsql.extend(enumerate_params(thedict=thejob['jobmeta']))
        jobsql.extend([config.step_delimiter])
        for jobstep in thejob['jobsteps']:
            jobsql.extend([f"EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @{k}={jobstep[k]}, " for k in [list(jobstep.keys())[0]]])
            jobsql.extend(enumerate_params(thedict=jobstep))
            jobsql.extend([config.step_delimiter])
        jobsql.extend([f"EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = {thejob['start_step_id']}"])
        jobsql.extend([config.step_delimiter])
        if thejob['jobschedule']:
            jobsql.extend([f"EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @{k}={thejob['jobschedule'][k]}, " for k in [list(thejob['jobschedule'].keys())[0]]])
            jobsql.extend(enumerate_params(thedict=thejob['jobschedule']))
        jobsql.extend([config.step_delimiter])
        jobsql.extend(["EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'"])
        jobsql.extend(thejob['footer'])
        outfile.write(config.newline.join(jobsql))
        outfile.write('\n\n')
    return outputfile

def json2mermaid(inputfile: str, outputfile: str) -> None:
    with open(inputfile, 'r') as infile, open(outputfile, 'w') as outfile:
        thejob = json.load(infile)
        jobsteps = thejob['jobsteps']
        jobgraph = []
        jobgraph.append(f'# {strip_nvarchar(thejob["jobmeta"]["job_name"])}')
        jobgraph.append('```mermaid')
        jobgraph.append('graph TD')
        jobgraph.extend([f"\t{jobstep['step_id']}[{jobstep['step_id'] + ' ' + strip_nvarchar(jobstep['step_name'])}]" for jobstep in jobsteps])
        for jobstep in jobsteps:
            step_name = strip_nvarchar(jobstep['step_name'])
            if re.search(r'Generate.*CSV', step_name, re.IGNORECASE):
                jobgraph.append(f"\t{jobstep['step_id']} --> {jobstep['step_id']}S[GENERATE_CSV]")
            elif re.search(r'Send.*Email', step_name, re.IGNORECASE):
                jobgraph.append(f"\t{jobstep['step_id']} --> {jobstep['step_id']}S[SEND_EMAIL]")
            else:
                jobgraph.append(f"\t{jobstep['step_id']} --> {jobstep['step_id']}S[{strip_nvarchar(jobstep['command'])[:120]}]".replace("''", "'").replace('\n', '').replace(';', ''))
        jobgraph.extend([f"\t{jobstep['step_id']} -- OK --> {int(jobstep['on_success_step_id']) if jobstep['on_success_action'] in ['1', '2'] else int(jobstep['step_id'])+1}" for jobstep in jobsteps])
        jobgraph.extend([f"\t{jobstep['step_id']} -- Fail --> {jobstep['on_fail_step_id']}" for jobstep in jobsteps])
        jobgraph.append('```')
        jobgraph.append('\n\n')
        outfile.write(config.newline.join(jobgraph))
    return outputfile

def jobsteps(inputfolder: str, outputfile: str) -> str:
    everything = []
    for f in glob.glob(f'{inputfolder}/*.json'):
        with open(f, 'r') as infile:
            print(f)
            thejob = json.load(infile)
            everything.append(strip_nvarchar(thejob['jobmeta']['job_name'].replace('FinancialReporting ', '')))
            everything.append(config.newline)
            for jobstep in thejob['jobsteps']:
                everything.append('\t{}. {} - {}'.format(jobstep['step_id'], strip_nvarchar(jobstep['step_name']), jobstep['command'][:120].replace(config.newline, ' ')))
                everything.append(config.newline)
        everything.append(config.newline)
    with open(outputfile, 'w') as outfile:
        outfile.writelines(everything)
    return outputfile

def jobsteps2depcsv(inputfolder: str, outputfile: str) -> str:
    from string import ascii_uppercase
    fields = ['job', 'jobname', 'step', 'procname', 'tablename', 'stepnum']
    with open('datacubes.json', 'r') as datacubes_file, open('sourcetables.json', 'r') as sourcetables_file, open(outputfile, 'w', newline='') as outfile:
        datacubes = {}
        jcubes = json.load(datacubes_file)
        for cube in jcubes:
            name = cube['DATACUBE_NAME']
            row = {}
            row['INTERSECTION'] = cube['INTERSECTION_TABLE'].replace('PRESENTATION.', '') if cube['INTERSECTION_TABLE'] else None
            row['LID'] = cube['LID_TABLE'].replace('PRESENTATION.', '') if cube['LID_TABLE'] else None
            row['DETAIL'] = cube['DETAIL_TABLE'].replace('PRESENTATION.', '') if cube['DETAIL_TABLE'] else None
            datacubes[name] = row
        # Add rolling 24 cubes.
        datacubes['TRIAL_BALANCE_CMP4'] = {'INTERSECTION_ROLLING24': 'INTERSECTION_TRIAL_BALANCE_CMP4'}
        datacubes['TRIAL_BALANCE_CMP5'] = {'INTERSECTION_ROLLING24': 'INTERSECTION_TRIAL_BALANCE_CMP5'}
        datacubes['ENROLLMENT'] = {'INTERSECTION_ROLLING24': 'INTERSECTION_ENROLLMENT'}
        datacubes['FINANCIAL_AID'] = {'INTERSECTION_ROLLING24': 'INTERSECTION_FINANCIAL_AID'}
        datacubes['DISTRIBUTED_COURSE_REVENUE'] = {'INTERSECTION_ROLLING24': 'INTERSECTION_DISTRIBUTED_COURSE_REVENUE'}
        datacubes['ENROLLMENT_AUXILIARY'] = {'INTERSECTION_ROLLING24': 'INTERSECTION_ENROLLMENT_AUXILIARY'}
        
        sourcetables = {}
        jtables = json.load(sourcetables_file)
        sourcetables['SOURCE_JZ'] = [ t for t in jtables if t['PROCESSING_TYPE'] == 'R' and t['HOST_NAME'] == 'JZ' ]
        sourcetables['SOURCE_AQ'] = [ t for t in jtables if t['PROCESSING_TYPE'] == 'R' and t['HOST_NAME'] == 'AQ' ]

        writer = csv.DictWriter(outfile, fields)
        writer.writeheader()
        i = 0
        for f in glob.glob(f'{inputfolder}/*.json'):
            with open(f, 'r') as infile:
                print(f)
                thejob = json.load(infile)
                jobname = strip_nvarchar(thejob['jobmeta']['job_name'].replace('FinancialReporting ', ''))
                source = ascii_uppercase[i] + '_' + '_'.join([ x for x in strip_nvarchar(thejob['jobmeta']['job_name'].replace('FinancialReporting ', '')).split()])
                for jobstep in thejob['jobsteps']:
                    step_name = strip_nvarchar(jobstep['step_name'])
                    row = {}
                    step = strip_nvarchar(jobstep['command'][:120].replace(config.newline, ' '))
                    step = re.sub(r'EXEC INPUT\.', '', step)
                    step = re.sub(r'SELECT 1.*', 'N_TEST', step)
                    procname = step
                    if step.upper().startswith('EXEC STAGING.VALIDATE_DATACUBE_COMPONENT'):
                        procname = 'VALIDATE_DATACUBE_COMPONENT'
                        step = 'VAL'
                    if step.upper().startswith('EXEC STAGING.LOAD_SOURCE_TABLES'):
                        m = re.search(r'EXEC STAGING.LOAD_SOURCE_TABLES .* \'\'([^\s]*)\'\';', step, re.IGNORECASE)
                        procname = 'LOAD_SOURCE_TABLES'
                        tablename = 'SOURCE_JZ' if m.group(1) == 'R' else m.group(1)
                        step = ''.join([t[0] for t in tablename.split('_')])
                        # Enumerate the source tables.
                        if config.enumerate_sourcetables and tablename in ['SOURCE_JZ', 'SOURCE_AQ']:
                            for t in sourcetables[tablename]:
                                row['job'] = source
                                row['jobname'] = jobname
                                row['step'] = step
                                row['procname'] = procname
                                row['tablename'] = t['TABLE_NAME']
                                row['stepnum'] = jobstep['step_id']
                                writer.writerow(row)
                    if step.upper().startswith('EXEC STAGING.PREPARE_STAGING_TABLES '):
                        m = re.search(r'EXEC STAGING.PREPARE_STAGING_TABLES \'\'(.*)\'\'', step, re.IGNORECASE)
                        procname = 'PREPARE_STAGING_TABLES'
                        tablename = m.group(1)
                        step = ''.join([t[0] for t in tablename.split('_')])
                    if step.upper().startswith('EXEC STAGING.GENERATE_'):
                        m = re.search(r'EXEC STAGING.(.*) \'\'(.*)\'\'', step, re.IGNORECASE)
                        procname = m.group(1)
                        m = re.search(r'EXEC STAGING.GENERATE_(.*) \'\'(.*)\'\'', step, re.IGNORECASE)
                        tablename = datacubes[m.group(2)][m.group(1)]
                        tablename = 'N_' + m.group(1) if not tablename else tablename
                        step = ''.join([t[0] for t in tablename.split('_')]) if tablename else None
                    if re.search(r'Generate.*CSV', step_name, re.IGNORECASE):
                        procname = 'GENERATE_CSV'
                        tablename = 'CSV'
                        step = tablename
                    if re.search(r'Send.*Email', step_name, re.IGNORECASE):
                        procname = 'SEND_DBMAIL'
                        tablename = step_name.upper().split()[1][0] + '_EMAIL'
                        step = tablename
                    if step_name == 'Check for errors':
                        procname = 'VALIDATE_DATACUBE_COMPONENT'
                        tablename = 'RUN_VALIDATION'
                        step = 'VAL'
                    if step_name == 'Copy Student Group Code by Period to Jenzabar':
                        procname = 'COPY'
                        tablename = 'TWU_STUDENT_GROUP_INTENT_BY_PERIOD'
                        step = ''.join([t[0] for t in tablename.split('_')])
                    row['job'] = source
                    row['jobname'] = strip_nvarchar(thejob['jobmeta']['job_name'].replace('FinancialReporting ', ''))
                    row['step'] = step
                    row['procname'] = procname
                    row['tablename'] = tablename
                    row['stepnum'] = jobstep['step_id']
                    writer.writerow(row) if step and len(step) > 0 else None
            i += 1
    return outputfile

def csv2treetxt(inputfile: str, outputfile: str) -> str:
    output = []
    with open(inputfile, 'r') as infile, open(outputfile, 'w') as outfile:
        reader = csv.reader(infile)
        next(reader, None) # Ignore the header
        rows = []
        app_categories = []
        category_templates = []
        parens_to_ltgt = str.maketrans('()', '<>')
        for row in reader:
            app, category, template, comment = [ x.translate(parens_to_ltgt) for x in row ]
            l1 = f'{app}\t{category}\tLevel 1'
            l2 = f'{category}\t{template}\t{comment}'
            if not l1 in app_categories:
                app_categories.append(l1)
            if not l2 in category_templates:
                category_templates.append(l2)
            rows.append(row)
        rows = np.array(rows)
        print(rows.shape)
        apps = np.unique(rows[:, 0])
        categories = np.unique(rows[:, 1])
        templates = np.unique(rows[:, 2])

        output.append(f'Parent\tChild\tComment')
        output.append(config.newline)
        output.extend(config.newline.join([f'Templates\t{x}\tLevel 0' for x in apps]))
        output.append(config.newline)
        output.extend(config.newline.join(sorted(app_categories)))
        output.append(config.newline)
        output.extend(config.newline.join(sorted(category_templates)))
        outfile.writelines(output)

    return outputfile

def metanotebook2initsql(inputfile: str, outputfile: str) -> str:
    with open(inputfile, 'r') as infile, open(outputfile, 'w') as outfile:
        nb = json.load(infile)['cells'][1:] # skip the first cell, which is the notebook title + instructions
        output = []
        for cell in nb:
            if cell['cell_type'] == 'code':
                source = re.sub('SELECT \* FROM ', '', cell['source'][0])
                rowcount = cell['outputs'][0]['data']['text/html'] 
                data = cell['outputs'][2]['data']['application/vnd.dataresource+json']
                fields = [ f['name'] for f in data['schema']['fields'] ]
                values = [ list(v.values()) for v in data['data'] ]
                metainfo = f'-- {source} Recorded: {rowcount} Actual: {len(values)}'
                print(metainfo)
                output.append(f'{metainfo}{config.newline}')
                output.append(f'SET IDENTITY_INSERT {source} ON;{config.newline}')
                output.append(f'TRUNCATE TABLE {source};{config.newline}')
                insertheader = 'INSERT INTO {} ({}) VALUES '.format(source, ', '.join(fields))
                if len(values) > 0:
                    for i in range(0, len(values), config.batchsize):
                        insertdetail = str(f',{config.newline}').join(['({})'.format(', '.join(["'{}'".format(re.sub("'", "''", str(v))) for v in value])) for value in values[i:i+100]])
                        insertdetail = re.sub("'NULL'", 'NULL', insertdetail) # Fix NULLs
                        insertdetail = re.sub('\r\n', config.newline, insertdetail) # Fix CRLFs
                        output.append(f'{insertheader}{config.newline}{insertdetail};{config.newline}')
                output.append(f'SET IDENTITY_INSERT {source} OFF;{config.newline}')
            output.append(config.newline)
        outfile.writelines(output)

    return outputfile

def metanotebook2json(inputfile: str, outputfile: str) -> str:
    with open(inputfile, 'r') as infile, open(outputfile, 'w') as outfile:
        nb = json.load(infile)['cells'][1:] # skip the first cell, which is the notebook title + instructions
        output = []
        for cell in nb:
            if cell['cell_type'] == 'code':
                source = re.sub('SELECT \* FROM ', '', cell['source'][0])
                rowcount = cell['outputs'][0]['data']['text/html'] 
                data = cell['outputs'][2]['data']['application/vnd.dataresource+json']
                fields = [ f['name'] for f in data['schema']['fields'] ]
                values = [ list(v.values()) for v in data['data'] ]
                metainfo = f'-- {source} Recorded: {rowcount} Actual: {len(values)}'
                print(metainfo)
                outputrow = {}
                outputrow['source'] = source
                outputrow['data'] = []
                if len(values) > 0:
                    for fieldvalues in values:
                        row = {}
                        for k, v in zip(fields, fieldvalues):
                            row[k] = v
                        outputrow['data'].append(row)
                    output.append(outputrow)

        json.dump(output, outfile, indent=4)

    return outputfile

def metajson2depcsv(inputfile: str, outputfile: str) -> str:
    possible_keywords = [ 'FROM', 'EXEC', 'EXECUTE', 'INTO', 'UPDATE' ]
    outfields = [ 'source', 'target', 'filter', 'freshness', 'description' ]
    presentationfilename = 'presentation.csv'
    with open(inputfile, 'r') as infile, open(outputfile, 'w', newline='') as outfile, open(presentationfilename, 'w', newline='') as presentationfile:
        writer = csv.DictWriter(outfile, outfields)
        writer.writeheader()
        writer_p = csv.DictWriter(presentationfile, ['schemaname', 'tablename'])
        writer_p.writeheader()
        meta = json.load(infile)
        for m in meta:
            if m['source'] == 'META.STAGING_TABLE':
                for d in m['data']:
                    source = d['TABLE_NAME']
                    print(source) if config.debugging else None
                    proc = d['STAGING_PROCEDURE'].replace('[', '').replace(']', '')
                    for keyword in [ 'FROM', 'EXEC' ]:
                        for x in parse_targets(proc, keyword):
                            row = {}
                            row['source'] = source
                            row['target'] = x.split('.')[-1]
                            writer.writerow(row) if row['source'] != row['target'] else None
            elif m['source'] == 'META.DATACUBE':
                for d in m['data']:
                    for proc in [ 'INTERSECTION', 'LID', 'DETAIL' ]:
                        source = d[f'{proc}_TABLE'].replace('PRESENTATION.', '')
                        if source != 'NULL':
                            print(source) if config.debugging else None
                            writer_p.writerow({'schemaname': 'PRESENTATION', 'tablename': source})
                            for keyword in [ 'FROM', 'EXEC' ]:
                                for x in parse_targets(d[f'{proc}_PROCEDURE'].replace('[', '').replace(']', ''), keyword):
                                    row = {}
                                    row['source'] = source
                                    row['target'] = x.split('.')[-1]
                                    writer.writerow(row) if row['source'] != row['target'] and '.' in x else None

    return outputfile

def sql2depcsv(inputfile: str, outputfile: str) -> str:
    outfields = [ 'source', 'target', 'filter', 'freshness', 'description' ]
    with open(inputfile, 'r') as infile, open(outputfile, 'w') as outfile:
        writer = csv.DictWriter(outfile, outfields)
        writer.writeheader()
        proc = ''.join(infile.readlines())
        target = os.path.splitext(os.path.basename(inputfile))[0]
        rows = []
        for keyword in [ 'FROM', 'EXEC' ]:
            for x in parse_targets(proc, keyword):
                row = {}
                row['source'] = x
                row['target'] = target
                if row['source'] != row['target'] and row not in rows:
                    rows.append(row)
                    writer.writerow(row) 
        for keyword in [ 'INTO', 'UPDATE' ]:
            for x in parse_targets(proc, keyword):
                row = {}
                row['source'] = target
                row['target'] = x.replace('(', '')
                if row['source'] != row['target'] and row not in rows and {'source': row['target'], 'target': row['source']} not in rows:
                    rows.append(row)
                    writer.writerow(row) 
    return outputfile

def allagents(args: argparse.Namespace) -> None:
    inputfolder = args.inputfolder
    outputfolder = args.outputfolder
    for f in glob.glob(f'{inputfolder}/*.sql'):
        print(f)
        agent2json(inputfile=f, outputfile=f.replace(inputfolder, outputfolder).replace('.sql', '.json'))
        # json2mermaid(inputfile=f.replace(inputfolder, outputfolder).replace('.sql', '.json'), outputfile=f.replace(inputfolder, outputfolder).replace('.sql', '.md'))
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
