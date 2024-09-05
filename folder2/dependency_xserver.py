################################################################################
## Author:       Juswaldy Jusman
## Date Created: 2020-10-20
## Description:  Generate a dependency table of Programmables, Views and Triggers
##               from a combination of SQL Server databases.
################################################################################
import argparse
import sys
import config
import pyodbc
import ahocorasick
import csv
import re
import datetime

reload(sys)
sys.setdefaultencoding('utf8')

''' Take care of arguments '''
def parse_args():
    desc = "Generate code dependency table from JZ, AQ, FR, ICS"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--targetfolder', type=str, default=None, help='Target folder to save the file', required=False)
    parser.add_argument('--pattern', type=str, default=None, help='Supply a name pattern matching the objects you want to pull', required=False)
    parser.add_argument('--reverse', action='store_true', help='Reverse caller/callee direction', required=False)
    return check_args(parser.parse_args())
def check_args(args):
    return args

''' Get objects and definitions from sql_modules '''
def get_objects(connection, name_pattern=None):
	conn = pyodbc.connect(connection, autocommit=True)
	conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-16le', ctype=pyodbc.SQL_WCHAR, to=str)
	cursor = conn.cursor()
	sql = """
		WITH base AS (
			SELECT
				m.object_id id,
				OBJECT_SCHEMA_NAME(m.object_id) schema_name,
				OBJECT_NAME(m.object_id) name,
				o.type_desc,
				m.definition
			FROM sys.sql_modules m
			JOIN sys.objects o ON m.object_id = o.object_id
		)
		SELECT *
		FROM base
	"""
	if name_pattern:
		sql += "WHERE name LIKE '{}'\n".format(name_pattern)
	sql += "ORDER BY name"
	cursor.execute(sql)
	rows = cursor.fetchall()
	cursor.close()
	return rows

''' Get a prefix from the object name, used for dividing up top levels in the tree '''
def get_name_prefix(name, style, segment):
	result = None
	if style == 'jz':
		segment = 2 if segment > 2 else segment
		result = '_'.join(name.split('_', 3)[0:segment])
	elif style == 'aq':
		if segment == 1:
			result = name.split('.')[0]
		else:
			segments = re.findall(r"([A-Z][^A-Z]*)", name.split('.')[1]) 
			result = '.'.join([name.split('.')[0], segments[0]]) if segments else None
	elif style == 'fr':
		result = '_'.join(name.split('_', 3)[0:segment])
		if segment == 1:
			result = name.split('.')[0]
		else:
			result = name.split('.')[1].split('_')[0]
	elif style == 'ics':
		segment = 2 if segment > 2 else segment
		result = '_'.join(name.split('_', 3)[0:segment])
	return result

''' Write csv row using the given writer. Check for blanks '''
def write_csv_row(writer, row):
	if row[0] and row[1] and row[0].strip() and row[1].strip():
		writer.writerow(row)

''' Format node name according to the given style '''
def get_nodename(row, style):
	result = row.name.upper() if style in ['jz', 'ics'] else '.'.join([row.schema_name.upper(), row.name.upper()])
	return result

''' Get original nodename '''
def get_nodename_original(row, style):
	return '.'.join([row.schema_name, row.name])

''' Format code definition according to the given style '''
def get_definition(row, style):
	return row.definition.upper()

def main():
	# Parse arguments.
	args = parse_args()
	if args is None:
		print("Problem!")
		exit()

	# Get object types and definitions from db.
	# Setup prefix and original name tables for the final tree.
	types = {}
	definitions = {}
	prefix0 = {}
	prefix1 = {}
	prefix2 = {}
	original_name = {}
	for conn in ['jz_PROD', 'aq_PROD', 'fr_PROD', 'ics_PROD']:
		style = conn.split('_')[0].lower()
		rows = get_objects(getattr(config, conn), args.pattern)
		for r in rows:
			if r.name and r.definition:
				nodename = get_nodename(r, style)
				nodename_original = get_nodename_original(r, style)
				definitions[nodename] = get_definition(r, style)
				prefix0[nodename] = style
				prefix1[nodename] = r.schema_name
				prefix2[nodename] = get_name_prefix(nodename_original, style, 3)
				original_name[nodename] = nodename_original
				if r.type_desc:
					types[nodename] = r.type_desc

	# Get list of object names.
	object_list = definitions.keys()

	# Create an object trie and make an Aho-Corasick automaton.
	A = ahocorasick.Automaton()
	for i, x in enumerate(object_list):
		A.add_word(x, (i, x))
	A.make_automaton()

	# Go through each object definition and collect the objects that are found in it.
	dependency_table = []
	callers = []
	callees = []
	for m, caller in enumerate(object_list):

		# Remove comments.
		definition = re.sub(r"(--[^\n]*\n)|((?s)/\*.*?\*/)", ' ', definitions[caller])

		# Find callees.
		for n, (i, callee) in A.iter(definition):
			j = n - len(callee) + 1 # Get the callee start index.

			# Sanity check: make sure the found item is in the specified index.
			# print((j, n, (i, callee)))
			assert definition[j:j + len(callee)] == callee

			# Initialize pair of caller/callee or callee/caller according to args.reverse.
			call_pair = [callee, caller] if args.reverse else [caller, callee]

			# Ignore these cases:
			# 1. Self-reference
			# 2. Callee is a substring of caller
			# 3. The pair is already in the table
			if not ( \
			caller == callee \
			or (callee in caller and len(callee) <= len(caller)) \
			or call_pair in dependency_table \
			):
				callers.append(caller)
				callees.append(callee)
				dependency_table.append(call_pair)

	# Get top level callers i.e. callers who are not also a callee.
	# Or bottom level callees i.e. callees who are not also a caller, if args.reverse is true.
	toplevel = []
	sentinel_pos = callees if args.reverse else callers
	sentinel_neg = callers if args.reverse else callees
	for x in sentinel_pos:
		if x not in sentinel_neg \
		and x not in toplevel:
			toplevel.append(x)

	# Divide up level 1 and 2 callers by the first 2 prefixes of the name.
	# Use level 0 for source database.
	level0 = []
	level1 = {}
	level2 = {}
	for x in toplevel:
		l0 = prefix0[x]
		l1 = prefix1[x]
		l2 = prefix2[x]
		if l0 not in level0: # Add source database.
			level0.append(l0)
		if l0 not in level1.keys(): # Set up level 1 callers.
			level1[l0] = []
		if l1 not in level1[l0]: # Add level 1 callers.
			level1[l0].append(l1)
		if l1 == l2: # If same, then grab first 10 characters.
			l1 = l2[0:10]
		if l1 not in level2.keys(): # Set up level 2 callers.
			level2[l1] = []
		if l2 not in level2[l1]: # Add level 2 callers.
			level2[l1].append(l2)

	# Write into csv.
	rootnode = 'SQL'
	project = 'Callers' if args.reverse else 'Deps'
	with open('{}/Tree_{}_{}_{}.txt'.format(args.targetfolder, project, rootnode, datetime.date.today()), 'w') as outfile:
		writer = csv.writer(outfile, delimiter='	')
		write_csv_row(writer, ['Parent', 'Child', 'Comment'])
		for l0 in level0:
			write_csv_row(writer, [rootnode, l0, 'Level 0'])
			for l1 in level1[l0]:
				write_csv_row(writer, [l0, l1, 'Level 1'])
				for l2 in level2[l1]:
					write_csv_row(writer, [l1, l2, 'Level 2'])
		for t in toplevel:
			write_csv_row(writer, [get_name_prefix(original_name[t], style, 3), original_name[t], types[t]])
		for p in dependency_table:
			write_csv_row(writer, [original_name[p[0]], original_name[p[1]], types[p[1]]])

if __name__ == '__main__':
    main()
