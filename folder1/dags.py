#!/usr/bin/python

import argparse
import csv
import json
from glob import glob
from tqdm import tqdm
import os
import re

"""our helper class"""
class Dags:
    essays = {
        'app': {
            'source': 'EnrollmentrxRx__Enrollment_Opportunity__c.csv',
            'id': 'EnrollmentrxRx__Applicant__c',
            'files': {
                'aboutme.csv': ['About_Me__c'],
                'education.csv': ['Educ_Career_Goals__c', 'Teaching_Experiences__c'],
                'nursing.csv': ['Nursing_Career_Goals__c', 'Nursing_Career_Reasons__c', 'Nursing_Communication_Skills__c', 'Nursing_Crim_Impairment_Expl__c', 'Nursing_Impairments__c', 'Nursing_Other_Questions__c', 'Nursing_Previous_App_Date__c', 'Nursing_Previous_App__c', 'Nursing_Registration_Number__c', 'Nursing_Rigor_Challenge__c', 'Nursing_Self_Motivation__c', 'Nursing_Volunteer_Exp__c', 'Nursing_What_it_offers__c', 'Nursing_What_you_offer__c', 'Nursing_Why_TWU__c'],
                'lifeexp.csv': ['Significant_Life_Experience__c'],
                'extracurr.csv': ['Extracurricular__c'],
                'questions.csv': ['Questions__c'],
                'whyapply.csv': ['Why_Applying__c'],
                'activities.csv': ['Activities__c']
            }
        },
        'contact': {
            'source': 'Contact.csv',
            'id': 'Id',
            'files': {
                'inquiry.csv': ['Comments_Inquiry__c', 'Description']
            }
        }
    }

    """Read Vena hierarchy file and insert intermediate grouping nodes"""
    def groupings(self, args):
        with open(args.inputfile, 'r') as infile, open(args.outputfile, 'w', newline='') as outfile:
            reader = csv.DictReader(infile)
            edges = {}

            # Get non-course-patterned parents who own course-patterned children.
            for row in reader:
                child = row['_member_name']
                parent = row['_parent_name']
                if re.search(r'[A-Z]{3,4}\s{1,2}\d{3}', child) and not re.search(r'[A-Z]{3,4}\s{1,2}\d{3}', parent):
                    if parent in edges.keys():
                        edges[parent].append(child)
                    else:
                        edges[parent] = [child]

            # Go through those parent-children households and pick those that pass some thresholds.
            # Create intermediate groupings for these households.
            grouped_households = {}
            threshold_count = 1 # Number of children.
            threshold_type = 1 # Number of different types of children.
            group_id = 0
            for (parent, children) in edges.items():
                types = set([x.split(' ')[0] for x in children])
                if len(children) > threshold_count and len(types) > threshold_type:
                    group_id += 1
                    grouped_children = {}
                    for t in types:
                        group_name = f'{t} Courses #{group_id:04d}'
                        grouped_children[group_name] = [x for x in children if x.split(' ')[0] == t]
                    grouped_households[parent] = grouped_children

            print(f'Found {len(grouped_households)} households to group.')

            # Go through the edges again and replace grouped parents with the created intermediate grouped parents.
            infile.seek(0)
            next(reader, None) # Ignore the header.
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            writer.writeheader()
            for row in reader:
                parent = row['_parent_name']
                if parent in grouped_households.keys():
                    grouped_children = grouped_households[parent]
                    for (newparent, children) in grouped_children.items():
                        row['_parent_name'] = parent
                        row['_member_name'] = newparent
                        row['_member_alias'] = newparent
                        writer.writerow(row)
                        for c in children:
                            row['_parent_name'] = newparent
                            row['_member_name'] = c
                            row['_member_alias'] = c
                            writer.writerow(row)
                    grouped_households[parent] = {}
                else:
                    writer.writerow(row)

    """Read Vena hierarchy file and generate a d3 tree visualization file"""
    def hierarchy2viz(self, args):
        with open(args.inputfile, 'r') as infile, open(args.outputfile, 'w') as outfile:
            reader = csv.DictReader(infile)
            outfile.write('parent\tchild\tnote\n')
            for row in reader:
                parent = row['_parent_name']
                child = row['_member_name']
                alias = row['_member_alias']
                if len(parent) == 0:
                    parent = 'Course'
                outfile.write(f'{parent}\t{child}\t{alias}\n')

"""parsing and configuration"""
def parse_args():
    desc = "Dags tasks"
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('--task', type=str, default=None, help='Which task?', required=True, choices=['groupings', 'hierarchy2viz'])

    parser.add_argument('--inputfolder', type=str, default=None, help='Input folder')
    parser.add_argument('--inputfile', type=str, default=None, help='Input file')
    parser.add_argument('--outputfolder', type=str, default=None, help='Output folder')
    parser.add_argument('--outputfile', type=str, default=None, help='Output file')
    parser.add_argument('--numfiles', type=int, default=None, help='Number of files')

    return check_args(parser.parse_args())

"""checking arguments"""
def check_args(args):
    return args

if __name__ == '__main__':
    # Parse arguments.
    args = parse_args()
    if args is None:
        print("Problem!")
        exit()

    f = getattr(Dags(), args.task)
    f(args)

