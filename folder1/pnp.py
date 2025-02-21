#!/usr/bin/python
"""
Copyright (c) 2022-2024 J.Jusman

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
import pathlib
from shutil import copyfile
import subprocess
from bs4 import BeautifulSoup
import random

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
""" Configs """
class Configs:
    def __init__(self, **kwargs):

        self.debugging = True
        self.newline = '\n'
        self.image_ext = 'png'
        self.fieldnames = ['id', 'prompt', 'frameno']
        self.granularity = 50
        self.randominterval = range(self.granularity, self.granularity*2)
        self.stripquotes = True
        self.choices = [
            'scrap',
            'collect_docs'
        ]

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
""" Argument parsing and checking """
def parse_args() -> argparse.Namespace:
    desc = 'Tools for working with transformer prompts'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--fn', type=str, default=None, help='Which function?', required=True, choices=config.choices)
    parser.add_argument('--inputfile', type=str, default=None, help='Input file', required=False)
    parser.add_argument('--outputfile', type=str, default=None, help='Output file', required=False)
    parser.add_argument('--inputfolder', type=str, default=None, help='Input folder (multiple ok, separated by comma)', required=False)
    parser.add_argument('--outputfolder', type=str, default=None, help='Output folder', required=False)
    parser.add_argument('--str', type=str, default=None, help='Miscellaneous string parameter', required=False)
    parser.add_argument('--int', type=int, default=None, help='Miscellaneous integer parameter', required=False)
    parser.add_argument('--float', type=float, default=None, help='Miscellaneous float parameter', required=False)
    return check_args(parser.parse_args())
def check_args(args: argparse.Namespace) -> argparse.Namespace:
    """ No checks for now. """
    return args

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
""" Helper functions """


""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
""" Exposed functions """

""" Collect all the documents in the input folder and write them to the output file. """
def collect_docs(args: argparse.Namespace) -> None:
    import pypdf, docx
    with open(f'{args.inputfolder}/{args.outputfile}', 'w', encoding='utf-8') as outfile:
        files = glob.glob(f'{args.inputfolder}/*.pdf') + glob.glob(f'{args.inputfolder}/*.docx')
        for file in files:
            print(file)
            if file.endswith('.pdf'):
                pdf = pypdf.PdfReader(file)
                text = [ page.extract_text().strip() for page in pdf.pages ]
            elif file.endswith('.docx'):
                doc = docx.Document(file)
                text = [ p.text.strip() + '\n' for p in doc.paragraphs ]
            outfile.write(''.join(text))
            outfile.write('\n\n' + 80*'#' + '\n\n')
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
