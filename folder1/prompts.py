#!/usr/bin/python
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
        self.thumbnail_size = (100, 100)
        self.fieldnames = ['id', 'prompt', 'frameno']
        self.granularity = 50
        self.randominterval = range(self.granularity, self.granularity*2)
        self.stripquotes = True
        self.deforum_fields = [
            'batch_name',
            'sd_model_name', 'sd_model_hash',
            'fps',
            'W', 'H',
            'restore_faces',
            'seed',
            'sampler',
            'steps',
            'positive_prompts', 'negative_prompts',
            'animation_mode',
            'max_frames',
            'zoom',
            'translation_x', 'translation_y', 'translation_z',
            'rotation_3d_x', 'rotation_3d_y', 'rotation_3d_z',
            'cn_1_module', 'cn_1_model',
        ]
        self.motion_fields = [
            'frameno',
            'prompt',
            '2d-zoom',
            '2d-angle',
            '2d-transformcenterx',
            '2d-transformcentery',
            'shared-translationx',
            'shared-translationy',
            '3d-translationz',
            '3d-rotationx', # roll
            '3d-rotationy', # yaw
            '3d-rotationz', # pitch
        ]
        self.choices = [
            'scrap',
            'text2csv',
            'csv2json',
            'prepdeforum',
            'concatimages',
            'images2html',
            'cheatsheet2csv',
            'deforumcsv',
            'prompt2styled',
            'motion2md',
            'rels2csv'
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

""" Convert text file to csv id,text,frame file """
def text2csv(args: argparse.Namespace) -> None:
    inputfile, outputfile, userandomintervals = args.inputfile, args.outputfile, (args.int == 1)
    with open(inputfile, 'r') as infile, open(outputfile, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(config.fieldnames)
        frameno = 0
        for i, line in enumerate(infile):
            outtext = line.strip()
            outtext = outtext.replace("'", "\'").replace('"', '\"') if config.stripquotes else outtext
            output = [i, outtext, frameno]
            frameno += (random.choice(config.randominterval) if userandomintervals else 0)
            writer.writerow(output)
            print(output)
    return

""" Convert csv file to json frameno-prompttext file """
def csv2json(args: argparse.Namespace) -> None:
    inputfile, outputfile = args.inputfile, args.outputfile
    with open(inputfile, 'r') as infile, open(outputfile, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        output = {}
        for r in reader:
            output[r['frameno']]= r['text']
            print(r['text'])
        json.dump(output, outfile, indent=4)
    return

""" Prep and clean up deforum folders """
def prepdeforum(args: argparse.Namespace) -> None:
    inputfolder, outputfolder, component = args.inputfolder, args.outputfolder, args.str
    deforum_fields = config.deforum_fields
    pathlib.Path(f'{outputfolder}').mkdir(parents=True, exist_ok=True)
    with open(f'{outputfolder}/x.csv', 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, ['id'] + deforum_fields + ['prompt_0'])
        writer.writeheader()
        i = 1
        for d in sorted(glob.glob(f'{inputfolder}/Deforum_*')):
            print(d)

            # Create csv row.
            with open(glob.glob(f'{d}/*_settings.txt')[0], 'r') as infile:
                settings = json.load(infile)
                row = {}
                for f in deforum_fields:
                    row[f] = settings[f] if f in settings else ''
                row['id'] = i
                row['prompt_0'] = settings['prompts']['0']
                writer.writerow(row)

            if component == 'mp4':
                # Copy mp4 file.
                sourcemp4 = glob.glob(f'{d}/*.mp4')[0] if len(glob.glob(f'{d}/*.mp4')) > 0 else None
                print(sourcemp4, i)
                copyfile(sourcemp4, f'{outputfolder}/{i}.mp4') if sourcemp4 else None
            elif component == 'images':
                # Copy and resize images.
                pathlib.Path(f'{outputfolder}/{i}').mkdir(parents=True, exist_ok=True)
                j = 1
                skip = 5
                print(d, end='')
                for f in sorted(glob.glob(f'{d}/*.{config.image_ext}')):
                    if j % skip == 1:
                        targetfile = f'{outputfolder}/{i}/{j:07}.{config.image_ext}'
                        print('.', end='', flush=True)
                        subprocess.run(['convert', f'{f}', '-resize', 'x'.join([str(i) for i in config.thumbnail_size]), f'{targetfile}'])
                    j += 1
                print('Done')
            elif component == 'html':
                # Create html file for viewing images.
                targetfile = f'{outputfolder}/{i}.html'
                with open(targetfile, 'w') as outfile:
                    outfile.write('<html><body>')
                    cwd = os.getcwd()
                    os.chdir(outputfolder)
                    for f in sorted(glob.glob(f'{i}/*.{config.image_ext}')):
                        outfile.write(f'<img src="{f}">')
                    os.chdir(cwd)
                    outfile.write('</body></html>')
                    print(targetfile)

            i += 1

    # Create tar ball.
    subprocess.run(['tar', '-czvf', f'{outputfolder}-{component}.tar.gz', f'{outputfolder}'])

    return

""" Copy images from multiple folders into one folder and renumber them """
def concatimages(args: argparse.Namespace) -> None:
    inputfolders, outputfolder = args.inputfolder, args.outputfolder
    pathlib.Path(f'{outputfolder}').mkdir(parents=True, exist_ok=True)
    i = 1
    for d in inputfolders:
        for f in sorted(glob.glob(f'{d}/*.{config.image_ext}')):
            copyfile(f, f'{outputfolder}/{i:07}.{config.image_ext}')
            print(f, f'{outputfolder}/{i:07}.{config.image_ext}')
            i += 1
    return

""" Convert cheatsheet html to csv """
def cheatsheet2csv(args: argparse.Namespace) -> None:
    inputfile, outputfile = args.inputfile, args.outputfile
    fieldnames = [ 'id', 'name', 'birth', 'death', 'checkpoint', 'categories', 'extrainfo' ]
    with open(inputfile, 'r', encoding='utf8') as infile, open(outputfile, 'w', newline='', encoding='utf8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames)
        writer.writeheader()

        categories = []

        # Read the html file and parse each row into the csv.
        cheats = BeautifulSoup(infile.read(), 'html.parser')
        for c in cheats.find('table', id='listedartists').contents[0]:
            if len(c.find_all('td')) > 0:
                row = {}
                for i, td in enumerate(c.find_all('td')):
                    row[fieldnames[i]] = td.text.strip()
                print(row) if config.debugging else None

                # Gather categories.
                for c in row['categories'].split(','):
                    c = c.strip()
                    if c not in categories:
                        categories.append(c)

                writer.writerow(row)
        
        # Write categories.
        print(categories)
    return

""" Generate deforum planning csv """
def deforumcsv(args: argparse.Namespace) -> None:
    inputfile, outputfile, maxframes, granularity = args.inputfile, args.outputfile, args.int, config.granularity
    
    # Read input csv for the prompt-frameno pairs. Then write the prompt at the appropriate frame in the output csv.
    with open(inputfile, 'r', encoding='utf8') as infile, open(outputfile, 'w', newline='', encoding='utf8') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, config.motion_fields)
        writer.writeheader()
        inrow = next(reader)
        allgone = False
        for n in range(0, maxframes+1, granularity):
            outrow = {}
            outrow['frameno'] = n
            
            # Write the prompt at the appropriate frame.
            if (not allgone) and int(inrow['frameno']) <= n:
                outrow['prompt'] = inrow['prompt']
                try:
                    inrow = next(reader)
                except StopIteration:
                    allgone = True

            # Write initial values in the first row.
            if n == 0:
                outrow['2d-zoom'] = 0.995
                outrow['2d-angle'] = 0
                outrow['2d-transformcenterx'] = 0.5
                outrow['2d-transformcentery'] = 0.5
                outrow['shared-translationx'] = 0
                outrow['shared-translationy'] = 0
                outrow['3d-translationz'] = 1.75
                outrow['3d-rotationx'] = 0
                outrow['3d-rotationy'] = 0
                outrow['3d-rotationz'] = 0

            writer.writerow(outrow)
    return

""" Take a csv of prompt-frameno pairs and apply styles + frameno dilation/contraction """
def prompt2styled(args: argparse.Namespace) -> None:
    inputfile, outputfile, multiplier = args.inputfile, args.outputfile, args.float
    minstyles, maxstyles = 1, 3
    with open('sd/favs.txt', 'r', encoding='utf8') as stylefile, open(inputfile, 'r', encoding='utf8') as infile, open(outputfile, 'w', newline='', encoding='utf8') as outfile:
        styles = set(stylefile.read().split('\n')) - {''}
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, reader.fieldnames)
        writer.writeheader()
        for row in reader:
            s = ', '.join([ 'style of ' + e for e in random.sample(list(styles), random.randint(minstyles, maxstyles))])
            row['prompt'] = f'{row["prompt"]}, {s}'
            row['frameno'] = int(float(row['frameno']) * multiplier)
            writer.writerow(row)
            print(row) if config.debugging else None
    return

""" Read in the deforum-motion csv and output a markdown file with the parameters """
def motion2md(args: argparse.Namespace) -> None:
    promptbase, inputfolder = args.str, args.inputfolder
    inputfile, outputfile = f'{inputfolder}/{promptbase}-deforum-motion.csv', f'{inputfolder}/{promptbase}-deforum-motion.md'
    with open(inputfile, 'r', encoding='utf8') as infile, open(outputfile, 'w', encoding='utf8') as outfile:
        reader = csv.DictReader(infile)
        
        # Setup the values dict.
        values = {}
        for f in config.motion_fields:
            values[f] = {}
        
        # Populate the values dict.
        for row in reader:
            print(row) if config.debugging else None
            for f in config.motion_fields:
                if row[f]:
                    if f == 'prompt':
                        values[f][row['frameno']] = row[f]
                    else:
                        values[f][row['frameno']] = f'({row[f]})'
            
        outfile.write(f'# Deforum Motion Parameters - {promptbase}\n\n')
        outfile.write(f'## Generic\n\n')
        outfile.write(f'- Source file: `{inputfile}`\n')
        outfile.write(f'- Check: `Sampler, Step, Width, Height, Seed, Max Frames`\n')
        outfile.write(f'- Positive prompts: `very detailed, highly intricate, high focus, masterpiece,`\n')
        outfile.write(f'- Negative Prompts: `nude, naked, nsfw, nipple, genitals, penis, vagina, mutation, deformed, deformed iris, duplicate, morbid, mutilated, disfigured, poorly drawn hand, poorly drawn face, bad proportions, gross proportions, extra limbs, cloned face, long neck, malformed limbs, missing arm, missing leg, extra arm, extra leg, fused fingers, too many fingers, extra fingers, mutated hands, blurry, bad anatomy, out of frame, contortionist, contorted limbs, exaggerated features, disproportionate, twisted posture, unnatural pose, disconnected, disproportionate, warped, misshapen, out of scale`\n\n')
        outfile.write(f'## Prompts\n\n')
        outfile.write(f'```json\n')
        outfile.write(json.dumps(values["prompt"], indent=4, ensure_ascii=False) + '\n')
        outfile.write(f'```\n\n')
        outfile.write(f'## 2D Mode\n\n')
        outfile.write(f'|Parameter|Value|\n|---|---|\n')
        outfile.write(f'|Zoom|`' + json.dumps(values['2d-zoom']).replace('"', '') + '`|\n')
        outfile.write(f'|Angle|`' + json.dumps(values["2d-angle"]).replace('"', '') + '`|\n')
        outfile.write(f'|Transform Center X|`' + json.dumps(values["2d-transformcenterx"]).replace('"', '') + '`|\n')
        outfile.write(f'|Transform Center Y|`' + json.dumps(values["2d-transformcentery"]).replace('"', '') + '`|\n')
        outfile.write(f'|Translation X|`' + json.dumps(values["shared-translationx"]).replace('"', '') + '`|\n')
        outfile.write(f'|Translation Y|`' + json.dumps(values["shared-translationy"]).replace('"', '') + '`|\n\n')
        outfile.write(f'## 3D Mode\n\n')
        outfile.write(f'|Parameter|Value|\n|---|---|\n')
        outfile.write(f'|Translation X|`' + json.dumps(values["shared-translationx"]).replace('"', '') + '`|\n')
        outfile.write(f'|Translation Y|`' + json.dumps(values["shared-translationy"]).replace('"', '') + '`|\n')
        outfile.write(f'|Translation Z|`' + json.dumps(values["3d-translationz"]).replace('"', '') + '`|\n')
        outfile.write(f'|Rotation 3D X|`' + json.dumps(values["3d-rotationx"]).replace('"', '') + '`|\n')
        outfile.write(f'|Rotation 3D Y|`' + json.dumps(values["3d-rotationy"]).replace('"', '') + '`|\n')
        outfile.write(f'|Rotation 3D Z|`' + json.dumps(values["3d-rotationz"]).replace('"', '') + '`|\n')
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

def rels2csv(args: argparse.Namespace) -> None:
    promptbase, inputfolder = 'working-holidays.io', args.inputfolder
    inputfile, outputfile = f'{inputfolder}/{promptbase}.csv', f'{inputfolder}/{promptbase}.summary.csv'
    with open(inputfile, 'r') as infile, open(outputfile, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        fromto = {}
        destinations = set()
        urls = {}

        # Read the csv and collect the relationships.
        for row in reader:
            destination, destinationurl, origin, originurl = row['Title'], row['Title_URL'], row['Title1'], row['Title_URL1']
            print(origin, destination) if config.debugging else None
            if origin not in fromto.keys():
                fromto[origin] = []
                urls[origin] = originurl
            fromto[origin].append(destination)

            if destination not in urls.keys():
                urls[destination] = destinationurl

            destinations.add(destination)

        # Write the summary csv listing the origin on the rows, and the destinations on the columns.
        fields = ['FromTo'] + sorted(destinations)
        writer = csv.DictWriter(outfile, fields)
        writer.writeheader()
        for origin in sorted(filter(None, fromto.keys())):
            row = {}
            row['FromTo'] = origin
            for destination in sorted(destinations):
                row[destination] = 'x' if destination in fromto[origin] else ''
            writer.writerow(row)

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
