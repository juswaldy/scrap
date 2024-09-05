#!/usr/bin/python3

import sys
import pandas as pd
import pandas_profiling as pdprof

rootfolder = '/home/share/IT/Prod/Profiler'
filename = sys.argv[1]
basename = '.'.join(filename.split('.')[0:-1])
extension = filename.split('.')[-1]

if extension == 'xlsx':
  df = pd.read_excel(f'{rootfolder}/{filename}')
elif extension == 'csv':
  df = pd.read_csv(f'{rootfolder}/{filename}')
rpt = pdprof.ProfileReport(df, title=filename)
rpt.to_file(f'{rootfolder}/{basename}.html')
