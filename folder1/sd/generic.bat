python prompts.py --fn text2csv --inputfile sd/flyingaround.txt --outputfile sd/flyingaround.csv --int 1
python prompts.py --fn prompt2styled --inputfile sd/flyingaround.csv --outputfile sd/flyingaround-styled.csv --float 1.32
python prompts.py --fn deforumcsv --inputfile sd/flyingaround-styled.csv --outputfile sd/flyingaround-deforum.csv --int 2600
python prompts.py --fn motion2md --inputfolder sd --str flyingaround
