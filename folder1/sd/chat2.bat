python prompts.py --fn text2csv --inputfile sd/chat2.txt --outputfile sd/chat2.csv --int 1
python prompts.py --fn prompt2styled --inputfile sd/chat2.csv --outputfile sd/chat2-styled.csv --float 1.32
python prompts.py --fn deforumcsv --inputfile sd/chat2-styled.csv --outputfile sd/chat2-deforum.csv --int 2600
python prompts.py --fn motion2md --inputfolder sd --str chat2
