#! /usr/bin/python

import sys
import csv

columns = ['Review', 'Rating']

def do_map_ratings(line):
    reader = csv.reader([line])
    for row in reader:
        zips = zip(columns, row)
        dicts = dict(zips)
        ratings = dicts['Rating']
        yield ratings, 1

for line in sys.stdin: 
    for key, value in do_map_ratings(line): 
        print(key + '\t' + str(value))

        

