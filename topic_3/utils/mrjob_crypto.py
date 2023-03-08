#! /usr/bin/python

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

COLUMNS = [
    'coin',
    'symbol',
    'price',
    '1h',
    '24h',
    '7d',
    '24h_volume',
    'mkt_cap',
    'date'
]
TOP_COL = '24h_volume'

class FindMax(MRJob):
    def steps(self):
        return[
            MRStep(
                mapper=self.mapper_extract,
                reducer=self.reducer_top
            )
        ]

    def mapper_extract(self, _, line):
        # convert each line into a dictionary
        row = dict(
            zip(
                COLUMNS, 
                [x.strip() for x in next(csv.reader([line]))]
            )
        )
        # yield the column needed
        try:
            yield TOP_COL, (float(row[TOP_COL][1:]), line.split(',')[0])
        except ValueError:
            self.increment_counter('warn', 'missing gross', 1)

    def reducer_top(self, key, values):
        topten = []
        # for the column needed compute the top 10
        for p in values:
            topten.append(p)
            topten.sort()
            topten = topten[-10:]
        for p in topten:
            yield key, p

if __name__ == '__main__':
    FindMax.run()