#! /usr/bin/python

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

COLUMNS = ['Review', 'Rating']

class CountRatings(MRJob):
    def steps(self):
        return[
            MRStep(
                mapper=self.mapper_extract_ratings,
                reducer=self.reducer_count_ratings
            )
        ]
    
    def mapper_extract_ratings(self, _, line):
        """
        Mapper function, extracts rating from the record.
        
        """
        reader = csv.reader([line])
        for row in reader:
            zips = zip(COLUMNS, row)
            dicts = dict(zips)
            ratings = dicts['Rating']
            yield ratings, 1

    def reducer_count_ratings(self, key, values):
        """
        Reducer function, counts ratings.
        
        """
        yield key, sum(values)

if __name__ == '__main__':
    CountRatings.run()