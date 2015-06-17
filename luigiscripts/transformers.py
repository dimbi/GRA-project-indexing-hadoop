import csv
import datetime

import logging
logger = logging.getLogger('luigi-interface')

class Transformer(object):
    """
    Tranform input data to the common data format.
    """

    def get_output_row(self, latitude, longitude, date, dataset_id, keyword):
        """
        Get an output row in the common data format.
        """
        return {
            'latitude': latitude,
            'longitude': longitude,
            'date': date,
            'dataset_id': dataset_id,
            'keyword': keyword
        }

def unicode_csv_reader(utf8_data, dialect=csv.excel, **kwargs):
    """
    A unicode-ready CSV reader for python.

    @see https://docs.python.org/2/library/csv.html#csv-examples
    """
    csv_reader = csv.reader(utf8_data, dialect=dialect, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, 'utf-8') for cell in row]

class CSV(Transformer):
    """
    Tranform CSV input data to the common data format.
    """

    def __init__(self, metadata):
        self.metadata = metadata

    def transform(self, input_filename, input_file):

        # create a UTF-8 CSV reader
        reader = unicode_csv_reader(input_file, dialect=csv.excel)

        # skip the header row if one exists
        if self.metadata['has_header_row']:
            reader.next()
        
        # read and yield lines one-by-one
        for line in reader:
            latitude = line[self.metadata['latitude_column']]
            longitude = line[self.metadata['longitude_column']]
            date_str = line[self.metadata['date_column']]
            keyword = line[self.metadata['keyword_column']]
            try:
                dt = datetime.datetime.strptime(date_str, self.metadata['date_format'])
                date_value = dt.date().isoformat()
            except ValueError:
                logger.debug('Unable to parse date %s with format %s' % (date_str, self.metadata['date_format']))
                date_value = None
            # yield so that processing can go row-by-row instead
            # of loading the whole dataset into memory at once
            yield self.get_output_row(
                latitude,
                longitude,
                date_value,
                input_filename,
                keyword)

