# python imports
import datetime
import json
import os.path
import logging
logger = logging.getLogger('luigi-interface')

# luigi imports
import luigi
from luigi import configuration
from luigi.s3 import S3Client, S3Target, S3PathTask
from luigi.task import ExternalTask

# mortar-luigi imports
from mortar.luigi import mortartask
from mortar.luigi import target_factory

# local imports
import transformers

"""
  Luigi pipeline to run ETL on NYC datasets, producing 
  content ready for a search index.

  Task Order:

  ProcessDay -> Geocode -> TransformInputData -> FileExists

  To Run for 2014-10-20 data in dev environment:

    mortar luigi luigiscripts/cusp_dataset_etl.py \
        --data-date "2014-10-20" \
        --environment "dev"
"""

s3_client = None
def get_s3_client():
    """
    Get an Amazon S3 client for
    working with S3 data.
    """
    # only create a new client once
    # per session
    global s3_client
    if not s3_client:
        s3_client = S3Client()
    return s3_client

def _base_path(environment, folder, data_date, filename=None):
    """
    Helper function to generate S3 paths.
    """
    # example path: s3://cusp-dataset-search-dev/input/2014-10-20/myfile
    base = os.path.join(
        's3://%s' % get_s3_bucket_for_environment(environment),
        folder,
        str(data_date))
    return os.path.join(base, filename) if filename else base

def get_s3_bucket_for_environment(environment):
    """
    Get the S3 bucket used for the given environment.
    """
    return 'cusp-dataset-search-%s' % environment

def input_path(environment, data_date, filename=None):
    """
    Get a path to input data.
    """
    return _base_path(environment, 'input', data_date, filename)

def work_path(environment, data_date, filename=None):
    """
    Get a path to a working dataset produced by the ETL.
    """
    return _base_path(environment, 'work', data_date, filename)

def output_path(environment, data_date, filename=None):
    """
    Get a path to an output dataset ready for consumption by the API.
    """
    return _base_path(environment, 'output', data_date, filename)

def is_metadata_file(filename):
    """
    Is the given filename a metadata file?
    """
    return filename and filename.endswith('.metadata')

class FileExists(ExternalTask):
    """
    An External Task to require existence of
    a target path, either in S3 or locally.
    """
    path = luigi.Parameter()
        
    def output(self):
        """
        The only output of this Task is 
        the target path. Luigi will
        check to be sure it exists.
        """
        return [target_factory.get_target(self.path)]

class TransformInputData(luigi.Task):
    """
    Transform the given input data path
    to the common data format, ready for geocoding.
    Uses the metadata stored with the input data file.

    Example:
     - input file: s3://cusp-dataset-search-dev/input/2014-10-20/myfile
     - metadata file: s3://cusp-dataset-search-dev/input/2014-10-20/myfile.metadata
    """

    # Path to the original input data file
    input_data_path = luigi.Parameter()

    # Path for working data storage (this is just a directory in S3)
    work_data_path = luigi.Parameter()

    def requires(self):
        """
        Require that both the input data file
        and its metadata file exist before processing.
        """
        return [
            FileExists(self.input_data_path),
            FileExists(self.metadata_file_path())
        ]

    def output(self):
        """
        Tell Luigi about the output that this Task produces.
        If that output already exists, Luigi will not rerun it.
        """
        return [target_factory.get_target(self.transformed_file_path())]

    def run(self):
        """
        Transform the input data to the common data format.
        """

        # read in the metadata for this Task's input data file
        metadata = self.get_metadata()

        # get a transformer for the given format
        if metadata['format'] == 'csv':
            tranformer = transformers.CSV(metadata)
        else:
            raise Exception('Unknown metadata format: %s for input_data_path: %s and metadata: %s' % \
                    (metadata['format'], self.input_data_path, metadata))

        # open up the target output to store data
        output_data_file = self.output()[0].open('w')
        
        # read in the input data
        in_filename = self.input_filename()
        input_target = self.input()[0][0]
        with input_target.open('r') as input_file:

            # transform each row and write to output
            row_count = 0
            for output_row in tranformer.transform(in_filename, input_file):
                # convert to json
                output_json = json.dumps(output_row)

                # write to output file
                output_data_file.write(output_json + '\n')

                # print status occasionally
                if row_count in (0,10**1, 10**2, 10**3, 10**4) or (row_count % 10**4 == 0):
                    logger.info("Line %s in %s transformed to common format" % (row_count, in_filename))
                row_count += 1

        # close and store the data
        output_data_file.close()
    
    def metadata_file_path(self):
        """
        Get the location of the metadata file for this input data.
        """
        base, extension = os.path.splitext(self.input_data_path)
        return '%s.metadata' % base

    def get_metadata(self):
        """
        Get the metadata dictionary for this input data.
        """
        metadata_file_target = target_factory.get_target(self.metadata_file_path())
        with metadata_file_target.open('r') as metadata_file:
            return json.load(metadata_file)

    def transformed_file_path(self):
        """
        Get the output path for the transformed data.
        """
        return os.path.join(self.work_data_path, self.input_filename())

    def input_filename(self):
        """
        Get the input filename, which also serves as the
        dataset_id.
        """
        return os.path.basename(self.input_data_path)


class Geocode(mortartask.MortarProjectPigscriptTask):
    """
    Geocode the latitude and longitude for the given
    input data to produce output that can be loaded into
    cubes.
    """

    # Path to the original data file
    input_data_path = luigi.Parameter()

    # Path for working data storage
    work_data_path = luigi.Parameter()

    # Path to the output file
    output_data_path = luigi.Parameter()

    # S3 bucket where GeoJson data is stored
    geojson_s3_bucket = luigi.Parameter()

    # S3 ky where GeoJson data is stored
    geojson_s3_key = luigi.Parameter(default='input/geo/ny_zipcodes.json')

    # cluster size to use
    # Use 0 to run directly on Mortar Pig Server.
    # Use 2 or more to run on a Hadoop cluster
    cluster_size = luigi.IntParameter(default=0)

    def token_path(self):
        """
        Location where a token will be written with the job_id
        of the currently running job. This helps Luigi pick up the
        already running Mortar pig job if it needs to restart.
        """
        return self.work_data_path

    def requires(self):
        """
        Require that the input data has been transformed to the
        common format before we geocode it.
        """
        return [TransformInputData(
            input_data_path=self.input_data_path,
            work_data_path=self.work_data_path)]

    def output(self):
        """
        Tell Luigi where this Task writes output
        so that it won't rerun it if the output already
        exists.
        """
        return self.script_output()

    def script_output(self):
        """
        Any location provided here will be cleaned up 
        if your job fails. This ensures that this Task 
        will stay idempotent for future runs.
        """
        return [S3Target(self.output_data_path)]

    def script(self):
        """
        Name of the Pig script to run.
        """
        return 'geocoding.pig'

    def parameters(self):
        """
        Pig parameters to pass to the Pig script.
        """
        return {
            'INPUT_DATA_PATH': self.input()[0][0].path,
            'OUTPUT_DATA_PATH': self.output_data_path,
            'GEOJSON_S3_BUCKET': self.geojson_s3_bucket,
            'GEOJSON_S3_KEY': self.geojson_s3_key
        }

class ProcessDay(luigi.Task):
    """
    Process all of the input data for a given date.
    Will scan the input directory for any data and kick off
    Tasks that ensure output is produced for each input file.
    If the output has already been written, these Tasks will do nothing.
    """
    # Environment where this should run (e.g. dev, test, production).
    # Each environment has it's own S3 bucket, 
    # e.g cusp-dataset-search-dev, cusp-dataset-search-test
    environment = luigi.Parameter(default='dev')

    # Date of data to process
    # Defaults to the current date in UTC
    data_date = luigi.DateParameter(default=datetime.datetime.utcnow().date())

    def requires(self):
        """
        Scan for any input data files in the parameter date and enviroinment
        folder on S3, and require that they have been processed.
        """
        s3_client = get_s3_client()

        # process anything in the path for this date and environemnt
        input_data_path = input_path(self.environment, self.data_date)

        input_filenames = s3_client.list(input_data_path)
        for filename in input_filenames:
            # exclude metadata files
            if filename and (not is_metadata_file(filename)):
                input_data_path = input_path(self.environment, self.data_date, filename)
                work_data_path = work_path(self.environment, self.data_date)
                output_data_path = output_path(self.environment, self.data_date, filename)
                s3_bucket = get_s3_bucket_for_environment(self.environment)
                yield Geocode(
                    input_data_path=input_data_path,
                    work_data_path=work_data_path,
                    output_data_path=output_data_path,
                    geojson_s3_bucket=s3_bucket)

    def complete(self):
        """
        Tell Luigi that this Task should always run.
        """
        return False


if __name__ == "__main__":
    """
    The final task in your pipeline, which will in turn pull in any dependencies
    that need to be run, should be called in the main method.
    """
    luigi.run(main_task_cls=ProcessDay)
