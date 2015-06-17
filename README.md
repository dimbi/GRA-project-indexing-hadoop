NOTE: Some of the codes from GRA project at NYU. Data source, original config file, main platform and any account file were kept private and maintained in separate repository 

# CUSP Dataset Search Mortar Project

This is a project for transforming many different NYC open data sets into a format suitable for search indexing.

## Overview

The ETL pipeline is powered by Luigi, and can be found in the Luigi script `luigiscripts/cusp_dataset_etl.py`. This Luigi script runs a data pipeline that scans for new input data on S3, transforms it into a common data format, geocodes it, and drops off the results in an output folder on S3.

## ETL Pipeline

The Luigi pipeline in `luigiscripts/cusp_dataset_etl.py` has several important parameters:

* **environment**: This allows for different environments for dev, test, production, etc. Each environment corresponds to it's own S3 bucket, e.g. cusp-dataset-search-dev, cusp-dataset-search-test. By default, environment set to `dev`.
* **data-date**: This parameter partitions the input folder on S3 by date, so that each time we run the pipeline we only scan for data on the given date (rather than across all time). By default, it is set to today's date in the UTC timezone.

The data pipeline progresses through four steps, where each step is a Luigi Task. In dependency order (from parent-to-child), they are:

1. **ProcessDay**: This Task processes all of the input data in the environment for a given date. It scans for input, and kicks off Tasks to process any data that it finds.
2. **Geocode**: This Task runs the `pigscripts/geocoding.pig` Pigscript on Mortar to Geocode lat/lon values into polygons using GeoJson.
3. **TransformInputData**: This Task transforms input data into a common format. It is driven by a metadata file that describes the data, including what type of file it is (csv, json, etc) and where to find the latitude, longitude, and timestamp fields. It writes data out to S3 in the common (json) format.
4. **FileExists**: This Task just checks that the appropriate data files and metadata files exist on S3.

The pipeline starts with ProcessDay, which will create the rest of the tasks based on the input it finds to process.

Because the pipeline is written with Luigi, it is *idempotent*. This means that the pipeline can be run as often as you like, and it will only do Tasks whose work has not already been completed. This makes it really easy to process new data (just rerun the pipeline) and deal with failures (just rerun the pipeline).

### Running the Pipeline

To run the pipeline for a given environment and day, first ensure you have [installed the Mortar development framework](https://help.mortardata.com/technologies/mortar/install_mortar). 

Next, check out the Mortar project to your computer if you haven't already:

```bash
# clone the Mortar project
mortar projects:clone cusp-dataset-search
```

Then, you can run the pipeline for a given day (e.g. 2014-10-20) and environment (e.g. dev) with one command:

```bash
# Run the pipeline on Mortar
mortar luigi luigiscripts/cusp_dataset_etl.py \
        --data-date "2014-10-20" \
        --environment "dev"
```

This will sync the current code to Github, and then run the pipeline on the Mortar platform.

If you'd like to run the pipeline locally, you can change the command to `local:luigi`:

```bash
# Run the pipeline locally
mortar local:luigi luigiscripts/cusp_dataset_etl.py \
        --data-date "2014-10-20" \
        --environment "dev"
```

It is also possible to schedule the pipeline to run automatically on Mortar--see [Scheduling](https://help.mortardata.com/technologies/mortar/scheduling).

## Data Storage

Data is stored in an S3 bucket. There is a bucket for each environment with a common naming scheme: `cusp-dataset-search-dev` for the dev environment, `cusp-dataset-search-test` for the test environment, and so on.

The bucket has three top-level folders: 

* `input`: This is where incoming data to be processed should be placed. It is partitioned into folders for each day. For instance, the input data for October 20, 2014 would be placed in `input/2014-10-20`. Additionally, some static input (like GeoJSON files) are stored in `input/geo`.
* `work`: This is working storage, used by Luigi and Pig as they process data. It is also partitioned by day.  For instance, the working storage for October 20, 2014 would be placed in `work/2014-10-20`.
* `output`: This is the final output of the transformation and geocoding, ready for use in the cubes. It is also partitioned by day.  For instance, the output for October 20, 2014 will be stored in `output/2014-10-20`.

An example layout might be:

```text
cusp-dataset-search-dev: S3 bucket
 - input
    - 2014-10-20
      - sw33-t3vk: The data set
      - sw33-t3vk.metadata: Metadata about this data set
    - geo
      - ny_zipcodes.json: GeoJSON for NY zipcodes
 - work
    - 2014-10-20
      - sw33-t3vk: The transformed data, produced by the Luigi ETL pipeline
 - output
    - 2014-10-20
      - sw33-t3vk: The output data, ready to load into cubes
```


## Metadata File Format

To capture the differences between different input files, the ETL pipeline reads a metadata file that describes the input data. This file can be evolved as you need, but currently it looks like:

```json
{
    "filename": "sw33-t3vk",
    "format": "csv",
    "has_header_row": true,
    "latitude_column": 49,
    "longitude_column": 50,
    "date_column": 1,
    "date_format": "%m/%d/%Y",
    "keyword_column": 0
}
```

As there are more variances that need to be captured, they can be added to the metadata file and handled by the pipeline.

The metadata file gets stored in the same folder as the input data with the same filename, but with an extension of `.metadata`.

## Geocoding Pigscript and UDF

The geocoding is done in Pig using [GeoAPI](http://www.geoapi.org/) to process the GeoJSON. This allows for parallelization of the geocoding step on the Hadoop cluster.

The Pig script, `pigscripts/geocoding.pig` is fairly minimal. It simply loads in data in a common format, feeds that data to a Java User-Defined Function (UDF) for geocoding, and then stores it back out.

The UDF is stored in `udfs/java/src/main/java/com/mortardata/cusp/Geocode.java`. It is included in a maven project, and can be built with one command by running:

```bash
# compile the UDF jar
cd udfs/java
mvn package
```

The Geocode UDF loads in the GeoJSON from S3, and then calls GeoAPI to resolve lat/lon points to their geographic area. The result is emitted back out to Pig.


