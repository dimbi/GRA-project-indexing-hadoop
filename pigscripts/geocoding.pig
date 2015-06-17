/**
 * Pigscript to geocode lat/lon
 * data in parallel.
 *
 * Parameters:
 *
 * INPUT_DATA_PATH: Directory for input JSON to be processed (in common format)
 * OUTPUT_DATA_PATH: Directory where output JSON with geocoded results should be written
 * GEOJSON_S3_BUCKET: S3 bucket where GeoJSON input file lives
 * GEOJSON_S3_KEY: S3 key path to GeoJSON file
 * AWS_ACCESS_KEY_ID: AWS access key ID to retrieve GeoJSON file from S3
 * AWS_SECRET_ACCESS_KEY: AWS secret access key to retrieve GeoJSON file from S3
 */
-- Geocode Java UDF
-- register 'cusp-udfs.jar';

-- Geocode python UDF
REGISTER '../udfs/python/geocode.py' USING streaming_python AS GeocodeUdfs;



-- Example locations are defined as parameters.
-- These are overridden by Luigi when it calls the Pigscript.
%default INPUT_DATA_PATH 's3://cusp-dataset-search/work/2014-10-20/sample.json';
%default OUTPUT_DATA_PATH 's3://cusp-dataset-search/output/2014-10-20/sample.json';
%default GEOJSON_S3_BUCKET 'cusp-dataset-search';
%default GEOJSON_S3_KEY 'input/geo/ny_zipcodes.json';

-- Geocode Java UDF
-- define Geocode com.mortardata.cusp.Geocode(
--     '$GEOJSON_S3_BUCKET',
--     '$GEOJSON_S3_KEY',
--     '$AWS_ACCESS_KEY_ID',
--     '$AWS_SECRET_ACCESS_KEY');

-- Geocode Python UDF
value = GeocodeUdfs.InitS3Object('$GEOJSON_S3_BUCKET','$GEOJSON_S3_KEY','$AWS_ACCESS_KEY_ID','$AWS_SECRET_ACCESS_KEY');

-- load the input JSON data in common format
input_data = load '$INPUT_DATA_PATH' 
      using org.apache.pig.piggybank.storage.JsonLoader(
        'latitude: chararray, longitude:chararray, date:chararray, dataset_id:chararray, keyword:chararray');

-- geocode the lat/lon using java udf
-- geocoded = foreach input_data generate 
--    *, Geocode(latitude, longitude) as geocode_result;

-- geocode the lat/lon using python udf
geocoded = foreach input_data generate 
     *, GeocodeUdfs.Geocode(latitude, longitude) as geocode_result;

-- store the output as JSON, first
-- removing any existing output at
-- the path b/c hadoop will refuse to write
-- if output already exists
rmf $OUTPUT_DATA_PATH;
store geocoded into '$OUTPUT_DATA_PATH' using org.apache.pig.builtin.JsonStorage();
