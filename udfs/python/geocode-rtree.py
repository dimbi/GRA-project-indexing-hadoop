###############################################################################
##
## Copyright (C) 2014,
## All rights reserved.
## Contact: Dimas Rinarso <drp354@nyu.edu>
##
## Geocode udf ported in python.
## required library: boto, shapely, rtree, json
##
###############################################################################
###############################################################################

import os,sys
import boto
import boto.s3.connection
from pig_util import outputSchema
import simplejson as json
from shapely.geometry import shape, Point
import luigi
import logging 
logger = logging.getLogger('luigi-interface') 



#class GeocodeTask(luigi.Task):

#global parameter for both
nyJsonData = None

@outputSchema('InitS3Object:int')
def InitS3Object(geoJsonBucket, geoJsonFileName, accessKey,secretAccessKey):
#############
#Init S3 Json object and pass it to Geocode() function to operate
#############"
    global nyJsonData
    try:
        logger.info("Trying to connect to S3 bucket..")
        conn = boto.connect_s3(
               aws_access_key_id = accessKey,
               aws_secret_access_key = secretAccessKey,
               calling_format = boto.s3.connection.OrdinaryCallingFormat(),
               )
        key = conn.get_bucket(geoJsonBucket).get_key(geoJsonFileName)
        jsonObject = key.read()
        nyJsonData = json.loads(jsonObject)
        logger.info("JSON file loaded")
    except:
        logger.error("Error when connecting to S3 to read JSON object")
        return 0
    return 1


@outputSchema('resultMap:(chararray,chararray)')
def Geocode(latitude,longitude):
#############
#main function, geocode 
#############
    resultMap = None
    if not latitude or not longitude:
        logger.error("Error reading longitude/latitude")
        return None
    try:
        #check each polygon to see if it contains the point
        point = Point(longitude,latitude)
        for feature in nyJsonData['features']:
            polygon = shape(feature['geometry'])
            if polygon.contains(point):
               #return resultMap, do not understand yet.... what form it is
               #maybe additional information other than geometry?
               for featureKey, featureValue in feature.iteritems():
                   if featureKey != 'geometry':
                      resultMap=(str(featureKey), str(featureValue))
    except:
        logger.error("Unable to convert lat-lon: %d %d"%(latitude,longitude))        

    return resultMap
