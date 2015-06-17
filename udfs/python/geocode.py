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
from rtree import index as rtree
from pig_util import outputSchema
import rtree as rt
import json
from shapely.geometry import shape, Point

#global parameter for both
nyJsonData = Null

def InitS3Object(geoJsonBucket, geoJsonFileName, accessKey,secretAccessKey):
    #############
    #Init S3 Json object and pass it to Geocode() function to operate
    #############"
    global nyJsonData
    try:
        conn = boto.connect_s3(
               aws_access_key_id = accessKey,
               aws_secret_access_key = secretAccessKey,
               calling_format = boto.s3.connection.OrdinaryCallingFormat(),
               )
        key = conn.get_bucket(geoJsonBucket).get_key(geoJsonFileName)
        jsonObject = key.read()
        nyJsonData = json.loads(jsonObject)
    except:
        print "Error when connecting to S3 to read JSON object"

    return None

@outputSchema('resultMap:(chararray,chararray)')
def Geocode(latitude,longitude):
    #############
    #main function, geocode 
    #############
    resultMap = Null
    if latitude == Null or longitude == Null:
        return "error reading lattitude"
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
        print "Unable to convert lat-lon: %d %d " % (latitude,longitude) 
        
    return resultMap;
