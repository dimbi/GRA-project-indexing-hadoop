package com.mortardata.cusp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geometry.DirectPosition2D;
import org.opengis.feature.Feature;
import org.opengis.feature.Property;
import org.opengis.geometry.DirectPosition;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;

public class Geocode extends EvalFunc<Map<String, Object>> {
    
    @SuppressWarnings("rawtypes")
    private FeatureCollection features = null;
    private AmazonS3Client s3Client = null;
    private String geoJsonBucket;
    private String geoJsonFileName;

    public Geocode(String geoJsonBucket, String geoJsonFileName, String accessKey, String secretAccessKey) throws IOException {
        this.geoJsonBucket = geoJsonBucket;
        this.geoJsonFileName = geoJsonFileName;
        
        BasicAWSCredentials creds = new BasicAWSCredentials(accessKey, secretAccessKey);
        this.s3Client = new AmazonS3Client(creds);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, Object> exec(Tuple input) throws IOException {
        if (this.features == null) {
            // load features on the first row
            loadFeatures();
        }
        Map<String, Object> resultMap = new HashMap<String, Object>();
        
        Object latitudeObj = input.get(0);
        Object longitudeObj = input.get(1);
        if (latitudeObj == null || longitudeObj == null) {
            return resultMap;
        }

        Double latitude;
        Double longitude;
        try {
            latitude = new Double(latitudeObj.toString());
            longitude = new Double(longitudeObj.toString());
        } catch (NumberFormatException e) {
            warn("Unable to convert latitude: [" + latitudeObj +
                    "] or longitude: [" + longitudeObj + "] fields to a Double" , 
                    PigWarning.UDF_WARNING_1);
            return resultMap;
        }

        DirectPosition position = new DirectPosition2D(longitude, latitude);
        FeatureIterator fi = features.features();
        while(fi.hasNext()) {
            Feature f = fi.next();
            if (f.getBounds().contains(position)) {
                for (Property p : f.getProperties()) {
                    //Skip geometry property since its big and I assume not useful.
                    if (!p.getName().toString().equals("geometry")) {
                        resultMap.put(p.getName().toString(), p.getValue());
                    }
                }
                break;
            }
        }
        fi.close();
        
        return resultMap;
    }
    
    private void loadFeatures() throws IOException {
        S3Object geoJSONFile = s3Client.getObject(this.geoJsonBucket, this.geoJsonFileName);
        FeatureJSON fJson = new FeatureJSON();
        this.features = fJson.readFeatureCollection(geoJSONFile.getObjectContent());
    }
}
