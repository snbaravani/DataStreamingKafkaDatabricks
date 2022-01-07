package com.iag.activepipe;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.iag.activepipe.AppConstants.AP_PROPERTY_LISTING_TOPIC;

/**
 * S3 event is the trigger, it downloads the file, extract the data and pushes to kafka**/

public class S3EventHandler implements RequestHandler< S3Event, String> {
    private  Logger LOG = LoggerFactory.getLogger(S3EventHandler.class);

    public String handleRequest(S3Event event, Context context){
        S3EventNotification.S3EventNotificationRecord record=event.getRecords().get(0);
        String bucketName = record.getS3().getBucket().getName();
        String key = record.getS3().getObject().getKey();
        LOG.info("File being ingested ==> {}", key);
        AmazonS3Client s3Client = new AmazonS3Client();
        S3Object fileObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
        InputStream inputStream = new BufferedInputStream(fileObject.getObjectContent());

        try {
            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject)jsonParser.parse(
                    new InputStreamReader(inputStream, "UTF-8"));

            APKafkaProducer.writeToTopic(AP_PROPERTY_LISTING_TOPIC,   APKafkaProducer.getPropertyListings(jsonObject) );
            inputStream.close();
            fileObject.close();
        }
        catch (ExecutionException e) {
            LOG.error(e.getMessage());
        }
        catch (InterruptedException e) {
            LOG.error(e.getMessage());
        } catch (ParseException e) {
            LOG.error(e.getMessage());
        } catch (UnsupportedEncodingException e) {
            LOG.error(e.getMessage());
        } catch (IOException e) {
            LOG.error(e.getMessage());
        } catch (TimeoutException e) {
            LOG.error(e.getMessage());
        }

        return "success";
    }
}