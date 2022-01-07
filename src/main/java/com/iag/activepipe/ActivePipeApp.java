package com.iag.activepipe;

import appropertylisting.PropertyImages;
import appropertylisting.PropertyListing;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.iag.activepipe.AppConstants.AP_PROPERTY_LISTING_TOPIC;

/**
 * Application reads data from S3 bucket s3://activepipe-poc and
 * copies the content to kafka topic/s at TODO. This is part of the pipeline
 * where the final place will be Databricks's DeltaLake
 */

public class ActivePipeApp {

    private static Logger LOG = LoggerFactory.getLogger(ActivePipeApp.class);

    public static void main(String[] args) throws IOException, ParseException, InterruptedException {
        LOG.info(" Active Pipe Ingestion Application starts !");
        SpringApplication.run(ActivePipeApp.class, args);
      //  produceMessages();
      //  KafkaCnsumer.runConsumer();
      //  printId();
//        InputStream inputStream = ActivePipeApp.class.getClassLoader().getResourceAsStream("1636434776.3039.json");
//        JSONParser jsonParser = new JSONParser();
//        JSONObject jsonObject = (JSONObject)jsonParser.parse(
//                new InputStreamReader(inputStream, "UTF-8"));
//        JSONObject listings = (JSONObject) jsonObject.get("listings");
//        JSONArray set = (JSONArray) listings.get("set");
//        Map<String, String> recordMap = new HashMap<>();
//
//        List<PropertyListing> propertyListings=   APKafkaProducer.getPropertyListings(jsonObject);
//        for(int i = 0; i<set.size();i++){
//            JSONObject listing = (JSONObject) set.get(i);
//            String key = (String) listing.get("sourceid");
////            ArrayList list =  getPropertyImages(listing);
//            String value = listing.toJSONString();
//            recordMap.put(key,value);
//           // System.out.println("key:"+key);
//
//        }


        }

//        static void  produceMessages() throws IOException, ParseException {
//            File folder = new File("/Users/s115778/Documents/project/ActivePiPe/Docs/data/export-2021-11-09");
//            File[] listOfFiles = folder.listFiles();
//            Map<String, String> recordMap = new HashMap<>();
//            long count = 0;
//            for(int i = 0; i<listOfFiles.length;i++) {
//                try {
//                    InputStream inputStream = new FileInputStream(listOfFiles[i]);
//                    JSONParser jsonParser = new JSONParser();
//                    JSONObject jsonObject = (JSONObject) jsonParser.parse(
//                            new InputStreamReader(inputStream, "UTF-8"));
//                    List<PropertyListing> listing  = APKafkaProducer.getPropertyListings(jsonObject);
//                    count = count + listing.size();
//                    System.out.println(listOfFiles[i].getName()+"-->"+count);
//                    APKafkaProducer.writeToTopic("test_topic",   listing);
//                    inputStream.close();
//                    inputStream.close();
//                } catch(Exception e){
//                    e.printStackTrace();
//                }
//            }
//
//            System.out.println("Total records sent->"+count);
//        }
//        private static void printId(){
//            File folder = new File("/Users/s115778/Documents/project/ActivePiPe/Docs/data/export-2021-11-09");
//            File[] listOfFiles = folder.listFiles();
//            Map<String, String> recordMap = new HashMap<>();
//            for(int i = 0; i<listOfFiles.length;i++){
//                try {
//                    InputStream inputStream = new FileInputStream(listOfFiles[i]);
//                    JSONParser jsonParser = new JSONParser();
//                    JSONObject jsonObject = (JSONObject)jsonParser.parse(
//                            new InputStreamReader(inputStream, "UTF-8"));
//                    JSONObject listings = (JSONObject) jsonObject.get("listings");
//                    JSONArray set = (JSONArray) listings.get("set");
//
//                    for(int j = 0; j<set.size();j++){
//                        JSONObject listing = (JSONObject) set.get(j);
//                        String key = (String) listing.get("sourceid");
//                        String value =key;
//                        recordMap.put(key,value);
//                        // System.out.println("key:"+key);
//
//                    }
//                } catch (FileNotFoundException | UnsupportedEncodingException e) {
//                    e.printStackTrace();
//                } catch (ParseException e) {
//                    e.printStackTrace();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//
//            }
//            System.out.println(recordMap.entrySet().size());
//        }
//    private static ArrayList<PropertyImages> getPropertyImages(JSONObject listing ){
//        JSONArray images = (JSONArray) listing.get("property_images");
//        ArrayList<PropertyImages> propertyImagesList = new ArrayList<>();
//        for(int i = 0; i< images.size();i++){
//            JSONObject jsonObject = (JSONObject) images.get(i);
//            PropertyImages propertyImage = PropertyImages.newBuilder()
//                    .setPosition((Long)jsonObject.get("position"))
//                    .setSource((String)jsonObject.get("title"))
//                    .setTitle((String)jsonObject.get("source"))
//                    .build();
//            propertyImagesList.add(propertyImage);
//        }
//        return propertyImagesList;
//    }

}