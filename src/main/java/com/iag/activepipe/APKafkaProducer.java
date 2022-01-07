package com.iag.activepipe;


import appropertylisting.PropertyImages;
import appropertylisting.PropertyListing;
import appropertylisting.PropertyListingKey;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;


public class APKafkaProducer {
    private  static Logger LOG = LoggerFactory.getLogger(APKafkaProducer.class);

    public static Producer<String, String> createProducer() {
        Map<String, Object> configProps = new HashMap<>();

        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-ldvj1.ap-southeast-2.aws.confluent.cloud:9092");
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        configProps.put("schema.registry.url", "https://psrc-5mn3g.ap-southeast-2.aws.confluent.cloud");
        configProps.put("basic.auth.credentials.source", "USER_INFO");
        configProps.put("basic.auth.user.info", "Enter your creds");

        configProps.put("security.protocol","SASL_SSL");
        configProps.put("sasl.mechanism","PLAIN");
        configProps.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"JDLBWUI2GWJRGJPR\" password=\"OOC6BPOfduu4XrOhLEITWcGby6Z4eWvkRv9/XQrYxfXTY10s7L7a1fUUjHpJOsQN\";");

        return new org.apache.kafka.clients.producer.KafkaProducer<>(configProps);
    }

    public static void writeToTopic(String topic, List<PropertyListing> propertyListings) throws InterruptedException, ExecutionException, TimeoutException {
        LOG.info("Writing to the topic ", topic);
        Producer<String, String> producer = APKafkaProducer.createProducer();

        for(PropertyListing listing: propertyListings){
            PropertyListingKey key = PropertyListingKey.newBuilder()
                    .setIntegrationId(listing.getIntegrationId())
                    .setSourceid(listing.getSourceid())
                    .build();
            ProducerRecord<String, String> data = new ProducerRecord(topic,
                    key, listing);
            RecordMetadata metadata = producer.send(data).get(30, TimeUnit.SECONDS);
            producer.flush();
//            LOG.info("Record sent with key: {}  to partition {} with offset !!",
//                    key.getSourceid()+"<->"+key.getIntegrationId(), metadata.partition(), metadata.offset());

        }

    }

    public static  List<PropertyListing> getPropertyListings( JSONObject jsonObject)   {
        JSONObject listings = (JSONObject) jsonObject.get("listings");
        JSONArray set = (JSONArray) listings.get("set");
        List<PropertyListing> propertyListings = new ArrayList<>();
        for(int i = 0; i<set.size();i++){
            try {
                JSONObject listing = (JSONObject) set.get(i);
                System.out.println(listing.get("latitude")+"--"+listing.get("longitude"));
                //LOG.info("listing {}  {}",listing.toJSONString(),listing.get("integration_id"));
                PropertyListing propertyListing = PropertyListing.newBuilder()
                        .setIntegrationId((Long) listing.get("integration_id"))
                        .setAlternateId((String) listing.get("alternate_id"))
                        .setSource((String) listing.get("source"))
                        .setSourceid((String) listing.get("sourceid"))
                        .setStatus((String) listing.get("status"))
                        .setType((String) listing.get("type"))
                        .setSourceStatus((String) listing.get("source_status"))
                        .setSaleType((String) listing.get("saletype"))
                        .setListingType((String) listing.get("listingtype"))
                        .setHouseName((String) listing.get("housename"))
                        .setStreetNumber((String) listing.get("streetnumber"))
                        .setStreetName((String) listing.get("streetname"))
                        .setCity((String) listing.get("city"))
                        .setState((String) listing.get("state"))
                        .setPostCode((String) listing.get("postcode"))
                        .setRegion((String) listing.get("region"))
                        .setCountry((String) listing.get("country"))
                        .setPrice((String) listing.get("price"))
                        .setWeeklyRent((String) listing.get("weeklyrent"))
                        .setMonthlyRent((String) listing.get("monthlyrent"))
                        .setBond((String) listing.get("bond"))
                        .setAuctionDate((String) listing.get("auctiondate"))
                        .setSoldDate((String) listing.get("solddate"))
                        .setDateAvailable((String) listing.get("dateavailable"))
                        .setHeadline((String) listing.get("headline"))
                        .setBedrooms((Long) listing.get("bedrooms"))
                        .setBathrooms((Long) listing.get("bathrooms"))
                        .setCarparks((Long) listing.get("carparks"))
                        .setStudies((Long) listing.get("studies"))
                        .setBuildingArea((String) listing.get("buildingarea"))
                        .setLotsize((String) listing.get("lotsize"))
                        .setLotsizeUnitofmeasure((String) listing.get("lotsize_unitofmeasure"))
                        .setExternalLink((String) listing.get("externallink"))
                        .setSoilink((String) listing.get("soilink"))
                        .setDisplayAddress((String) listing.get("displayaddress"))
                        .setAddressVisible((Long) listing.get("addressvisible"))
                        .setSoldPrice((String) listing.get("soldprice"))
                        .setSoldPriceVisible((Long) listing.get("soldpricevisible"))
                        .setSoldPriceFormatted((String) listing.get("soldpriceformatted"))
                        .setPriceFormatted((String) listing.get("priceformatted"))
                        .setPriceVisible((Long) listing.get("pricevisible"))
                        .setUnderOffer((Long) listing.get("underoffer"))
                        .setListedAt((String) listing.get("listed_at"))
                        .setStatusChangedate((String) listing.get("statuschangedate"))
                        .setPriceChangedate((String) listing.get("pricechangedate"))
                        .setReceptions((String) listing.get("receptions"))
                        .setExclude((Long) listing.get("exclude"))
                        .setEnergyRating((String) listing.get("energyrating"))
                        .setPending((Long) listing.get("pending"))
                        .setLatitude( doDouble(listing.get("latitude")))
                        .setLongitude( doDouble(listing.get("longitude")))
                        .setGeodate((String) listing.get("geodate"))
                        .setCategory((String) listing.get("category"))
                        .setPropertyImages(getPropertyImages(listing))
                        .build();

                propertyListings.add(propertyListing);
            }catch (Exception e){

                System.out.println("Exception "+e);

            }
        }
        return propertyListings;
    }
    private static Double doDouble (Object val){
        if ( val != null &&  val.toString().contains(".")){
            return (Double)val;
        } else {
             return null;
        }
    }

    private static ArrayList<PropertyImages> getPropertyImages(JSONObject listing ){
        JSONArray images = (JSONArray) listing.get("property_images");
        ArrayList<PropertyImages> propertyImagesList = new ArrayList<>();
        if(images == null || images.size() <=0 ){
            return null;
        }
        for(int i = 0; i< images.size();i++){
            JSONObject jsonObject = (JSONObject) images.get(i);
            PropertyImages propertyImage = PropertyImages.newBuilder()
                    .setPosition((Long)jsonObject.get("position"))
                    .setSource((String)jsonObject.get("source"))
                    .setTitle((String)jsonObject.get("title"))
                    .build();
            propertyImagesList.add(propertyImage);
        }
        return propertyImagesList;
    }

}
