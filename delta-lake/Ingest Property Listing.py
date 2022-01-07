# Databricks notebook source
from pyspark.sql.functions import col, from_json
from pyspark.sql import SparkSession,Row
from pyspark.sql.types import IntegerType, StructType,StructField, StringType, ArrayType,LongType
from pyspark.sql.functions import split, explode
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql.avro.functions import from_avro, to_avro
import pyspark.sql.functions as fn
from pyspark.sql.functions import *
 
fromAvroOptions = {"mode":"FAILFAST"}
 
options = {
    "kafka.sasl.jaas.config": 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="JDLBWUI2GWJRGJPR" password="OOC6BPOfduu4XrOhLEITWcGby6Z4eWvkRv9/XQrYxfXTY10s7L7a1fUUjHpJOsQN";',
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol" : "SASL_SSL",
    "kafka.bootstrap.servers": "pkc-ldvj1.ap-southeast-2.aws.confluent.cloud:9092",
    "application.id": "ap-data-ingest-temp11",
    "subscribe": "ap_property_listing",
    "startingOffsets": "earliest",
}
schema_registry_conf = {
    'url': "https://psrc-5mn3g.ap-southeast-2.aws.confluent.cloud",
    'basic.auth.user.info': '{}:{}'.format('XXP44LPOBI7RQ6TB', 'XXXX')}
 
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
value_schema = schema_registry_client.get_latest_version('ap_property_listing' + "-value").schema.schema_str
df = spark.readStream.format("kafka").options(**options).load().withColumn('fixedValue', fn.expr("substring(value, 6, length(value)-5)")).select(from_avro('fixedValue', value_schema,       fromAvroOptions).alias('parsedValue'))
table_df=df.select("parsedValue.sourceid",
                    "parsedValue.integration_id",
                    "parsedValue.source",
                    "parsedValue.alternate_id",
                    "parsedValue.type",
                    "parsedValue.status",                
                    "parsedValue.source_status",
                    "parsedValue.sale_type",
                    "parsedValue.listing_type",
                    "parsedValue.house_name",
                    "parsedValue.street_number",
                    "parsedValue.street_name",
                    "parsedValue.city",
                    "parsedValue.state",
                    "parsedValue.post_code",
                    "parsedValue.region",
                    "parsedValue.country",
                    "parsedValue.price",
                    "parsedValue.weekly_rent",
                    "parsedValue.monthly_rent",
                    "parsedValue.bond",
                    "parsedValue.auction_date",
                    "parsedValue.sold_date",
                    "parsedValue.date_available",
                    "parsedValue.headline",
                    "parsedValue.bedrooms",
                    "parsedValue.bathrooms",
                    "parsedValue.carparks",
                    "parsedValue.studies",
                    "parsedValue.building_area",
                    "parsedValue.lotsize",
                    "parsedValue.lotsize_unitofmeasure",
                    "parsedValue.external_link",
                    "parsedValue.soilink",
                    "parsedValue.display_address",
                    "parsedValue.address_visible",
                    "parsedValue.sold_price",
                    "parsedValue.sold_price_visible",
                    "parsedValue.sold_price_formatted",
                    "parsedValue.price_formatted",
                    "parsedValue.price_visible",
                    "parsedValue.under_offer",
                    "parsedValue.listed_at",
                    "parsedValue.status_changedate",
                    "parsedValue.price_changedate",
                    "parsedValue.receptions",
                    "parsedValue.exclude",
                    "parsedValue.energy_rating",
                    "parsedValue.pending",
                    "parsedValue.latitude",
                    "parsedValue.longitude",
                    "parsedValue.geodate",
                    "parsedValue.category",
                    "parsedValue.property_images")
 
count = table_df.select(count("*"))
display(count) 
table_df.writeStream.format("delta").outputMode("append").partitionBy("state").option("overwriteSchema", "true").option("checkpointLocation", "/mnt/delta/bronze/events/_checkpoints/load-from-ap-listing").option("path","/delta/bronze/ap_listing").table("bronze_ap_property_listing")

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------


