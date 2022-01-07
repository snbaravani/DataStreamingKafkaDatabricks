# Databricks notebook source
import pyspark.sql.functions as fn
from pyspark.sql.functions import *
sales_df =  spark.readStream.format('delta').load("/delta/bronze/ap_listing").filter("listing_type='sale' and price is not null")

sales_table_df = sales_df.select("sourceid",
                    "integration_id",
                    "source",
                    "alternate_id",
                    "type",
                    "status",                
                    "source_status",
                    "listing_type",
                    "house_name",
                    "street_number",
                    "street_name",
                    "city",
                    "state",
                    "post_code",
                    "region",
                    "country",
                    "price",
                    "bond",
                    "auction_date",
                    "sold_date",
                    "date_available",
                    "headline",
                    "bedrooms",
                    "bathrooms",
                    "carparks",
                    "studies",
                    "building_area",
                    "lotsize",
                    "lotsize_unitofmeasure",
                    "external_link",
                    "soilink",
                    "display_address",
                    "address_visible",
                    "sold_price",
                    "sold_price_visible",
                    "sold_price_formatted",
                    "price_formatted",
                    "price_visible",
                    "under_offer",
                    "listed_at",
                    "status_changedate",
                    "price_changedate",
                    "receptions",
                    "exclude",
                    "energy_rating",
                    "pending",
                    "latitude",
                    "longitude",
                    "geodate",
                    "category",
                    "property_images")

count = sales_table_df.select(count("*"))
display(count) 

sales_table_df.writeStream.format("delta").outputMode("append").partitionBy("type").option("overwriteSchema", "true").option("checkpointLocation", "/mnt/delta/events/silver/_checkpoints/load-from-ap-listing-sale-type").option("path","/delta/gold/sale").option("path","/delta/silver/sales").table("silver_ap_property_listing_type_sale")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE silver_ap_property_listing_type_sale

# COMMAND ----------

import pyspark.sql.functions as fn
from pyspark.sql.functions import *

lease_df =  spark.readStream.format('delta').load("/delta/bronze/ap_listing").filter("listing_type='lease' and weekly_rent is not null")

lease_table_df = lease_df.select("sourceid",
                    "integration_id",
                    "source",
                    "alternate_id",
                    "type",
                    "status",                
                    "source_status",
                    "sale_type",
                    "listing_type",
                    "house_name",
                    "street_number",
                    "street_name",
                    "city",
                    "state",
                    "post_code",
                    "region",
                    "country",
                    "weekly_rent",
                    "monthly_rent",
                    "bond",
                    "date_available",
                    "headline",
                    "bedrooms",
                    "bathrooms",
                    "carparks",
                    "studies",
                    "building_area",
                    "lotsize",
                    "lotsize_unitofmeasure",
                    "external_link",
                    "soilink",
                    "display_address",
                    "address_visible",
                    "under_offer",
                    "listed_at",
                    "status_changedate",
                    "price_changedate",
                    "receptions",
                    "exclude",
                    "energy_rating",
                    "pending",
                    "latitude",
                    "longitude",
                    "geodate",
                    "category",
                    "property_images")

count = lease_table_df.select(count("*"))
display(count) 

lease_table_df.writeStream.format("delta").outputMode("append").partitionBy("type").option("overwriteSchema", "true").option("checkpointLocation", "/mnt/delta/events/silver/_checkpoints/load-from-ap-listing-lease-type").option("path","/delta/silver/lease").table("silver_ap_property_listing_type_lease")


# COMMAND ----------

# MAGIC %fs ls /delta/silver/sales

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE silver_ap_property_listing_type_lease
