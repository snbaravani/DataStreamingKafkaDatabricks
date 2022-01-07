# Databricks notebook source
# MAGIC %sql
# MAGIC Select   year(sold_date) as year_sold, count(*) as num_of_sales from silver_ap_property_listing_type_sale group by  year(sold_date) order by year_sold asc

# COMMAND ----------

# MAGIC %sql
# MAGIC Select   year(listed_at) as listed_year, count(*) as num_of_lists from silver_ap_property_listing_type_lease group by  year(listed_at) order by listed_year asc

# COMMAND ----------

# MAGIC %sql
# MAGIC Select   avg(price) as avg_price, year(sold_date) as sold_year from silver_ap_property_listing_type_sale where   year(sold_date) is not null group by   year(sold_date) order by  year(sold_date)  asc 

# COMMAND ----------


