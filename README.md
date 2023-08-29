** Streaming data from Kafka to Databricks using Confluent Kafka, Databricks workspace and live tables**
 
 This application reads data from S3 bucket s3://activepipe-poc and copies the content to kafka topic "ap_property_listing" in 
 confluent cloud. This topics needs to be created with 10 partitions or X number of partitions. 
 This is part of the pipeline  where the final place will be Databricks's DeltaLake
 
 This is the first part of the pipeline for ActivePipe and in the next part , the data that is in Kafka will be 
 copied to Delta Lake. THat will be done on Databrick's env
 
 **Building the project:**
 
 Run "maven clean install" at /ActivePiPe/poc/ap-ingest-kafka
 
 **Build Requirements:**
 1. Java 11
 2. Maven 3.x
 
 
 **Running the project:**
 
 target folder will cotain the .jar file , this needs to run as a lmbda function in AWS. For now , this lambda needs
 to be crated manually and this jar needs to be uploaded there with 30 secs timeout. 
 
 S3 event creation
 
 The bucket where the files will be dropped, needs to have a event trigger for all file creation events. Configure the lambda trigger
 manually.
 
 
 **PS**: _`Documentation explaing the ppc is available in "docs" folder_ `
