# flik-processor with Kafka

## Step 1
For quickly launching a small development instance of Kafka I often piggyback on the work of the fine folks over at Confluent who graciously distribute Community and Enterprise Platform Docker Images of Kafka and Kafka related infrastructure.

Here is a docker-compose.yaml file which I use to spin up a local Kafka cluster for this demonstration.

To run this Docker Compose service I run the following in the same directory as the docker-compose.yaml file.

![image](https://user-images.githubusercontent.com/60826600/161108386-7def6fa7-d3e4-43c9-8055-c472da9bceb9.png)

## Step 2
To establish source and sink tables in the Apache Flink program I'll create two topics in Kafka. The first topic will be named sales-usd which will hold fictitious sales data with sales values in US Dollars and be used as input data to my Flink program. The second topic will be named sales-euros and be where I write the sales in USD converted to Euros.

![image](https://user-images.githubusercontent.com/60826600/161109035-2082243d-a1da-4bae-967f-4c1885ca3306.png)

![image](https://user-images.githubusercontent.com/60826600/161109904-7f924913-0a1a-4c1a-b58f-da3572f0127d.png)


## Step 3

The last piece of the Kafka setup to go over is the Python based sales producer program named sales_producer.py which I'll use to generate fake sales data to work with in this tutorial. The sales_producer.py file will also be contained in this project.

# Flink Python Sales Processor Application

When it comes to connecting to Kafka source and sink topics via the Table API I have two options. I can use the Kafka descriptor class to specify the connection properties, format and schema of the data or I can use SQL Data Definition Language (DDL) to do the same. I prefer the later as I find the DDL to be more readable and a slightly more stable API.

Before I can read or write data from an external Kafka cluster I need to download the Kafka SQL Connector so I can configure my Apache Flink program to use it. The Kafka SQL Connector is a simple Jar library which I can download on https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.13.0/flink-sql-connector-kafka_2.11-1.13.0.jar

Now in the same directory as the Jar file I just download I'll create a new python module named sales_processor.py representing the Flink program.

The source code should be reasonably commented enough to get a pretty good idea of what is going on. Briefly, I establish a TableEnvironment in streaming mode which is the main entry point through which my Flink processing code is funneled into the Flink runtime. After creating the TableEnvironment I register the Kafka SQL Connector Jar library with the environment. Next I use DDL to define the source table to map the schema and subsequent data of the Kafka input topic to the Schema of an associated Flink table.

![image](https://user-images.githubusercontent.com/60826600/161112048-180f03c2-77d3-42f7-8ee3-bed8d6e4a216.png)

With the TableEnvironment configured to source data from Kafka I then write a simple SQL query to perform a one minute tumbling windowed aggregate to sum and convert seller sales from USD to Euros. The results of this calculation is assigned to the Table variable named revenue_tbl. The last thing the program does is define a sink Flink table mapping the results of the revenue aggregation and conversion calculation to a destination Kafka topic named sales-euros.



