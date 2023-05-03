# ASVSP
Big Data project, using batch and streaming processing

## Batch Data
Download data from : https://www.kaggle.com/datasets/ashishjangra27/imdb-movies-dataset?select=movies.csv.
Next create Data folder and place the data in there.
Python files that are used for loading data are in Producer/Batch folder.

## Streaming processing
Data was taken from TMDB api. A reguest to get a list of movies is sent every 20 seconds.

## Run process
To start processing run **./start.sh** file. 
This will start batch producer and consumer.
Batch Producer loads data onto HDFS so it is enought to load it once. Batch Consumer starts transformation methods on the data and saves the resulting view in postgres database. In order to visualize the results we used Metabase. Some of the visualizations created in Metabase are saved in Consumer/Batch/Visualization folder.

The script runs the consumer for the real time processing. Meanwhile the producer is run from the docker-compose in init-kafka container.

## Docker compose

This is a Docker Compose configuration file that defines a multi-container environment consisting of several services such as Hadoop namenode and datanode, Spark Master and Worker, Hue, PostgreSQL, pgAdmin, Metabase, Apache Kafka and ZooKeeper.

It includes the following containers:

**namenode**: service runs the Hadoop NameNode, which is responsible for managing the file system namespace and regulating access to files by clients.  

**datanode1**: service runs the Hadoop DataNode, which is responsible for storing and serving data blocks.

**spark-master**: service runs the Spark Master, which manages the allocation of resources and scheduling of tasks for Spark applications.  

**spark-worker1**: service runs a Spark Worker, which executes tasks assigned by the Spark Master.

**hue**: service runs Hue, which is a web-based interface for interacting with Hadoop and related services.

**db**: service runs a PostgreSQL database, which is used by the metabase service to store its metadata.

**pgadmin**: service runs pgAdmin, which is a web-based interface for managing PostgreSQL databases.

**metabase**: service runs Metabase, which is a business intelligence and analytics tool that connects to the PostgreSQL database to perform queries and generate reports.

**ZooKeeper** (zoo1): This container is used to manage the Kafka cluster's configuration and state.

**Kafka brokers** (kafka1 and kafka2): These are the main components of the Kafka cluster that handle the storage and processing of data.

**Kafka producer** (producer): This container generates data and sends it to the Kafka cluster.

**Init-kafka:** This container is used to initialize the Kafka topics and set up the necessary configuration.

The Docker Compose file also includes environment variables and volume mounts to configure the containers and ensure data persistence.

By running this Docker Compose file, you can easily create a local Kafka cluster and start producing and consuming data.
