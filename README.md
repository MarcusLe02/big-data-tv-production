# Big Data Project - TV production company

## Table of Contents
- [Introduction](#introduction)
- [Data Snapshots](#data-snapshots)
- [Project Files](#project-files)

## Introduction

This project focuses on processing and analyzing large datasets for a television production company. The data, exceeding 20 million records, originates from diverse sources:

- User contract information and interaction data (TXT files)
- User log watching duration history (JSON files)
- User log search history (Parquet files)

Data is retrieved from various storage solutions, including MySQL, Azure SQL, and the local file system. Subsequently, it undergoes transformation and organization into structured insight tables within a PostgreSQL database.

## Data Snapshots

#### User Contract & Interaction Data
![User Contract Inforamtion & Interaction Data](https://github.com/MarcusLe02/big-data-tv-production/blob/main/contract_interaction.png)
#### User Watch Duration Data
![User Watch Duration Data](https://github.com/MarcusLe02/big-data-tv-production/blob/main/log_duration.png)
#### User Log Search Data
![User Log Search Data](https://github.com/MarcusLe02/big-data-tv-production/blob/main/log_search.png)

## Technologies

- AzureSQL
- MySQL
- Python
- Apache Spark
- PostgreSQL

## Project Files

- [docker-compose.yml](https://github.com/MarcusLe02/realtime-pipeline-hiring-platform/blob/master/docker-compose.yml): Configure containers for each technology
- [kafka-stream.py](https://github.com/MarcusLe02/realtime-pipeline-hiring-platform/blob/master/dags/kafka-stream.py): Streams data from the API to Kafka
- [spark_stream.py](https://github.com/MarcusLe02/realtime-pipeline-hiring-platform/blob/master/spark-stream.py): Processes data from Kafka and stores it in Cassandra
- [faking_log.py](https://github.com/MarcusLe02/realtime-pipeline-hiring-platform/blob/master/faking_log.py): Generates sample interaction log data for testing
- [spark_cdc.py](https://github.com/MarcusLe02/realtime-pipeline-hiring-platform/blob/master/spark_cdc.py): Captures changes in Cassandra, transforms and pushes them to MySQL
