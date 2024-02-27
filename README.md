# Big Data Project - 

## Table of Contents
- [Introduction](#introduction)
- [Data Snapshots](#data-snapshots)
- [Project Files](#project-files)
- [Getting Started](#getting-started)

## Introduction

This is a big data processing project for a TV production company. There are 3 data sources: user contract information & interaction data as txt files, user log interaction as json files, and user log search history as parquet files, with a combined total of more than 20 millions records. Data is ingested from MySQL, AzureSQL, and local file system and transformed into structured insight tables in PostgreSQL.

## Data Snapshots

![User Contract & Interaction Data](https://github.com/MarcusLe02/realtime-pipeline-hiring-platform/blob/master/data-engineering-architecture.png)
![User Watch Duration Data](https://github.com/MarcusLe02/realtime-pipeline-hiring-platform/blob/master/data-engineering-architecture.png)
![User Log Search Data](https://github.com/MarcusLe02/realtime-pipeline-hiring-platform/blob/master/data-engineering-architecture.png)

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

## Getting Started

1. Clone the repository:
    ```bash
    git clone https://github.com/MarcusLe02/realtime-pipeline-hiring-platform.git
    ```

2. Navigate to the project directory:
    ```bash
    cd realtime-pipeline-hiring-platform
    ```

3. Run Docker Compose to spin up the services:
    ```bash
    docker-compose up -d
    ```
