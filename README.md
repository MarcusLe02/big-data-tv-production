# Big Data Project - TV production company

## Table of Contents
- [Introduction](#introduction)
- [Data Snapshots](#data-snapshots)
- [Project Files](#project-files)

## Introduction

This project focuses on processing and analyzing large datasets for a television production company. The data, exceeding 20 million records, originates from diverse sources:

- User contract information and interaction data (TXT files)
- User log watching history (JSON files)
- User log search history (Parquet files)

Data is retrieved from various storage solutions, including MySQL, Azure SQL, and the local file system. Subsequently, it undergoes transformation and organization into structured insight tables within a PostgreSQL database.

## Data Snapshots

#### User Contract & Interaction Data
![User Contract Inforamtion & Interaction Data](https://github.com/MarcusLe02/big-data-tv-production/blob/main/contract_interaction.png)
#### User Watch History Data
![User Watch History Data](https://github.com/MarcusLe02/big-data-tv-production/blob/main/log_duration.png)
#### User Log Search Data
![User Log Search Data](https://github.com/MarcusLe02/big-data-tv-production/blob/main/log_search.png)

## Technologies

- AzureSQL
- MySQL
- Python
- Apache Spark
- PostgreSQL

## Project Files

- [etl_log_content.py](https://github.com/MarcusLe02/big-data-tv-production/blob/main/etl_log_content.py): Ingests, transforms, and loads watch history data
- [etl_log_search.py](https://github.com/MarcusLe02/big-data-tv-production/blob/main/etl_log_search.ipynb): Ingests, transforms, and loads log search data
- [user_analysis.ipynb](https://github.com/MarcusLe02/big-data-tv-production/blob/main/user_analysis.ipynb): Analyzes user behavior from contract information and interaction data
- [mysql_azuresql_connector_template.ipynb](https://github.com/MarcusLe02/big-data-tv-production/blob/main/mysql_azuresql_connector_template.ipynb): PySpark templates for connecting data sources and destinations
