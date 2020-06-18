# Project Summary

## Introduction
Sparkify, a music streaming startup is looking to move their song data that resides in S3 into a data warehouse for their analytics. The purpose of this project to to create a dimensional model on the songs data and event logs in a Redshift Data Warahouse on AWS. It involves creating ETL pipelines to move the data from S3 buckets to Redshift.

## Explanation of data files in the repository
* ***dwh.cfg*** contains the configuration details for the Redshift cluster
* ***sql_queries.py*** contains the sql queries to create the database tables and insert data. It also contains the commands to copy data from S3 into Redshift
* ***create_tables.py*** contains the python modules to create the DWH tables
* ***etl.py*** inserts data into the staging tables and the fact and dimension tables

## Procedure to run ETL Scripts
The ETL scripts created should be run in the following order:
1. Run ***create_tables.py*** to create the staging tables and the fact and dimension tables
2. Rum ***etl.py*** to move data from S3 into the staging tables and fact and dimension tables in Redshift

