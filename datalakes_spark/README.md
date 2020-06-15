## Project Summary
The project foucsses on building an ETL pipeline for Sparkify, a music streaming startup. The project uses Spark to load data from an S3 data lake, create analytic tables and load them back to S3 in parquet format.

## Explanation of files in the repo
1. * dl.cfg * : configuration file containing aws access key id and secret
2. * etl.py * : an ETL script that loads data from S3 using PySpark, creates analytics tables and loads them to an S3 bucket

## How to run the files
***etl.py*** is the only file that needs running.