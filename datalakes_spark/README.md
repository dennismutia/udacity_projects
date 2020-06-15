## Project Brief
The project focusses on building an ETL pipeline for Sparkify, a music streaming startup. The project uses Spark to load data from an S3 data lake, create analytic tables and load them back to S3 in parquet format.

The data is stored in json files and is needed for by the analytics teams to find insights on what songs their users are listening to. To enable this, we an ETL pipeline in Spark that create the relevant dimension and fact tables and loads data into them.


## Dataset
The project uses song files and log files which are in json format and can be found in the following links:
* songs data `s3://udacity-dend/song_data`
* log data `s3://udacity-dend/log_data`

The song data contains metadata about a song and its artist.
The log data contains log activity of users song listening history from Sparkify's music streaming application.

## Process Justification
Using PySpark for our ETL process ensures that we can read the data across a distributed cluster for faster query performance.
Using a cloud service such as AWS EMR allows for easier and faster scalability as the data increases in size due to increased app usage.


## Schema Justification
We used a star schema with the following fact and dimension tables:
* **Fact table**      : songplays
* **Dimension tables**  : users, songs, artists, time
The star schema allows us to add descriptive fields to songplays with simple queries. This increases the performance of our queries.


## Explanation of files in the repo
1. * dl.cfg * : configuration file containing aws access key id and secret
2. * etl.py * : an ETL script that loads data from S3 using PySpark, creates analytics tables and loads them to an S3 bucket

## How to run the files
***etl.py*** is the only file that needs running. It can be run in a terminal by navigating to the directory with the etl.py file and then executing the command below:
    `python etl.py`