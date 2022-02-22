# Data Warehouse With AWS using S3 and Spark

## Datasets
There are two datasets being used in this project. The first is song stored in s3://udacity-dend/song_data/*.
The other dataset is log data stored in s3://udacity-dend/log_data/*. 

### Raw JSON data structures
* **log_data**: log_data contains data about what users have done (columns: event_id, artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId)
* **song_data**: song_data contains data about songs and artists (columns: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year)


## Project Template
The project template includes four files:

1. etl.py is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
2. load_bronze_layer.py get the raw data and load in bronze layer, creating parquet files called raw_song_data and raw_log_data.
3. load_silver_layer.py get the bronze layer and load silver layer, creating parquet files for users, songs, artists, time and songplays.
4. transform_fact_songs_tables.py create the songplays paquet file.
5. load_gold_layer.py get the silver layer and load gold layer, creating aggregations and insights arount the data. In the final, is creating two gold tables called songsWithMorePlayPerArtists and topArtists.
6. README.md is where you'll provide discussion on your process and decisions for this ETL pipeline.
7. dl.cfg contains your AWS credentials.

## Schema 
### Fact Table
1. songplays - records in event data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
1. users - user_id, first_name, last_name, gender, level
2. songs - songs in music database song_id, title, artist_id, year, duration
3. artists - artists in music database artist_id, name, location, lattitude, longitude
4. time - timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year, weekday

### How to run this project:
Run python etl.py

## ETL Process
The steps:
1. Get the data s3://udacity-dend/log_data/* and s3://udacity-dend/song_data/*
2. Create a Delta Lake using pyspark. Make some transformations
