import configparser
from datetime import datetime
from pyspark.sql import functions as f
from pyspark.sql import types as t
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from load_bronze_layer import LoadBronzeLayer
from load_silver_layer import LoadSilverLayer
from load_gold_layer import LoadGoldLayer
from transform_fact_songs_table import TransformSongTables


config = configparser.ConfigParser()
config.read('dl.cfg')

# get credentials to access S3 bucket
aws_acess = config["AWS"]['AWS_ACCESS_KEY_ID']
aws_secret = config["AWS"]['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create a Spark session"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    hadoopConf = spark._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", aws_acess)
    hadoopConf.set("fs.s3a.secret.key", aws_secret)
    hadoopConf.set("fs.s3a.endpoint", "s3." + "us-west-2" + ".amazonaws.com")
    return spark


def process_song_data(spark, output_data):
    
    """
    Transform raw song data from S3 into analytics tables on S3
    
    This function reads in song data in parquet format from bronze layer; Make some transformations to save
    songs and artists analytics tables, then writes the tables into partitioned parquet files silver layer.
    
    Args:
        spark: a Spark session
        output_data: a folder to write analytics tables to
    """
    
    raw_song_data = read_raw_data(spark, "raw_song_data")
    
    # create songs parquet file in silver layer
    table_name_to_use_sql = "songs"
    output_data_song = os.path.join(output_data, "silver", table_name_to_use_sql)
    silver_song = LoadSilverLayer(spark, table_name_to_use_sql, output_data_song)
    sql = f"""
        SELECT song_id, title, artist_id, year, duration
        FROM {table_name_to_use_sql}
        ORDER BY song_id
    """
    songs_table = silver_song.transform(sql, raw_song_data).write_silver_layer(True, ["year", "artist_id"])

    # create artists parquet file in silver layer
    table_name_to_use_sql = "artists"
    output_data_artist = os.path.join(output_data, "silver", table_name_to_use_sql)
    silver_artist = LoadSilverLayer(spark, table_name_to_use_sql, output_data_artist)
    sql = f"""
        SELECT  artist_id        AS artist_id, 
            artist_name      AS name, 
            artist_location  AS location, 
            artist_latitude  AS latitude, 
            artist_longitude AS longitude 
        FROM {table_name_to_use_sql}
        ORDER BY artist_id desc
    """
    
    artists_table = silver_artist.transform(sql, raw_song_data).write_silver_layer(False, [])



def process_log_data(spark, output_data):
    
    """
    Transform raw log data from S3 into analytics tables on S3
    
    This function reads in log data in parquet format from bronze layer. Make some transformations to save 
    songplays, users, and time analytics tables, and then writes the tables into partitioned parquet files silver layer.
    
    Args:
        spark: a Spark session
        output_data: a folder bucket to write analytics tables to
    """
    
    # get raw log data from S3
    raw_log_data = read_raw_data(spark, "raw_log_data")
    
    # create users parquet file in silver layer
    table_name_to_use_sql = "users"
    output_data_users = os.path.join(output_data, "silver", table_name_to_use_sql)
    users_song = LoadSilverLayer(spark, table_name_to_use_sql, output_data_users)
    sql = f"""
        SELECT  DISTINCT userId AS user_id, 
                     firstName AS first_name, 
                     lastName  AS last_name, 
                     gender, 
                     level
            FROM {table_name_to_use_sql}
            ORDER BY last_name
    """
    users_table = users_song.transform(sql, raw_log_data).write_silver_layer(False, [])


    # create timestamp column from original timestamp column
    # create time parquet file in silver layer
    #raw_log_data = raw_log_data.withColumn("datetime",  f.from_unixtime(f.col("ts")/1000).cast(dataType=t.TimestampType()))
    raw_log_data = raw_log_data.withColumn("datetime",  f.from_unixtime(f.col("ts")/1000))
    raw_log_data.createOrReplaceTempView("raw_date_temp")
    
    sql = """
        SELECT  DISTINCT 
                    datetime AS start_time,
                    EXTRACT(hour FROM datetime) AS hour,
                    EXTRACT(day FROM datetime) AS day,
                    EXTRACT(week FROM datetime) AS week,
                    EXTRACT(month FROM datetime) AS month,
                    EXTRACT(year FROM datetime) AS year,
                    EXTRACT(week FROM datetime) AS weekday
            FROM    raw_date_temp log
            WHERE log.page = 'NextSong'
    """
    time_table = spark.sql(sql)
    output_data_time = os.path.join(output_data, "silver", "time")
    time_table.write.mode("overwrite").partitionBy(["year", "month"]).parquet(output_data_time)

    # get raw song data from S3
    raw_song_data = read_raw_data(spark, "raw_song_data")
    
    # create songplays parquet file in silver layer
    output_data_songplays = os.path.join(output_data, "silver", "songplays")
    songsplays = TransformSongTables(spark, raw_log_data, raw_song_data, output_data_songplays, time_table)
    songplays_table = songsplays.transform().write_silver_layer()


def read_raw_data(spark, tableName):
    """Read raw data from bronze layer"""
    path = os.path.join("data", "bronze", tableName)
    return spark.read.parquet(path)
    
def write_bronze_layer(spark, input_data, output_data):
    """Write the raw data from S3 in output destination"""
    bronze = LoadBronzeLayer(input_data, output_data, spark)
    bronze.loadTable().write_bronze_layer()
    
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "data"
    
    # write song raw data in bronze layer
    bronze_path_song = os.path.join(output_data, "bronze", "raw_song_data") 
    write_bronze_layer(spark, input_data + "/song_data/A/A/A/*", bronze_path_song)
    
    #write log raw data in bronze layer
    bronze_path_log = os.path.join(output_data, "bronze", "raw_log_data") 
    write_bronze_layer(spark, input_data + "/log_data/2018/11/*", bronze_path_log)
    
    #create silver layer for raw song data
    process_song_data(spark, output_data) 
    
    #create silver layer for raw log data
    process_log_data(spark, output_data)
    
    #write gold layer
    gold = LoadGoldLayer(spark)
    silver_path_layer = os.path.join(output_data, "silver")
    gold_path_layer = os.path.join(output_data, "gold")
    gold.write_gold_layer(silver_path_layer, gold_path_layer)


if __name__ == "__main__":
    main()
