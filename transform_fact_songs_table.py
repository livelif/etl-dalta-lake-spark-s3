from pyspark.sql import functions as f
class TransformSongTables:
    def __init__(self, spark, raw_log_data, raw_song_data, output_path, time_table):
        self.spark = spark
        self.raw_log_data = raw_log_data
        self.raw_song_data = raw_song_data
        self.output_path = output_path
        self.time_table = time_table
    
    def transform(self):
        self.songplays  = self.raw_log_data.alias("log").join(self.raw_song_data.alias("song"), (self.raw_log_data.artist == self.raw_song_data.artist_name), how = "inner")\
                                                              .withColumn("start_time", f.from_unixtime(f.col("ts")/1000))\
                                                              .withColumn("songplay_id", f.monotonically_increasing_id())\
                                                              .select(f.col("start_time"),\
                                                                      f.col("songplay_id"), \
                                                                      f.col("log.userId").alias("user_id"), \
                                                                      f.col("log.level").alias("level"), \
                                                                      f.col("song.song_id").alias("song_id"), f.col("song.artist_id").alias("artist_id"), \
                                                                      f.col("log.sessionId").alias("session_id"), \
                                                                      f.col("log.location").alias("location"), \
                                                                      f.col("log.userAgent").alias("user_agent"))\
                                                              .withColumn('year', f.year(f.col("start_time")))\
                                                              .withColumn('month', f.month(f.col("start_time")))\
                                                              .where("page = 'NextSong'")
        return self

    def write_silver_layer(self):
        self.songplays.write.mode("overwrite").partitionBy(['year','month']).parquet(self.output_path)
        
        return self.songplays
        