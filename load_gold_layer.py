import os
class LoadGoldLayer:
    def __init__(self, spark):
        self.spark = spark
        
    def write_gold_layer(self, silver_layer_path, gold_layer_path):
        song_plays_path = os.path.join(silver_layer_path, "songplays")
        song_plays = self.spark.read.parquet(song_plays_path)
        
        artists_path = os.path.join(silver_layer_path, "artists")
        artists = self.spark.read.parquet(artists_path)
                                             
        songs_path = os.path.join(silver_layer_path, "songs")
        songs = self.spark.read.parquet(songs_path)
        
        joined_table_artists = song_plays.join(artists, ["artist_id"], how = "inner")
        joined_table_artists_songs = joined_table_artists.join(songs.alias("songs"), ["song_id"], how = "inner")
        
        top_artists = joined_table_artists.select("name", "artist_id").groupBy("name").agg({"artist_id": "count"}).withColumnRenamed("count(artist_id)", "plays").orderBy("plays")

        songs_with_more_play_artists = joined_table_artists_songs.select("name", "songs.title", "song_id").groupBy("name", "songs.title")\
            .agg({"song_id": "count"})\
            .withColumnRenamed("count(song_id)", "plays")\
            .orderBy("name", "plays")
        
        songs_with_more_play_artists.write.mode("overwrite").parquet(gold_layer_path + "/songsWithMorePlayPerArtists")
        top_artists.write.mode("overwrite").parquet(gold_layer_path + "/topArtists")
        
        