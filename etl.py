import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, year, month, dayofmonth, hour, weekofyear, dayofweek, monotonically_increasing_id
import logging


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


class DataProcessor(object):
    
    def __init__(self, input_path, output_path):
        
        self.input_path = input_path
        self.output_path = output_path
        self.access_key = os.environ['AWS_ACCESS_KEY_ID']
        self.secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
        self.spark = SparkSession \
                        .builder \
                        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
                        .getOrCreate()
        self.sc = self.spark.sparkContext
        self.hadoop_conf = self.sc._jsc.hadoopConfiguration()
        self.hadoop_conf.set("fs.s3a.awsAccessKeyId", self.access_key)
        self.hadoop_conf.set("fs.s3a.awsSecretAccessKey", self.secret_key)


    def process_song_data(self):
        # get filepath to song data file
        song_data = f'{self.input_path}/song_data/*/*/*/*.json'

        # read song data file
        df = self.spark.read.json(song_data)

        # extract columns to create songs table
        songs_table = df.select(
                                'song_id', 
                                'title', 
                                'artist_id', 
                                'year', 
                                'duration',
                                )\
                                .dropDuplicates()

        # write songs table to parquet files partitioned by year and artist
        
        songs_table_path = f'{self.output_path}/song_data/songs/'
        
        songs_table = songs_table.write.partitionBy('year','artist_id')\
                                                    .parquet(songs_table_path)

        # extract columns to create artists table
        
        artists_table = df.selectExpr(
                                'artist_id',
                                'artist_name as name',
                                'artist_location as location',
                                'artist_latitude as latitude',
                                'artist_longitude as longitude',
                                )\
                                .dropDuplicates()        

        # write artists table to parquet files
        
        artists_table_path = f'{self.output_path}/song_data/artists/'
        
        artists_table = artists_table.write.parquet(artists_table_path)


    def process_log_data(self):
        # get filepath to log data file
#         log_data = f'{self.input_path}/log_data/2018/11/*.json'
        log_data = f'{self.input_path}/log_data/*.json'
        # read log data file
        df = self.spark.read.json(log_data)

        # filter by actions for song plays
        df = df.filter(df.page == 'NextSong')

        # extract columns for users table user_id, first_name, last_name, gender, level    
        users_table = df.selectExpr(
                                'userId as user_id',
                                'firstName as first_name',
                                'lastName as last_name',
                                'gender',
                                'level'
                                )\
                                .dropDuplicates()

        # write users table to parquet files
        user_table_path = f'{self.output_path}/log_data/users/'
        
        users_table = df.write.parquet(user_table_path)

         # create datetime column from original timestamp column 
        get_datetime = udf(lambda ts: datetime.fromtimestamp(ts/1000).strftime("%Y-%m-%d %H:%M:%S"))
        df = df.withColumn('start_time', get_datetime(df.ts))

        # extract columns to create time table
        time_table = df.select('start_time')\
                        .dropDuplicates()\
                        .withColumn('year', year('start_time'))\
                        .withColumn('month', month('start_time'))\
                        .withColumn('day', dayofmonth('start_time'))\
                        .withColumn('week', weekofyear('start_time'))\
                        .withColumn('weekday', dayofweek('start_time'))\
                        .withColumn('hour', hour('start_time'))                        
        
        # write time table to parquet files partitioned by year and month
        
        time_table_path = f'{self.output_path}/log_data/time/'
        time_table = time_table.write.partitionBy('year', 'month').parquet(time_table_path)

        # read in song data to use for songplays table
        song_path = f'{self.input_path}/song_data/*/*/*/*.json'
        songs = self.spark.read.json(song_path)

        # extract columns from joined song and log datasets to create songplays table 
        songplays_table = df.alias('logs').join(songs.alias('songs'), 
                                            (col('logs.song') == col('songs.title')) & (col('logs.artist') == col('songs.artist_name'))
                                               )\
                                            .selectExpr(
                                            'logs.start_time',
                                            'logs.userId as user_id',
                                            'logs.level',
                                            'songs.song_id',
                                            'songs.artist_id',
                                            'logs.sessionId as session_id',
                                            'logs.location',
                                            'logs.userAgent as user_agent'
                                                        )\
                                            .withColumn('songplay_id', monotonically_increasing_id())

        # write songplays table to parquet files partitioned by year and month
        
        songplays_table_path = f'{self.output_path}/log_data/songplays/'
        songplays_table = songplays.write.partitionBy(year('start_time'), month('start_time'))\
                                    .parquet(song_plays_table_path)

    def main(self):

        self.process_song_data()    
        self.process_log_data()


if __name__ == "__main__":
    logging.basicConfig(filename='logs.log',
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)

    logging.info("Running S3 processor")
    
    dp = DataProcessor(
                        input_path = "./data/", 
                        output_path = "s3a://data-lake-project-dlm/"
                        )
    dp.main()
