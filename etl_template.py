import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''

    Create spark session object
    return: spark session object
    '''


    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Description: This function reads the data from S3 and extracts the following two tables:
            1. song table
            2. artist table

        Parameters:
            spark       : Spark Session from function (create_spark_session)
            input_data  : S3 location of song_data  files with the songs metadata. Files are in json format
            output_data : S3 bucket where dimensional tables are stored in parquet format
    """



    # get filepath to song data file
    song_data =input_data + 'song_data/*/*/*.json'



    # read song data file
    df =spark.read.json(song_data ,  inferSchema=True)



    # extract columns to create songs table
    songs_table =["title", "artist_id", "year", "duration"]

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_table =["artist_id", "artist_name", "artist_location", "artist_latitude","artist_longitude"]

    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """
        Description: This function reads the data from S3 and extracts the following two tables:
            1. users table
            2. time table
            3. songplay table

        Parameters:
            spark       : Spark Session from function (create_spark_session)
            input_data  : S3 location of log data  files with the songs metadata. Files are in json format
            output_data : S3 bucket where dimensional tables are stored in parquet format
    """


    # get filepath to log data file
    log_data = input_data+'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data, inferSchema=true)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

###Users Table
    # extract columns for users table
    users_table =["userdId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]

    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')


###Time Table

    # create timestamp column from original timestamp column
    get_timestamp =  udf(date_convert, TimestampType())
    df = df.withColumn("start_time", get_datetime('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: to_date(x), TimestampType())
    df = df.withColumn("start_time", get_timestamp(col("ts")))


    df = df.withColumn("hour", hour("timestamp"))
    df = df.withColumn("day", dayofmonth("timestamp"))
    df = df.withColumn("month", month("timestamp"))
    df = df.withColumn("year", year("timestamp"))
    df = df.withColumn("week", weekofyear("timestamp"))
    df = df.withColumn("weekday", dayofweek("timestamp"))

    time_table = df.select(col("start_time"), col("hour"), col("day"), col("week"), \
                           col("month"), col("year"), col("weekday")).distinct()

    # extract columns to create time table
    time_table =

    # write time table to parquet files partitioned by year and month
    songs_table.write.partitionBy("year", "month").parquet(output_data + 'time/')





###SONGPLAYS

    # read in song data to use for songplays table

    songs_df = spark.read.parquet(output_data + 'songs/*/*/*')
    artists_df = spark.read.parquet(output_data + 'artists/*')
    songs_logs_df = df.join(songs_df, (df.song == songs_df.title))
    artists_songs_logs_df = songs_logs_df.join(artists_df, (songs_logs_df.artist == artists_df.name))


    # extract columns from joined song and log datasets to create songplays table

    songplays_df = artists_songs_logs_df.join(
        time_table,
        artists_songs_logs_df.ts == time_table.start_time, 'left'
    ).drop(artists_songs_logs_df.year)

    # write songplays table to parquet files partitioned by year and month

    songplays_table = songplays_df.select(
        col('start_time').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        col('month').alias('month'),
    ).repartition("year", "month")

    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/')




def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
