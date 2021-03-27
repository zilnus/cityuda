import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType,StructField as Fld,DoubleType as Dbl, StringType as Str, IntegerType as Int
from pyspark.sql.types import LongType as Long, TimestampType as TST, DateType as Dat

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']

def getDateTime(timestamp):
    """ Convert Timestamp to Date Time format
    
    Args:
      timestamp (long)   : timestamp format
      
    Returns:
      datetime : datetime format
    
    """
    
    return datetime.fromtimestamp(timestamp/1000.0)

def create_spark_session():
    """ Create Spark Session
    
    Args:
      None
      
    Returns:
      spark : Spark session
    
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Process song_data json files which located in S3
        Create table songs_table and artists_table
        Store the table in parque format in S3
        Return the table to be used in process_log_data function
    
    Args:
      spark                 : Spark Session
      input_data  (string)  : location json files (input)
      output_data (string)  : location parque files (output)
      
    Returns:
      songs_data    (Spark Dataframe) : Song Data tables
    
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # set schema song data
    songSchema = StructType([
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_name", Str()),
        Fld("duration", Dbl()),
        Fld("num_songs", Int()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("year", Int()),
])    
    
    # read song data file
    df = spark.read.json(song_data, schema=songSchema)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration") \
                    .where("song_id is not null") \
                    .dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(os.path.join(output_data, 'songs'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select(col("artist_id"), 
                              col("artist_name").alias("name"), 
                              col("artist_location").alias("location"),  
                              col("artist_latitude").alias("latitude"),  
                              col("artist_longitude").alias("longitude")) \
                      .where("artist_id is not null") \
                      .dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')

    # return song_data table to be used in process_log_data
    return df


def process_log_data(spark, input_data, output_data, songs_data):
    """ Process log_data json files which located in S3
        Create table users, time and song_plays
        songs_data will be needed in creation song_plays table
        Store the table in parque format in S3
    
    Args:
      spark                           : Spark Session
      input_data  (string)            : location json files (input)
      output_data (string)            : location parque files (output)
      songs_data  (Spark Dataframe)   : Song Data tables
      
    Returns:
      None
      
    
    """    
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # set schema log data
    logSchema = StructType([
        Fld("artist", Str()),
        Fld("auth", Str()),
        Fld("firstName", Str()),
        Fld("gender", Str()),
        Fld("itemInSession", Int()),
        Fld("lastName", Str()),
        Fld("length", Dbl()),
        Fld("level", Str()),
        Fld("location", Str()),
        Fld("method", Str()),
        Fld("page", Str()),
        Fld("registration", Dbl()),
        Fld("sessionId", Int()),
        Fld("song", Str()),
        Fld("status", Int()),
        Fld("ts", Long()),
        Fld("userAgent", Str()),
        Fld("userId", Str()),
])    
    
    # read log data file
    df = spark.read.json(log_data, schema=logSchema)
    
    # filter by actions for song plays
    df = df.filter("page == 'NextSong'")
    
    # create temporary View for Log
    df.createOrReplaceTempView("logView")
        
    # extract columns for users table    
    users_table = spark.sql("""
        WITH latestChange AS (
            SELECT userId AS userIdLatest,
                   MAX(ts) AS maxTs
            FROM logView
            GROUP BY userId
        )
        SELECT userId AS user_id,
               ts AS tsTemp,
               firstName AS first_name,
               lastName  AS last_name,
               gender,
               level
        FROM logView AS t1
        JOIN latestChange AS t2 
        ON t1.userId = t2.userIdLatest AND t1.ts = t2.maxTs  
        WHERE userId IS NOT NULL
        """
    ).dropDuplicates(['user_id']).drop("tsTemp")
        
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: getDateTime(int(x)),TST())
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: getDateTime(int(x)),Dat())
    df = df.withColumn("date_time", get_datetime(df.ts)) 

    # extract columns to create time table
    time_table = df.select(col("start_time"), 
                           hour(df.start_time).alias("hour"), 
                           dayofmonth(df.date_time).alias("day"),  
                           weekofyear(df.date_time).alias("week"),  
                           month(df.date_time).alias("month"),
                           year(df.date_time).alias("year"),
                           date_format(df.date_time,"W").alias("weekday")  ) \
                      .where("start_time is not null") \
                      .dropDuplicates(['start_time']) 
 
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(os.path.join(output_data, 'time'), 'overwrite')

    # create temporary View for Log and Song tables
    df.createOrReplaceTempView("logView")
    songs_data.createOrReplaceTempView("songView")
        
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
        SELECT start_time,
               year(date_time) AS year,
               month(date_time) AS month,
               userId AS user_id,
               level,
               song_id,
               artist_id,
               sessionId AS session_id,
               location,
               userAgent AS user_agent 
        FROM logView AS t1
        JOIN songView AS t2
        ON  (t1.artist = t2.artist_name)
        AND (t1.song   = t2.title)
        AND (t1.length = t2.duration)
        """
    )

    songplays_table.show(10)
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(os.path.join(output_data, 'songplays'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-satria-de-output/"

#   improve speed write to S3    
    sc = spark.sparkContext
    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    hadoopConf.set("fs.s3a.fast.upload", "true")
        
    songs_data = process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data, songs_data)

    
if __name__ == "__main__":
    main()
