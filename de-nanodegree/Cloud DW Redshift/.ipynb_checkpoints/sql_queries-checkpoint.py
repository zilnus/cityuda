import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events ( 
        artist VARCHAR, 
        auth   VARCHAR,    
        first_name VARCHAR, 
        gender VARCHAR(1),
        item_session INTEGER, 
        last_name VARCHAR, 
        length DECIMAL, 
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration VARCHAR,
        session_id INT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        user_agent VARCHAR,
        user_id VARCHAR
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs ( 
        artist_id VARCHAR, 
        artist_latitude DECIMAL,    
        artist_location VARCHAR, 
        artist_longitude DECIMAL,
        artist_name VARCHAR, 
        duration DECIMAL,
        num_songs INTEGER,
        song_id VARCHAR,
        title VARCHAR,
        year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays ( 
        songplay_id BIGINT IDENTITY(1,1) PRIMARY KEY, 
        start_time TIMESTAMP NOT NULL REFERENCES time,    
        user_id VARCHAR NOT NULL REFERENCES users, 
        level VARCHAR, 
        song_id VARCHAR REFERENCES songs,
        artist_id VARCHAR REFERENCES artists, 
        session_id INT, 
        location VARCHAR, 
        user_agent VARCHAR 
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id VARCHAR PRIMARY KEY, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender VARCHAR,
        level VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY, 
        title VARCHAR, 
        artist_id VARCHAR, 
        year INT, 
        duration DECIMAL 
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY, 
        name VARCHAR, 
        location VARCHAR, 
        latitude DECIMAL,
        longitude DECIMAL 
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY, 
        hour INT, 
        day INT, 
        week INT, 
        month INT, 
        year INT, 
        weekday INT 
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION '{}' 
    COMPUPDATE OFF
    JSON {};
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), \
            'us-west-2', config.get('S3', 'LOG_JSONPATH') 
           )

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION '{}' 
    COMPUPDATE OFF
    JSON 'auto';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'), \
            'us-west-2' 
           )

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
           user_id,
           level,
           song_id,
           artist_id,
           session_id,
           location,
           user_agent
      FROM staging_events AS se
      JOIN staging_songs AS ss
      ON  (se.artist = ss.artist_name)
      AND (se.song = ss.title)
      AND (se.length = ss.duration)
      WHERE page = 'NextSong';
""")
       
user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT user_id,
                    first_name,
                    last_name,
                    gender,
                    level
    FROM staging_events se1
    WHERE page = 'NextSong' AND
          user_id IS NOT NULL AND
          user_id NOT IN (SELECT DISTINCT user_id FROM users) AND
          ts = (SELECT MAX(ts) FROM staging_events se2 WHERE se1.user_id = se2.user_id )
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
                    title,
                    artist_id,
                    year,
                    duration
    FROM staging_songs
    WHERE song_id IS NOT NULL AND 
          song_id NOT IN (SELECT DISTINCT song_id FROM songs);
""")
 
artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude )
    SELECT DISTINCT artist_id,
                    artist_name,
                    artist_location,
                    artist_latitude,
                    artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL AND 
          artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
""")

time_table_insert = ("""

    INSERT INTO time (start_time, hour, day, week, month, year, weekday )
    WITH time_stamp_table AS (
        SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time
        FROM staging_events
        WHERE page = 'NextSong'
          AND ts IS NOT NULL
    )
    SELECT DISTINCT start_time,
                    EXTRACT(HOUR FROM start_time)  AS hour,
                    EXTRACT(DAY FROM start_time)   AS day,
                    EXTRACT(WEEK FROM start_time)  AS week,
                    EXTRACT(MONTH FROM start_time) AS month,
                    EXTRACT(YEAR FROM start_time)  AS year,
                    EXTRACT(DOW FROM start_time)   AS weekday
    FROM time_stamp_table
    WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
          
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
