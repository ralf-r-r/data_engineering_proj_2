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
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS 
staging_events(  
    artist VARCHAR, auth VARCHAR,  firstname VARCHAR,  gender VARCHAR, itemInSession INTEGER,
    lastname VARCHAR, length DOUBLE PRECISION,  level VARCHAR, location VARCHAR, 
    method VARCHAR, page VARCHAR, registration DOUBLE PRECISION, sessionid INTEGER, song VARCHAR,
    status INTEGER, ts BIGINT, useragent VARCHAR, userId INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS 
staging_songs(
    song_id VARCHAR, artist_id VARCHAR, artist_longitude DOUBLE PRECISION, artist_lattitude DOUBLE PRECISION,    
    artist_location VARCHAR, artist_name VARCHAR, title VARCHAR, duration DOUBLE PRECISION, year INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS
songplays (
    songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY, start_time TIMESTAMP NOT NULL ,user_id INTEGER NOT NULL,
    level VARCHAR, song_id VARCHAR NOT NULL, artist_id VARCHAR NOT NULL, 
    session_id INTEGER, location VARCHAR, user_agent VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS
users(
    user_id INTEGER PRIMARY KEY NOT NULL, first_name VARCHAR, last_name VARCHAR, 
    gender VARCHAR, level VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS
songs(
    song_id VARCHAR PRIMARY KEY NOT NULL, title VARCHAR, artist_id VARCHAR NOT NULL, 
    year INTEGER, duration DOUBLE PRECISION
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS
artists(
    artist_id VARCHAR PRIMARY KEY NOT NULL, name VARCHAR, location VARCHAR, lattitude DOUBLE PRECISION, 
    longitude DOUBLE PRECISION
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS
time(
    start_time TIMESTAMP PRIMARY KEY NOT NULL, hour INTEGER, day INTEGER, week INTEGER, 
    month INTEGER, year INTEGER, weekday INTEGER
)
""")


# STAGING TABLES
staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON {}
EMPTYASNULL
BLANKSASNULL
REGION {};
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'), config.get('S3','REGION'))

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto'
EMPTYASNULL
BLANKSASNULL
REGION {};
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','REGION'))


# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + ev.ts/1000 * interval '1 second' AS start_time,
                ev.userId, 
                ev.level, 
                so.song_id, 
                so.artist_id, 
                ev.sessionid, 
                ev.location, 
                ev.useragent
FROM staging_events AS ev
JOIN staging_songs AS so
ON (ev.song = so.title)
AND (ev.length = so.duration)
AND (ev.artist = so.artist_name)
WHERE ev.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT  ev.userId, ev.firstname, ev.lastname, ev.gender, ev.level
FROM staging_events AS ev
WHERE ev.userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT  so.song_id, so.title, so.artist_id, so.year, so.duration
FROM staging_songs AS so
WHERE so.song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, lattitude, longitude)
SELECT DISTINCT so.artist_id, so.artist_name, so.artist_location, so.artist_lattitude, so.artist_longitude
FROM staging_songs as so
WHERE so.artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT sp.start_time,
                EXTRACT(HOUR FROM sp.start_time) as hour,
                EXTRACT(DAY FROM sp.start_time) as day,
                EXTRACT(WEEK FROM sp.start_time) as week,
                EXTRACT(MONTH FROM sp.start_time) as month,
                EXTRACT(YEAR FROM sp.start_time) as year,
                EXTRACT(DOW FROM sp.start_time) as weekday
FROM songplays as sp
WHERE sp.start_time IS NOT NULL
""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]