# IMPORT PACKAGES 
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_S3_LOG_DATA = config.get("S3","LOG_DATA")
DWH_S3_SONG_DATA = config.get("S3","SONG_DATA")
DWH_S3_LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")


# SQL SYNTAX FOR CREATING SCHEMA 
schema_create_query = 'CREATE SCHEMA IF NOT EXISTS Sparkify'


# SQL SYNTAX FOR SETTING THE SCHEMA PATH
schema_set_path_query = 'SET search_path TO Sparkify'


# SQL SYNTAX FOR DROPPING TABLES 
staging_events_table_drop = 'DROP TABLE IF EXISTS "staging_events_table"'
staging_songs_table_drop = 'DROP TABLE IF EXISTS "staging_songs_table"'
songplay_table_drop = 'DROP TABLE IF EXISTS "songplay_table"'
user_table_drop = 'DROP TABLE IF EXISTS "user_table"'
song_table_drop = 'DROP TABLE IF EXISTS "song_table"'
artist_table_drop = 'DROP TABLE IF EXISTS "artist_table"'
time_table_drop = 'DROP TABLE IF EXISTS "time_table"'
timetemp_table_drop = 'DROP TABLE IF EXISTS "timetemp"'


# SQL SYNTAX FOR CREATING STAGING TABLES
staging_events_table_create= ("""
    CREATE TABLE "staging_events_table" (
    "artist" varchar(max),
    "auth" varchar(10),
    "firstName" varchar(max),
    "gender" varchar(10),
    "iteminSession" int,
    "lastName" varchar(max),
    "length" double precision,
    "level" varchar(10),
    "location" varchar(max),
    "method" varchar(10),
    "page" varchar(max),
    "registration"bigint,
    "sessionId" int,
    "song" varchar(max),
    "status" int,
    "ts" bigint,
    "userAgent" varchar(max),
    "userId" varchar(max));
""")

staging_songs_table_create = ("""
    CREATE TABLE "staging_songs_table" (
    "num_songs" int,
    "artist_id" varchar(max),
    "artist_latitude" varchar(max),
    "artist_longitude" varchar(max),
    "artist_location" varchar(max),
    "artist_name" varchar(max),
    "song_id" varchar(max),
    "title" varchar(max),
    "duration" double precision,
    "year" int);
""")


# SQL SYNTAX FOR CREATING ANALYTICAL TABLES
songplay_table_create = ("""
    CREATE TABLE "songplay_table" (
    "songplay_id" int identity(1,1) NOT NULL,
    "start_time" varchar(max) sortkey,
    "user_id" varchar(max),
    "level" varchar(5),
    "song_id" varchar(max),
    "artist_id" varchar(max),
    "session_id" int,
    "location" varchar(max),
    "user_agent" varchar(max),
    PRIMARY KEY (songplay_id),
    FOREIGN KEY (start_time) REFERENCES time_table (start_time),
    FOREIGN KEY (user_id) REFERENCES user_table (user_id),
    FOREIGN KEY (song_id) REFERENCES song_table (song_id),
    FOREIGN KEY (artist_id) REFERENCES artist_table (artist_id))
    diststyle even;
""")

user_table_create = ("""
    CREATE TABLE "user_table" (
    "user_id" varchar(max) sortkey NOT NULL,
    "first_name" varchar(max),
    "last_name" varchar(max),
    "gender" varchar(5),
    "level" varchar(5),
    PRIMARY KEY (user_id))
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE "song_table" (
    "song_id" varchar(max) sortkey NOT NULL,
    "title" varchar(max),
    "artist_id" varchar(max),
    "year" int,
    "duration" double precision,
    PRIMARY KEY (song_id))
    diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE "artist_table" (
    "artist_id" varchar(max) sortkey NOT NULL,
    "name" varchar(max),
    "location" varchar(max),
    "latitude" varchar(max),
    "longitude" varchar(max),
    PRIMARY KEY (artist_id))
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE "time_table" (
    "start_time" varchar(max) sortkey NOT NULL,
    "hour" varchar(max) NOT NULL,
    "day" varchar(max) NOT NULL,
    "week" varchar(max) NOT NULL,
    "month" varchar(max) NOT NULL,
    "year" varchar(max) NOT NULL,
    "weekday" varchar(max) NOT NULL,
    PRIMARY KEY (start_time))
    diststyle all;
""")

timetemp_table_create = ("""
    CREATE TABLE timetemp as 
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS time
    FROM staging_events_table
""")


# SQL SYNTAX FOR MOVING JSON DATA FROM S3 TO REDSHIFT
staging_events_copy = ("""COPY staging_events_table 
                          from {}
                          credentials 'aws_iam_role={}'
                          region 'us-west-2'
                          JSON {};
                        """).format(DWH_S3_LOG_DATA, DWH_ROLE_ARN, DWH_S3_LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs_table 
                         from {}
                         credentials 'aws_iam_role={}'
                         region 'us-west-2'
                         JSON 'auto';
                        """).format(DWH_S3_SONG_DATA, DWH_ROLE_ARN)


# SQL SYNTAX FOR INSERTING DATA IN ANALYTICAL TABLES
songplay_table_insert = ("""
INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' AS start_time,
       e.userId AS user_id,
       e.level AS level,
       s.song_id AS song_id,
       s.artist_id AS artist_id,
       e.sessionId AS session_id,
       s.artist_location AS location,
       e.userAgent AS user_agent
FROM staging_events_table e
JOIN staging_songs_table s
on 
(e.artist = s.artist_name 
AND e.song = s.title
AND e.length = s.duration)
WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO user_table (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId AS user_id,
       firstName AS first_name,
       lastName AS last_name,
       gender AS gender,
       level AS level
FROM staging_events_table;
""")


song_table_insert = ("""
INSERT INTO song_table (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id AS song_id,
       title AS title,
       artist_id AS artist_id,
       year AS year,
       duration AS duration
FROM staging_songs_table;
""")

artist_table_insert = ("""
INSERT INTO artist_table (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id AS artist_id,
       artist_name AS name,
       artist_location AS location,
       artist_latitude AS latitude,
       artist_longitude AS longitude
FROM staging_songs_table;
""")

time_table_insert = ("""
INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
SELECT time AS start_time,
    EXTRACT(hour FROM time) AS hour,
    EXTRACT(day FROM time) AS day,
    EXTRACT(week FROM time) AS week,
    EXTRACT(month FROM time) AS month,
    EXTRACT(year FROM time) AS year,
    EXTRACT(dow FROM time) AS weekday
FROM timetemp;
""")


# QUERY LISTS
create_staging_table_queries = [staging_events_table_create, staging_songs_table_create]

create_analytical_table_queries = [time_table_create, user_table_create, 
                                   song_table_create, artist_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, 
                      song_table_drop, artist_table_drop, time_table_drop, timetemp_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [time_table_insert, user_table_insert, song_table_insert, 
                        artist_table_insert, songplay_table_insert]
