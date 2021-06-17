import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create= ("""
	CREATE TABLE staging_events(
		artist VARCHAR,
		auth VARCHAR,
		firstName VARCHAR,
		gender VARCHAR,
		itemInSession INTEGER,
		lastName VARCHAR,
		length FLOAT,
		level VARCHAR,
		location VARCHAR,
		method VARCHAR,
		page VARCHAR,
		registration INTEGER,
		sessionId INTEGER,
		song VARCHAR,
		status INTEGER,
		ts TIMESTAMP,
		userAgent VARCHAR,
		userId VARCHAR
	)
""")

staging_songs_table_create = ("""
	CREATE TABLE staging_songs(
		num_songs INTEGER,
		artist_id VARCHAR,
		artist_latitude FLOAT,
		artist_longitude FLOAT,
		artist_location VARCHAR,
		artist_name VARCHAR,
		song_id VARCHAR,
		title VARCHAR,
		duration FLOAT,
		year INTEGER
	)
""")

songplay_table_create = ("""
	CREATE TABLE songplays(
		songplay_id INTEGER IDENTITY(0, 1) PRIMARY KEY,
		start_time TIMESTAMP SORT_KEY DIST_KEY,
		user_id VARCHAR NOT NULL,
		level VARCHAR,
		song_id VARCHAR NOT NULL,
		artist_id VARCHAR NOT NULL,
		session_id VARCHAR NOT NULL,
		location VARCHAR,
		user_agent VARCHAR
	)
""")

user_table_create = ("""
	CREATE TABLE users(
		user_id VARCHAR SORT KEY PRIMARY KEY,
		first_name VARCHAR,
		last_name VARCHAR,
		gender VARCHAR,
		level VARCHAR
	)
""")

song_table_create = ("""
	CREATE TABLE songs(
		song_id VARCHAR SORT KEY PRIMARY KEY,
		title VARCHAR,
		artist_id VARCHAR NOT NULL,
		year INTEGER,
		duration FLOAT
	)
""")

artist_table_create = ("""
	CREATE TABLE artists(
		artist_id VARCHAR SORT KEY PRIMARY KEY,
		name VARCHAR,
		location VARCHAR,
		latitude VARCHAR,
		longitude VARCHAR
	)
""")

time_table_create = ("""
	CREATE TABLE time(
		start_time TIMESTAMP SORT KEY DIST KEY PRIMARY KEY,
		hour INTEGER,
		day INTEGER,
		week INTEGER,
		month INTEGER,
		year INTEGER,
		weekday VARCHAR(20)
	)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'], log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES

user_table_insert = ("""
	INSERT INTO users (user_id, first_name, last_name, gender, level)
	SELECT DISTINCT(userId) as user_id, firstName as first_name, lastName as last_name, gender, level from
	staging_events WHERE user_id IS NOT NULL AND page='NextSong';
""")

song_table_insert = ("""
	INSERT INTO songs (song_id, title, artist_id, year, duration)
	SELECT DISTINCT(song_id) as song_id, title, artist_id, year, duration from staging_songs where song_id IS NOT NULL;
""")

artist_table_insert = ("""
	INSERT INTO artists (artist_id, name, location, latitude, longitude)
	SELECT DISTINCT(artist_id) as artist_id, artist_name as name, artist_location as location,
	artist_latitude as latitide, artist_longitude as longityde FROM staging_songs where artist_id IS NOT NULL;
""")

time_table_insert = ("""
	INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT(start_time)                AS start_time,
            EXTRACT(hour FROM start_time)       AS hour,
            EXTRACT(day FROM start_time)        AS day,
            EXTRACT(week FROM start_time)       AS week,
            EXTRACT(month FROM start_time)      AS month,
            EXTRACT(year FROM start_time)       AS year,
            EXTRACT(dayofweek FROM start_time)  as weekday
    FROM songplays;
""")

songplay_table_insert = ("""
	INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT(e.ts)  AS start_time, 
            e.userId        AS user_id, 
            e.level         AS level, 
            s.song_id       AS song_id, 
            s.artist_id     AS artist_id, 
            e.sessionId     AS session_id, 
            e.location      AS location, 
            e.userAgent     AS user_agent
    FROM staging_events e
    JOIN staging_songs  s   ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page  ==  'NextSong'
""")

# GET NUMBER OF ROWS IN EACH TABLE
get_number_staging_events = ("""
    SELECT COUNT(*) FROM staging_events
""")

get_number_staging_songs = ("""
    SELECT COUNT(*) FROM staging_songs
""")

get_number_songplays = ("""
    SELECT COUNT(*) FROM songplays
""")

get_number_users = ("""
    SELECT COUNT(*) FROM users
""")

get_number_songs = ("""
    SELECT COUNT(*) FROM songs
""")

get_number_artists = ("""
    SELECT COUNT(*) FROM artists
""")

get_number_time = ("""
    SELECT COUNT(*) FROM time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
