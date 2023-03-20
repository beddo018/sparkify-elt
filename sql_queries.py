import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.config')
arn = config.get('IAM_ROLE', 'ARN')
log_data = config.get('S3', 'LOG_DATA')
song_data = config.get('S3', 'SONG_DATA')
log_json = config.get('S3', 'LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS song_plays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events 
        (
            artist VARCHAR(100) NOT NULL,
            auth VARCHAR(100),
            first_name VARCHAR(100) NOT NULL,
            gender VARCHAR(100),
            item_in_session INTEGER,
            last_name VARCHAR(100) NOT NULL,
            length INTEGER NOT NULL,
            level VARCHAR(100),
            location VARCHAR(100),
            method VARCHAR(100),
            page VARCHAR(100),
            registration FLOAT NOT NULL,
            session_id INTEGER NOT NULL,
            song VARCHAR(100) NOT NULL,
            status INT,
            ts INT,
            user_agent VARCHAR(100),
            user_id INT
        )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INT NOT NULL,
        artist_id VARCHAR(100) NOT NULL,
        artist_latitude DECIMAL,
        artist_longitude DECIMAL,
        artist_location VARCHAR(100),
        artist_name VARCHAR(100) NOT NULL,
        song_id VARCHAR(100) NOT NULL,
        title VARCHAR(100) NOT NULL,
        duration DECIMAL NOT NULL,
        year SMALLINT
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_plays (
        songplay_id INTEGER NOT NULL,
        start_time DATE NOT NULL SORTKEY,
        user_id INT NOT NULL,
        level VARCHAR(100) NOT NULL,
        song_id VARCHAR(100) NOT NULL DISTKEY,
        artist_id VARCHAR(100) NOT NULL,
        session_id INT NOT NULL,
        location VARCHAR(100),
        user_agent VARCHAR(100)
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT NOT NULL SORTKEY,
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL,
        gender VARCHAR(100),
        level VARCHAR(100) NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(100) NOT NULL SORTKEY DISTKEY,
        title VARCHAR(100) NOT NULL,
        artist_id VARCHAR(100) NOT NULL,
        year SMALLINT,
        duration DECIMAL NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(100) NOT NULL SORTKEY,
        name VARCHAR(100) NOT NULL,
        location VARCHAR(100),
        lattitude DECIMAL,
        longitude DECIMAL
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time DATE NOT NULL SORTKEY,
        hour SMALLINT,
        day SMALLINT,
        week SMALLINT,
        month SMALLINT,
        year SMALLINT,
        weekday VARCHAR(100)
    )
""")

# STAGING TABLES

staging_events_copy = ("""
copy log_data from {} 
credentials 'aws_iam_role={}'
format as json {}
gzip region 'us-west-2';
""").format(log_data, arn, log_json)

staging_songs_copy = ("""
copy song_data from {} 
credentials 'aws_iam_role={}'
gzip region 'us-west-2';
""").format(song_data, arn)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO song_plays (
        songplay_id,
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    ) VALUES (
        SELECT
            DISTINCT(e.ts::integer) AS songplay_id,
            TO_TIMESTAMP(e.ts) AS start_time,
            e.user_id,
            e.level,
            s.song_id,
            s.artist_id,
            e.session_id,
            e.location,
            e.user_agent
        FROM staging_events e
        JOIN staging_songs s ON (s.song = e.title AND s.artist_name = e.artist)
        WHERE e.page='NextSong'
    )
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    ) VALUES (
        SELECT
            DISTINCT(e.user_id)
            e.first_name,
            e.last_name,
            e.gender,
            e.level
        FROM staging_events e
    )
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    ) VALUES (
        SELECT
            DISTINCT(s.song_id),
            s.title,
            s.artist_id,
            s.year,
            s.duration
        FROM staging_songs s
    )
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        lattitude,
        longitude
    ) VALUES (
        SELECT
            DISTINCT(s.artist_id),
            s.artist_name AS name,
            s.artist_location AS location,
            s.artist_lattitude AS lattitude,
            s.artist_longitude AS longitude
        FROM staging_songs s
    )
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    ) VALUES (
        SELECT
            DISTINCT(TO_TIMESTAMP(e.ts::integer)) AS start_time,
            EXTRACT(hour from start_time) AS hour,
            EXTRACT(day from start_time) AS day,
            EXTRACT(week from start_time) AS week,
            EXTRACT(month from start_time) AS month,
            EXTRACT(year from start_time) AS year,
            EXTRACT(weekday from start_time) AS weekday
            FROM staging_events e
    )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
