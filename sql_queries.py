import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE staging_events"
staging_songs_table_drop = "DROP TABLE staging_songs"
songplay_table_drop = "DROP TABLE songplays"
user_table_drop = "DROP TABLE users"
song_table_drop = "DROP TABLE songs"
artist_table_drop = "DROP TABLE artists"
time_table_drop = "DROP TABLE time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events 
        (
            artist VARCHAR(50) NOT NULL,
            auth VARCHAR(25),
            first_name VARCHAR(50) NOT NULL,
            gender VARCHAR(1),
            item_in_session INTEGER,
            last_name VARCHAR(50) NOT NULL,
            length INTEGER NOT NULL,
            level VARCHAR(15),
            location VARCHAR(100),
            method VARCHAR(10),
            page VARCHAR(20),
            registration VARCHAR(50) NOT NULL,
            session_id INTEGER NOT NULL,
            song VARCHAR(50) NOT NULL,
            status INT,
            ts INT,
            user_agent VARCHAR(50),
            user_id INT
        )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INT NOT NULL,
        artist_id VARCHAR(50) NOT NULL,
        artist_latitude DECIMAL,
        artist_longitude DECIMAL,
        artist_location VARCHAR(50),
        artist_name VARCHAR(50) NOT NULL,
        song_id VARCHAR(50) NOT NULL,
        title VARCHAR(50) NOT NULL,
        duration DECIMAL NOT NULL,
        year SMALLINT,
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_plays (
        #songkey should be distkey, date should be sortkey
        songplay_id INTEGER NOT NULL,
        start_time DATE NOT NULL SORTKEY,
        user_id INT NOT NULL,
        level VARCHAR(15) NOT NULL,
        song_id VARCHAR(50) NOT NULL DISTKEY,
        artist_id VARCHAR(50) NOT NULL,
        session_id INT NOT NULL,
        location VARCHAR(100),
        user_agent VARCHAR(50)
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT NOT NULL SORTKEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        gender VARCHAR(1),
        level VARCHAR(15) NOT NULL,
    )
""")

song_table_create = ("""
    song_id VARCHAR(50) NOT NULL SORTKEY DISTKEY,
    title VARCHAR(50) NOT NULL,
    artist_id VARCHAR(50) NOT NULL,
    year SMALLINT,
    duration DECIMAL NOT NULL,
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(50) NOT NULL SORTKEY,
        name VARCHAR(50) NOT NULL,
        location VARCHAR(50),
        lattitude DECIMAL,
        longitude DECIMAL,
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
        weekday VARCHAR(10)
    )
""")

# STAGING TABLES

staging_events_copy = ("""
copy log_data from '{}' 
credentials 'aws_iam_role={}'
gzip region 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
copy song_data from '{}' 
credentials 'aws_iam_role={}'
gzip region 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
