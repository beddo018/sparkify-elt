# sparkify-elt
The purpose of this project is to provide Sparkify, a song streaming service, a platform for their data science team to analyze song plays and user preferences. The data comes from streaming logs and song metadata.


## File usage

### create_tables.py
Run this script to create the staging and sink tables in S3 - `staging_events`, `staging_songs`, `song_plays`, `artists`, `songs`, `time`, and `users` tables.

### etl.py
Contains the script for staging the raw event and song data into S3, then load it into a standard fact-dimension star schema for OLAP queries.

### sql_queries.py
Contains the raw SQL queries for creating, dropping, copying and inserting tables and data for use in etl.py and create-tables.py.

### dwh.cfg
This file contains constants used for database identification and authentication in S3 and Redshift and is formatted as follows:
```
    [CLUSTER]
    HOST=''
    DB_NAME=''
    DB_USER=''
    DB_PASSWORD=''
    DB_PORT=

    [IAM_ROLE]
    ARN=''

    [S3]
    LOG_DATA=''
    LOG_JSONPATH=''
    SONG_DATA=''
```
   
## Database design
I have created the database such that the loading tables each contain sortkeys to allow for faster processing and distkey linked to songs themselves, as opposed to users.

There are two staging tables:
```
staging_events:   
    artist VARCHAR
    auth VARCHAR
    first_name VARCHAR 
    gender VARCHAR
    item_in_session INTEGER
    last_name VARCHAR 
    length INTEGER 
    level VARCHAR
    location VARCHAR
    method VARCHAR
    page VARCHAR
    registration FLOAT 
    session_id INTEGER 
    song VARCHAR 
    status INT
    ts INT
    user_agent VARCHAR
    user_id INT

staging_songs:
    num_songs INT
    artist_id VARCHARL
    artist_latitude DECIMAL
    artist_longitude DECIMAL
    artist_location VARCHAR
    artist_name VARCHARL
    song_id VARCHAR
    title VARCHAR
    duration DECIMAL
    year SMALLINT
```

Which are processed into one fact table and four dimension tables:
```
(fact) 
song_plays:
    songplay_id INTEGER
    start_time DATE
    user_id INT
    level VARCHAR
    song_id VARCHAR
    artist_id VARCHAR
    session_id INT
    location VARCHAR
    user_agent VARCHAR

(dimensional)
users:
    user_id INT  SORTKEY
    first_name VARCHAR(100) 
    last_name VARCHAR(100) 
    gender VARCHAR(100)
    level VARCHAR(100) 

songs:
    song_id VARCHAR(100)  SORTKEY DISTKEY
    title VARCHAR(100) 
    artist_id VARCHAR(100) 
    year SMALLINT
    duration DECIMAL

artists:
    artist_id VARCHAR
    name VARCHAR 
    location VARCHAR
    lattitude DECIMAL
    longitude DECIMAL
        
time:
    start_time DATE
    hour SMALLINT
    day SMALLINT
    week SMALLINT
    month SMALLINT
    year SMALLINT
    weekday VARCHAR
```

This theoretically allows the ETL process to take advantage of Redshift's massively parallel processing capabilities and optimizes for queries related to songs.
