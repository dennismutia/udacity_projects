import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay;"
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS dim_song;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE staging_events(
    event_id        INT IDENTITY(0,1),
    artist_name     VARCHAR(MAX),
    auth            TEXT,
    user_first_name TEXT,
    user_gender     TEXT,
    item_in_session	INTEGER,
    user_last_name  TEXT,
    song_length	    DOUBLE PRECISION, 
    user_level      TEXT,
    location        TEXT,	
    method          TEXT,
    page            TEXT,	
    registration    TEXT,	
    session_id	    BIGINT,
    song_title      TEXT,
    status          INTEGER,
    ts              TIMESTAMP,
    user_agent      TEXT,	
    user_id         INT,
    PRIMARY KEY (event_id))
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    song_id             TEXT,
    num_songs           INTEGER,
    artist_id           TEXT,
    artist_latitude     DOUBLE PRECISION,
    artist_longitude    DOUBLE PRECISION,
    artist_location     VARCHAR(MAX),
    artist_name         VARCHAR(MAX),
    title               TEXT,
    duration            DOUBLE PRECISION,
    year                INTEGER,
    PRIMARY KEY (song_id))
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay 
(
    songplay_id          INT IDENTITY(0,1) PRIMARY KEY,
    start_time           TIMESTAMP sortkey,
    user_id              INT NOT NULL,
    level                TEXT,
    song_id              TEXT NOT NULL distkey,
    artist_id            TEXT,
    session_id           INT NOT NULL,
    location             TEXT,
    user_agent           TEXT NOT NULL
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user
(
    user_id INT PRIMARY KEY,
    first_name      TEXT,
    last_name       TEXT,
    gender          TEXT,
    level           TEXT
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song
(
    song_id     TEXT PRIMARY KEY,
    title       TEXT,
    artist_id   TEXT distkey,
    year        INT,
    duration    FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist
(
    artist_id          VARCHAR PRIMARY KEY,
    name               VARCHAR,
    location           VARCHAR,
    latitude           FLOAT,
    longitude          FLOAT
)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time
(
    start_time    TIMESTAMP PRIMARY KEY,
    hour          INT,
    day           INT,
    week          INT,
    month         INT,
    year          INT,
    weekday       INT
)
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from '{}'
 credentials 'aws_iam_role={}'
 region 'us-west-2' 
 TIMEFORMAT as 'epochmillisecs'
 STATUPDATE OFF
 JSON '{}'""").format(config.get('S3','LOG_DATA'),
                        config.get('IAM_ROLE', 'ARN'),
                        config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2' 
    STATUPDATE OFF
    JSON 'auto' truncatecolumns;
    """).format(config.get('S3','SONG_DATA'), 
                config.get('IAM_ROLE', 'ARN')            
)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                se.user_id as user_id,
                se.user_level as level,
                ss.song_id as song_id,
                ss.artist_id as artist_id,
                se.session_id as session_id,
                se.location as location,
                se.user_agent as user_agent
FROM staging_events se
JOIN staging_songs ss ON se.song_title = ss.title AND se.artist_name = ss.artist_name;
""")

user_table_insert = ("""
INSERT INTO dim_user(user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id as user_id,
                user_first_name as first_name,
                user_last_name as last_name,
                user_gender as gender,
                user_level as level
FROM staging_events
where user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO dim_song(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id as song_id,
                title as title,
                artist_id as artist_id,
                year as year,
                duration as duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO dim_artist(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id as artist_id,
                artist_name as name,
                artist_location as location,
                artist_latitude as latitude,
                artist_longitude as longitude
FROM staging_songs
where artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
SELECT distinct ts,
                EXTRACT(hour from ts),
                EXTRACT(day from ts),
                EXTRACT(week from ts),
                EXTRACT(month from ts),
                EXTRACT(year from ts),
                EXTRACT(weekday from ts)
FROM staging_events
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
