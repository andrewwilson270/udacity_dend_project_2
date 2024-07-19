import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
user_table_drop = "DROP TABLE IF EXISTS dim_user"
song_table_drop = "DROP TABLE IF EXISTS dim_song"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR(300)
    ,auth            VARCHAR(50)
    ,first_name      VARCHAR(50)
    ,gender          VARCHAR(1)
    ,item_in_session INTEGER
    ,last_name       VARCHAR(50)
    ,length          DECIMAL(9, 5)
    ,level           VARCHAR(10)
    ,location        VARCHAR(300)
    ,method          VARCHAR(6)
    ,page            VARCHAR(50)
    ,registration    DECIMAL(14, 1)
    ,session_id      INTEGER
    ,song            VARCHAR(300)
    ,status          INTEGER
    ,ts              BIGINT
    ,user_agent      VARCHAR(150)
    ,user_id         VARCHAR(10)
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs        INTEGER
    ,artist_id        VARCHAR(50)
    ,artist_latitude  DECIMAL(10, 5)
    ,artist_longitude DECIMAL(10, 5)
    ,artist_location  VARCHAR(300)
    ,artist_name      VARCHAR(300)
    ,song_id          VARCHAR(50)
    ,title            VARCHAR(300)
    ,duration         DECIMAL(9, 5)
    ,year             INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY
    ,start_time  TIMESTAMP NOT NULL
    ,user_id     VARCHAR(10)
    ,level       VARCHAR(10)
    ,song_id     VARCHAR(300) NOT NULL
    ,artist_id   VARCHAR(50) NOT NULL
    ,session_id  INTEGER
    ,location    VARCHAR(300)
    ,user_agent  VARCHAR(150)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user (
    user_id    VARCHAR(10) PRIMARY KEY
    ,first_name VARCHAR(50)
    ,last_name  VARCHAR(50)
    ,gender     VARCHAR(1)
    ,level      VARCHAR(10)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song (
    song_id   VARCHAR(50) PRIMARY KEY
    ,title     VARCHAR(300) NOT NULL
    ,artist_id VARCHAR(50)
    ,year      INTEGER
    ,duration  DECIMAL(9, 5) NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_id VARCHAR(50) PRIMARY KEY
    ,name      VARCHAR(300) NOT NULL
    ,location  VARCHAR(300)
    ,latitude  DECIMAL(10, 5)
    ,longitude DECIMAL(10, 5)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time (
    start_time TIMESTAMP PRIMARY KEY
    ,hour       INTEGER
    ,day        INTEGER
    ,week       INTEGER
    ,month      INTEGER
    ,year       INTEGER
    ,weekday    INTEGER
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {} 
iam_role {}
FORMAT AS JSON {};
""").format(
    config.get('S3', 'LOG_DATA'), 
    config.get('IAM_ROLE', 'ARN'), 
    config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs from {} 
iam_role {}
FORMAT AS JSON 'auto';
""").format(
    config.get('S3', 'SONG_DATA'), 
    config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplay (
    start_time
    ,user_id
    ,level
    ,song_id
    ,artist_id
    ,session_id
    ,location
    ,user_agent
)
SELECT DISTINCT 
    to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS')
    ,se.user_id
    ,se.level as level
    ,ss.song_id as song_id
    ,ss.artist_id as artist_id
    ,se.session_id as session_id
    ,se.location as location
    ,se.user_agent as user_agent
FROM staging_events se
INNER JOIN staging_songs ss 
    ON se.song = ss.title 
    AND se.artist = ss.artist_name
    AND se.length = ss.duration
where se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO dim_user (
    user_id
    ,first_name
    ,last_name
    ,gender
    ,level
)
SELECT DISTINCT 
    user_id
    ,first_name
    ,last_name
    ,gender
    ,level
FROM staging_events
WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO dim_song (
    song_id
    ,title
    ,artist_id
    ,year
    ,duration
)
SELECT DISTINCT 
    song_id
     ,title
     ,artist_id
     ,year
     ,duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO dim_artist (
    artist_id
    ,name
    ,location
    ,latitude
    ,longitude
)
SELECT DISTINCT 
    artist_id
    ,artist_name 
    ,artist_location
    ,artist_latitude
    ,artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO dim_time (
    start_time
    ,hour
    ,day
    ,week
    ,month
    ,year
    ,weekday
)
SELECT DISTINCT 
    TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'
    ,EXTRACT(hour from TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')
    ,EXTRACT(day from TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')
    ,EXTRACT(week from TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')
    ,EXTRACT(month from TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')
    ,EXTRACT(year from TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')
    ,EXTRACT(weekday from TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')
FROM staging_events
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
