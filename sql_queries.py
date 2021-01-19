# DROP TABLES

songplay_table_drop = " DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTs songplays (songplay_id SERIAL PRIMARY KEY , start_time BIGINT NOT NULL, user_id varchar, level varchar, song_id text, artist_id text , session_id varchar, location varchar, user_agent varchar) """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id text, first_name text, last_name text, gender varchar , level varchar,PRIMARY KEY (user_id) ) """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs ( song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year int, duration real)""")


artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar, name varchar, location varchar, latitude numeric, longitude numeric, PRIMARY KEY (artist_id) )""")

time_table_create = ("""CREATE TABLE time (start_time BIGINT PRIMARY KEY, hour varchar , day varchar, week varchar, month varchar, year varchar, weekday varchar)""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""")

user_table_insert = ("""INSERT INTO users ( user_id, first_name, last_name, gender, level) VALUES( %s, %s, %s, %s, %s)ON CONFLICT DO NOTHING""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES( %s, %s, %s, %s, %s)  ON CONFLICT DO NOTHING """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)  VALUES( %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month,year, weekday)  VALUES( %s, %s, %s, %s, %s, %s, %s)ON CONFLICT DO NOTHING""")

# FIND SONGS

song_select = ("""SELECT * From songs""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]