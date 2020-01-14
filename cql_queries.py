create_keyspace = "CREATE KEYSPACE IF NOT EXISTS music_library WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : '3'}"

create_table1 = "CREATE TABLE IF NOT EXISTS played_songs_by_session (session_id int, item_in_session int, artist_name text, song_title text, song_length decimal, PRIMARY KEY(session_id, item_in_session))"
create_table2 = "CREATE TABLE IF NOT EXISTS played_songs_by_user (user_id int, session_id int, artist_name text, item_in_session int, song_title text, user_fname text, user_lname text, PRIMARY KEY((user_id, session_id), item_in_session))"
create_table3 = "CREATE TABLE IF NOT EXISTS user_detail_by_song (song_title text, artist_name text, user_fname text, user_lname text, PRIMARY KEY(song_title, artist_name))"

drop_table1 =  "DROP TABLE IF EXISTS played_songs_by_session"
drop_table2 =  "DROP TABLE IF EXISTS played_songs_by_user"
drop_table3 =  "DROP TABLE IF EXISTS user_detail_by_song"

create_table_queries = [create_table1, create_table2, create_table3]
drop_table_queries = [drop_table1, drop_table2, drop_table3]
