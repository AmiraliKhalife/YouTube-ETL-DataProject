--create table
CREATE TABLE IF NOT EXISTS channels(
                        id SERIAL PRIMARY KEY,
                        username TEXT NOT NULL UNIQUE,
                        total_video_visit BIGINT CHECK(total_video_visit >= 0),
                        video_count INT CHECK(video_count >= 0),
                        start_date_timestamp BIGINT CHECK(start_date_timestamp >= 0),
                        followers_count BIGINT CHECK(followers_count >= 0),
                        country TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        update_count INT
                      );


--insert data into table
INSERT INTO channels(username, total_video_visit, video_count, start_date_timestamp,
                                  followers_count, country, update_count)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                username = EXCLUDED.username,
                total_video_visit = EXCLUDED.total_video_visit,
                video_count = EXCLUDED.video_count,
                start_date_timestamp = EXCLUDED.start_date_timestamp,
                followers_count = EXCLUDED.followers_count,
                country = EXCLUDED.country,
                update_count = EXCLUDED.update_count;



--create index on ids:
create index channel_id on channels(id);