CREATE TABLE IF NOT EXISTS bronze_channels
                 (
                     id UInt32,
                     username String,
                     total_video_visit UInt64,
                     video_count UInt32,
                     start_date_timestamp UInt64,
                     followers_count UInt64,
                     country String,
                     created_at DateTime DEFAULT now(),
                     update_count UInt32
                 )
                 ENGINE = MergeTree
                 ORDER BY (id)

-----------------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze_videos
(
    id UInt32,
    owner_username String,
    owner_id String,
    title String,
    tags String,
    uid String,
    visit_count UInt64,
    owner_name String,
    duration UInt32,
    posted_date DateTime,
    posted_timestamp UInt64,
    comments String,
    like_count UInt64,
    description String,
    is_deleted UInt8,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (id);

---------------------------------------------