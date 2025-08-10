
# This Python script is used to generate two fake data files with 20 million rows in CSV and JSON formats.

import csv
import json
import random
import string
from datetime import datetime, timedelta

def random_username(length=10):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def random_country():
    countries = [
        "USA", "Canada", "Germany", "France", "Italy", "Spain", "Mexico", "Brazil", "Argentina",
        "China", "India", "Japan", "South Korea", "Russia", "Australia", "New Zealand",
        "South Africa", "Egypt", "Nigeria", "Kenya", "Turkey", "Saudi Arabia", "Iran",
        "Poland", "Netherlands", "Belgium", "Sweden", "Norway", "Finland", "Denmark",
        "Portugal", "Greece", "Czech Republic", "Hungary", "Romania", "Bulgaria", "Croatia",
        "Slovakia", "Slovenia", "Lithuania", "Latvia", "Estonia", "Ukraine", "Belarus",
        "Chile", "Colombia", "Peru", "Venezuela", "Costa Rica", "Panama", "Guatemala",
        "Honduras", "El Salvador", "Nicaragua", "Cuba", "Jamaica", "Bahamas", "Iceland",
        "Ireland", "Switzerland", "Austria", "Luxembourg", "Monaco", "Liechtenstein",
        "Malta", "Cyprus", "United Kingdom", "Singapore", "Malaysia", "Indonesia", "Philippines",
        "Vietnam", "Thailand", "Bangladesh", "Pakistan", "Nepal", "Sri Lanka", "Morocco",
        "Algeria", "Tunisia", "Libya", "Sudan", "Ethiopia", "Uganda", "Tanzania", "Zambia",
        "Zimbabwe", "Botswana", "Namibia", "Angola", "Mozambique", "Madagascar", "Malawi"
    ]
    return random.choice(countries)

def random_timestamp(start_year=2010, end_year=2025):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86400)
    dt = start + timedelta(days=random_days, seconds=random_seconds)
    return int(dt.timestamp())

def random_datetime_iso(start_year=2010, end_year=2025):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86400)
    dt = start + timedelta(days=random_days, seconds=random_seconds)
    return dt.isoformat()

def generate_channels_csv(filename, num_records):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['username', 'total_video_visit', 'video_count', 'start_date_timestamp', 'followers_count', 'country', 'update_count']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        usernames = set()
        for i in range(num_records):
            # username must be unique
            while True:
                username = random_username(12)
                if username not in usernames:
                    usernames.add(username)
                    break
            total_video_visit = random.randint(0, 10_000_000)
            video_count = random.randint(0, 10_000)
            start_date_timestamp = random_timestamp()
            followers_count = random.randint(0, 5_000_000)
            country = random_country()
            update_count = random.randint(0, 100)
            writer.writerow({
                'username': username,
                'total_video_visit': total_video_visit,
                'video_count': video_count,
                'start_date_timestamp': start_date_timestamp,
                'followers_count': followers_count,
                'country': country,
                'update_count': update_count
            })
    return list(usernames)

def generate_videos_json(filename, usernames, num_records):
    with open(filename, 'w', encoding='utf-8') as jsonfile:
        for i in range(num_records):
            # Randomly pick a username from the existing channels to maintain relation
            owner_username = random.choice(usernames)
            obj_id = i + 1  # id related to channel id, sequential
            visit_count = random.randint(0, 1_000_000)
            duration = random.randint(1, 7200)  # duration in seconds (max 2 hours)
            posted_timestamp = random_timestamp()
            posted_date = datetime.fromtimestamp(posted_timestamp).strftime('%Y-%m-%d %H:%M:%S')
            comments_count = random.randint(0, 1000)
            comments = ' - '.join(['comment'] * comments_count) if comments_count > 0 else ''
            like_count = random.choice([None, random.randint(0, 50000)])
            is_deleted = random.choice([True, False])
            created_at = posted_timestamp * 1000
            expire_at = (posted_timestamp + 30*24*3600) * 1000  # expire after 30 days

            doc = {
                "_id": f"video_{obj_id}",
                "object": {
                    "id": obj_id,
                    "owner_username": owner_username,
                    "owner_id": f"user_{owner_username}",
                    "title": f"Video Title {obj_id}",
                    "tags": "tag1,tag2",
                    "uid": f"uid_{obj_id}",
                    "visit_count": visit_count,
                    "owner_name": f"Owner Name {obj_id}",
                    "duration": duration,
                    "posted_date": posted_date,
                    "posted_timestamp": posted_timestamp,
                    "comments": comments,
                    "like_count": like_count,
                    "description": f"Description of video {obj_id}",
                    "is_deleted": is_deleted
                },
                "created_at": created_at,
                "expire_at": expire_at,
                "update_count": random.randint(0, 10)
            }
            jsonfile.write(json.dumps(doc) + "\n")

if __name__ == "__main__":
    NUM_RECORDS = 20000000  # 20 million
    NUM_RECORDS_TEST = 10000

    print("Generating CSV data...")
    usernames = generate_channels_csv("channels_stripped.csv", NUM_RECORDS)
    print("Generating JSON data...")
    generate_videos_json("rep_videos.json", list(usernames), NUM_RECORDS)
    print("Done.")
