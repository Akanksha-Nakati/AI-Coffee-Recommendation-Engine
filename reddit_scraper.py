from dotenv import load_dotenv
import os
import time
import psycopg2
import praw
from datetime import datetime

load_dotenv()

r = praw.Reddit(
    client_id=os.environ['REDDIT_CLIENT_ID'],
    client_secret=os.environ['REDDIT_CLIENT_SECRET'],
    user_agent=os.environ.get('REDDIT_USER_AGENT', 'coffee-scraper')
)

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError("Set DATABASE_URL in your environment or .env")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

def save_submission(sub):
    """
    Save a Reddit submission to raw_posts table (matching schema).
    """
    try:
        # Convert Reddit timestamp to datetime
        posted_at = datetime.fromtimestamp(sub.created_utc)
        
        cur.execute("""
            INSERT INTO raw_posts (
                id, source, title, body, url, author, posted_at, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, (
            sub.id,
            'reddit',
            sub.title,
            sub.selftext or '',
            sub.url,
            str(sub.author) if sub.author else '[deleted]',
            posted_at,
            None  # Can store additional metadata as JSONB if needed
        ))
        
        conn.commit()
        print(f"✓ Added post: {sub.id} - {sub.title[:50]}...")
        
    except Exception as e:
        conn.rollback()
        print(f"✗ Error saving post {sub.id}: {e}")

# Fetch and save posts
print("Scraping r/Coffee...")
count = 0

for submission in r.subreddit("Coffee").hot(limit=200):
    save_submission(submission)
    count += 1
    time.sleep(0.5)  # Rate limiting

print(f"\n✓ Scraped {count} posts from r/Coffee")

cur.close()
conn.close()