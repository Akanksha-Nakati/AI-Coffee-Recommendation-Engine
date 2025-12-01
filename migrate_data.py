"""
Migrate data from raw_reviews to raw_posts
Parses the JSON format and creates properly structured records
"""
import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ.get('DATABASE_URL')

def migrate_data():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    print("=" * 70)
    print("MIGRATING DATA: raw_reviews → raw_posts")
    print("=" * 70)
    
    # Get all records from raw_reviews
    cur.execute("SELECT * FROM raw_reviews ORDER BY scraped_at")
    old_records = cur.fetchall()
    
    print(f"\nFound {len(old_records)} records in raw_reviews")
    
    migrated = 0
    skipped = 0
    errors = 0
    
    for record in old_records:
        try:
            # Parse the JSON data
            data = json.loads(record['json'])
            
            # Extract fields
            post_id = record['id']
            source = record['source']
            title = data.get('title', '')
            body = data.get('body', '')
            url = data.get('url', '')
            author = data.get('author', '')
            scraped_at = record['scraped_at']
            
            # Insert into raw_posts
            cur.execute("""
                INSERT INTO raw_posts (
                    id, source, title, body, url, author, 
                    scraped_at, posted_at, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
                RETURNING id
            """, (
                post_id,
                source,
                title,
                body,
                url,
                author,
                scraped_at,
                None,  # posted_at - we don't have this from old schema
                None   # metadata
            ))
            
            result = cur.fetchone()
            
            if result:
                migrated += 1
                if migrated % 50 == 0:
                    print(f"  Migrated {migrated} records...")
            else:
                skipped += 1  # Already exists
                
        except Exception as e:
            errors += 1
            print(f"  ✗ Error migrating record {record['id']}: {e}")
            conn.rollback()
            continue
    
    # Commit all changes
    conn.commit()
    
    print("\n" + "=" * 70)
    print("MIGRATION COMPLETE")
    print("=" * 70)
    print(f"✓ Migrated: {migrated}")
    print(f"○ Skipped (already exist): {skipped}")
    print(f"✗ Errors: {errors}")
    
    # Verify
    cur.execute("SELECT COUNT(*) as count FROM raw_posts")
    new_count = cur.fetchone()['count']
    print(f"\nraw_posts now has {new_count} records")
    
    cur.close()
    conn.close()
    
    print("\n✓ You can now run: python nlp_processor.py")

if __name__ == "__main__":
    migrate_data()