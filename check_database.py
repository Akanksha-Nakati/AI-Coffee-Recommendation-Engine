"""
Database diagnostic tool - Check what's in your database
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ.get('DATABASE_URL')

def check_database():
    if not DATABASE_URL:
        print("❌ DATABASE_URL not set!")
        return
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=" * 70)
        print("DATABASE DIAGNOSTIC REPORT")
        print("=" * 70)
        
        # 1. Check what tables exist
        print("\n1. EXISTING TABLES:")
        print("-" * 70)
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = cur.fetchall()
        
        if not tables:
            print("   ⚠️  No tables found in database!")
        else:
            for table in tables:
                print(f"   ✓ {table['table_name']}")
        
        # 2. Check row counts for common tables
        print("\n2. TABLE ROW COUNTS:")
        print("-" * 70)
        
        common_tables = ['raw_posts', 'raw_reviews', 'processed_reviews', 
                        'flavor_terms', 'entities', 'nlp_extractions']
        
        for table_name in common_tables:
            try:
                cur.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                result = cur.fetchone()
                count = result['count']
                
                if count > 0:
                    print(f"   ✓ {table_name}: {count:,} rows")
                else:
                    print(f"   ○ {table_name}: 0 rows (empty)")
                    
            except psycopg2.errors.UndefinedTable:
                print(f"   ✗ {table_name}: Table doesn't exist")
                conn.rollback()
        
        # 3. If raw_reviews exists, check structure
        print("\n3. RAW DATA CHECK:")
        print("-" * 70)
        
        try:
            cur.execute("SELECT * FROM raw_reviews LIMIT 1")
            sample = cur.fetchone()
            if sample:
                print("   ✓ raw_reviews table has data")
                print(f"   Columns: {', '.join(sample.keys())}")
                print(f"   Sample ID: {sample['id']}")
            else:
                print("   ○ raw_reviews exists but is empty")
        except psycopg2.errors.UndefinedTable:
            conn.rollback()
            print("   ✗ raw_reviews table doesn't exist")
        
        try:
            cur.execute("SELECT * FROM raw_posts LIMIT 1")
            sample = cur.fetchone()
            if sample:
                print("   ✓ raw_posts table has data")
                print(f"   Columns: {', '.join(sample.keys())}")
                print(f"   Sample ID: {sample['id']}")
            else:
                print("   ○ raw_posts exists but is empty")
        except psycopg2.errors.UndefinedTable:
            conn.rollback()
            print("   ✗ raw_posts table doesn't exist")
        
        # 4. Check processed vs unprocessed
        print("\n4. PROCESSING STATUS:")
        print("-" * 70)
        
        # Try both table names
        for raw_table in ['raw_reviews', 'raw_posts']:
            try:
                cur.execute(f"""
                    SELECT 
                        COUNT(*) as total_raw,
                        COUNT(pr.id) as processed_count,
                        COUNT(*) - COUNT(pr.id) as unprocessed_count
                    FROM {raw_table} rr
                    LEFT JOIN processed_reviews pr ON rr.id = pr.post_id
                """)
                result = cur.fetchone()
                
                if result['total_raw'] > 0:
                    print(f"\n   Using table: {raw_table}")
                    print(f"   Total raw posts: {result['total_raw']:,}")
                    print(f"   Processed: {result['processed_count']:,}")
                    print(f"   Unprocessed: {result['unprocessed_count']:,}")
                    
                    if result['unprocessed_count'] == 0:
                        print("   ℹ️  All posts have been processed!")
                    else:
                        print(f"   ⚠️  {result['unprocessed_count']} posts ready for processing")
                        
            except psycopg2.errors.UndefinedTable:
                conn.rollback()
                continue
        
        # 5. Check NLP extractions
        print("\n5. NLP EXTRACTION STATUS:")
        print("-" * 70)
        
        try:
            cur.execute("""
                SELECT COUNT(*) as total FROM nlp_extractions
            """)
            result = cur.fetchone()
            print(f"   NLP extractions stored: {result['total']:,}")
            
            if result['total'] > 0:
                cur.execute("""
                    SELECT 
                        processed_review_id,
                        jsonb_array_length(flavors) as flavor_count,
                        jsonb_array_length(roasters) as roaster_count
                    FROM nlp_extractions
                    LIMIT 1
                """)
                sample = cur.fetchone()
                print(f"   Sample extraction has {sample['flavor_count']} flavors, {sample['roaster_count']} roasters")
                
        except psycopg2.errors.UndefinedTable:
            conn.rollback()
            print("   ✗ nlp_extractions table doesn't exist")
        
        # 6. Recommendations
        print("\n6. RECOMMENDATIONS:")
        print("-" * 70)
        
        # Check if we have the old schema (raw_reviews) or new (raw_posts)
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('raw_reviews', 'raw_posts')
        """)
        existing = [r['table_name'] for r in cur.fetchall()]
        
        if 'raw_reviews' in existing and 'raw_posts' not in existing:
            print("   ⚠️  You're using OLD SCHEMA (raw_reviews)")
            print("   Action needed:")
            print("   1. Create raw_posts table (see schema.sql)")
            print("   2. Migrate data: INSERT INTO raw_posts SELECT * FROM raw_reviews")
            print("   3. Update nlp_processor.py to use raw_posts")
            
        elif 'raw_posts' in existing:
            print("   ✓ You're using NEW SCHEMA (raw_posts)")
            
            cur.execute("SELECT COUNT(*) as c FROM raw_posts")
            count = cur.fetchone()['c']
            
            if count == 0:
                print("   Action needed:")
                print("   1. Run: python reddit_scraper.py")
            else:
                print("   Schema looks good!")
        else:
            print("   ❌ No raw data tables found!")
            print("   Action needed:")
            print("   1. Run: psql $DATABASE_URL < schema.sql")
            print("   2. Run: python reddit_scraper.py")
        
        print("\n" + "=" * 70)
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Error connecting to database: {e}")
        print(f"   DATABASE_URL: {DATABASE_URL[:30]}...")

if __name__ == "__main__":
    check_database()