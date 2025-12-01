"""
Complete setup and execution script for coffee recommendation system
Runs all steps in correct order
"""

import os
import psycopg2
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()
DATABASE_URL = os.environ.get('DATABASE_URL')

def run_sql_file(filepath):
    """Execute a SQL file"""
    logger.info(f"Executing SQL file: {filepath}")
    
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    try:
        with open(filepath, 'r') as f:
            sql = f.read()
            cur.execute(sql)
            conn.commit()
            logger.info(f"✓ Successfully executed {filepath}")
    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Error executing {filepath}: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def check_tables_exist():
    """Verify all required tables exist"""
    required_tables = [
        'raw_posts',
        'processed_reviews',
        'flavor_terms',
        'entities',
        'origins',
        'coffee_products',
        'flavor_extractions',
        'coffee_mentions',
        'product_reviews',
        'nlp_extractions'
    ]
    
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    
    existing_tables = [row[0] for row in cur.fetchall()]
    
    missing = [t for t in required_tables if t not in existing_tables]
    
    if missing:
        logger.error(f"Missing tables: {missing}")
        return False
    
    logger.info(f"✓ All {len(required_tables)} required tables exist")
    cur.close()
    conn.close()
    return True


def get_table_counts():
    """Get row counts for all tables"""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    tables = [
        'raw_posts',
        'processed_reviews',
        'flavor_extractions',
        'coffee_mentions',
        'product_reviews',
        'coffee_products',
        'entities',
        'flavor_terms'
    ]
    
    logger.info("\n" + "=" * 60)
    logger.info("DATABASE STATISTICS")
    logger.info("=" * 60)
    
    for table in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            logger.info(f"{table:.<40} {count:>10,} rows")
        except Exception as e:
            logger.error(f"{table}: Error - {e}")
    
    cur.close()
    conn.close()


def main():
    """Main execution flow"""
    
    logger.info("=" * 60)
    logger.info("COFFEE RECOMMENDATION SYSTEM SETUP")
    logger.info("=" * 60)
    
    if not DATABASE_URL:
        logger.error("DATABASE_URL not set!")
        return
    
    # Step 0: Create database schema
    logger.info("\n### STEP 0: CREATE DATABASE SCHEMA ###")
    if os.path.exists('schema.sql'):
        run_sql_file('schema.sql')
    else:
        logger.warning("schema.sql not found - assuming schema already exists")
    
    # Verify tables
    if not check_tables_exist():
        logger.error("Schema setup incomplete. Please create all required tables.")
        return
    
    # Step 1: Scrape Reddit
    logger.info("\n### STEP 1: SCRAPING REDDIT ###")
    try:
        import reddit_scraper
        logger.info("✓ Reddit scraping complete")
    except Exception as e:
        logger.error(f"Reddit scraping failed: {e}")
        return
    
    # Step 2: NLP Processing
    logger.info("\n### STEP 2: NLP PROCESSING ###")
    try:
        import nlp_processor
        nlp_processor.main()
        logger.info("✓ NLP processing complete")
    except Exception as e:
        logger.error(f"NLP processing failed: {e}")
        return
    
    # Step 3: Entity Linking
    logger.info("\n### STEP 3: ENTITY LINKING ###")
    try:
        from entity_linker import EntityLinker
        linker = EntityLinker()
        linker.run(total_limit=500)
        linker.close()
        logger.info("✓ Entity linking complete")
    except Exception as e:
        logger.error(f"Entity linking failed: {e}")
        return
    
    # Step 4: Aggregation
    logger.info("\n### STEP 4: AGGREGATION ###")
    try:
        from entity_linker import DataAggregator
        aggregator = DataAggregator()
        aggregator.run_all_aggregations()
        aggregator.close()
        logger.info("✓ Aggregation complete")
    except Exception as e:
        logger.error(f"Aggregation failed: {e}")
        return
    
    # Final statistics
    get_table_counts()
    
    logger.info("\n" + "=" * 60)
    logger.info("✓ SETUP COMPLETE!")
    logger.info("=" * 60)
    logger.info("\nNext steps:")
    logger.info("1. Verify data quality in database")
    logger.info("2. Build recommendation engine")
    logger.info("3. Create API layer")
    logger.info("4. Build frontend interface")


if __name__ == "__main__":
    main()