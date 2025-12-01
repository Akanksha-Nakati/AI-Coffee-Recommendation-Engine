# entity_linker.py
"""
Entity Linking Layer - Connects NLP extractions to database entities
This runs AFTER nlp_processor.py and creates the links between:
- Extracted flavors → flavor_terms table
- Extracted roasters → entities table  
- Extracted origins → origins table
- Reviews → coffee_products (product matching)
"""

import os
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from dotenv import load_dotenv
import logging
from datetime import datetime
from difflib import SequenceMatcher
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError("Set DATABASE_URL")


class EntityLinker:
    """Links extracted entities from NLP to database tables"""
    
    def __init__(self):
        self.conn = psycopg2.connect(DATABASE_URL)
        self.cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        # Cache for lookups (avoid repeated queries)
        self.flavor_cache = {}
        self.entity_cache = {}
        self.origin_cache = {}
        
        # Load lookups into cache
        self._load_caches()
    
    def _load_caches(self):
        """Pre-load reference data into memory for fast lookups"""
        logger.info("Loading reference data into cache...")
        
        # Load all flavors
        self.cur.execute("SELECT id, term, normalized_term, synonyms FROM flavor_terms")
        for row in self.cur.fetchall():
            # Index by term
            self.flavor_cache[row['term'].lower()] = row
            # Index by normalized
            self.flavor_cache[row['normalized_term'].lower()] = row
            # Index by synonyms
            if row['synonyms']:
                for syn in row['synonyms']:
                    self.flavor_cache[syn.lower()] = row
        
        logger.info(f"Loaded {len(self.flavor_cache)} flavor terms")
        
        # Load all entities (roasters, cafes, brands)
        self.cur.execute("SELECT id, name, slug, entity_type FROM entities")
        for row in self.cur.fetchall():
            # Index by name and slug
            self.entity_cache[row['name'].lower()] = row
            self.entity_cache[row['slug'].lower()] = row
        
        logger.info(f"Loaded {len(self.entity_cache)} entities")
        
        # Load all origins
        self.cur.execute("SELECT id, country, region, normalized_name FROM origins")
        for row in self.cur.fetchall():
            key = row['normalized_name'].lower()
            self.origin_cache[key] = row
            # Also index by country alone
            self.origin_cache[row['country'].lower()] = row
        
        logger.info(f"Loaded {len(self.origin_cache)} origins")
    
    # =========================================================================
    # FLAVOR LINKING
    # =========================================================================
    
    def find_flavor_id(self, flavor_term):
        """
        Find flavor_id from flavor_terms table
        
        Args:
            flavor_term: String like "chocolate", "berry", etc.
        
        Returns:
            flavor_id (int) or None
        """
        flavor_term_lower = flavor_term.lower().strip()
        
        # Check cache first
        if flavor_term_lower in self.flavor_cache:
            return self.flavor_cache[flavor_term_lower]['id']
        
        # Not in cache - shouldn't happen if flavor was extracted from known list
        logger.warning(f"Flavor '{flavor_term}' not found in database")
        return None
    
    def link_flavors(self, processed_review_id, post_id, extracted_flavors):
        """
        Link extracted flavors to flavor_terms table via product_flavors
        
        Args:
            processed_review_id: ID from processed_reviews table
            post_id: Original post ID
            extracted_flavors: List of dicts from NLP processor
                [{'term': 'chocolate', 'intensity': 'prominent', 
                  'confidence': 0.85, 'sentiment': 0.7, ...}, ...]
        
        Returns:
            List of created flavor_ids
        """
        if not extracted_flavors:
            return []
        
        linked_flavor_ids = []
        
        for flavor_data in extracted_flavors:
            flavor_term = flavor_data['term']
            flavor_id = self.find_flavor_id(flavor_term)
            
            if not flavor_id:
                logger.warning(f"Skipping unknown flavor: {flavor_term}")
                continue
            
            # Insert into flavor_extractions table
            try:
                self.cur.execute("""
                    INSERT INTO flavor_extractions (
                        processed_review_id,
                        post_id,
                        flavor_term_id,
                        mention_text,
                        sentence_context,
                        intensity,
                        sentiment,
                        confidence_score,
                        is_primary_flavor,
                        mention_order
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                """, (
                    processed_review_id,
                    post_id,
                    flavor_id,
                    flavor_data.get('term'),
                    flavor_data.get('context', '')[:200],  # Limit context length
                    flavor_data.get('intensity', 'moderate'),
                    flavor_data.get('sentiment', 0.0),
                    flavor_data.get('confidence', 0.5),
                    flavor_data.get('is_primary', False),
                    extracted_flavors.index(flavor_data) + 1  # 1-indexed order
                ))
                
                result = self.cur.fetchone()
                if result:
                    linked_flavor_ids.append(flavor_id)
                    logger.debug(f"Linked flavor: {flavor_term} (ID: {flavor_id})")
            
            except Exception as e:
                logger.error(f"Error linking flavor {flavor_term}: {e}")
                continue
        
        return linked_flavor_ids
    
    # =========================================================================
    # ROASTER/ENTITY LINKING
    # =========================================================================
    
    def normalize_name(self, name):
        """Normalize entity name for matching"""
        return name.lower().strip().replace("'", "").replace("-", " ")
    
    def fuzzy_match_score(self, str1, str2):
        """Calculate similarity between two strings (0.0 to 1.0)"""
        return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()
    
    def find_or_create_entity(self, entity_name, entity_type='roaster'):
        """
        Find entity in database or create new one
        
        Args:
            entity_name: String like "Blue Bottle", "Onyx Coffee"
            entity_type: 'roaster', 'cafe', 'brand', 'retailer'
        
        Returns:
            entity_id (int)
        """
        normalized = self.normalize_name(entity_name)
        
        # Check exact match in cache
        if normalized in self.entity_cache:
            return self.entity_cache[normalized]['id']
        
        # Try fuzzy matching against existing entities
        best_match = None
        best_score = 0.0
        
        for cached_name, entity_data in self.entity_cache.items():
            if entity_data['entity_type'] != entity_type:
                continue
            
            score = self.fuzzy_match_score(normalized, cached_name)
            if score > best_score:
                best_score = score
                best_match = entity_data
        
        # If fuzzy match is good enough (>0.85 similarity), use it
        if best_match and best_score > 0.85:
            logger.info(f"Fuzzy matched '{entity_name}' to '{best_match['name']}' (score: {best_score:.2f})")
            return best_match['id']
        
        # No good match - create new entity
        logger.info(f"Creating new {entity_type}: {entity_name}")
        
        slug = entity_name.lower().replace(' ', '-').replace("'", "")
        
        try:
            self.cur.execute("""
                INSERT INTO entities (
                    entity_type, 
                    name, 
                    slug,
                    verified
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (slug) DO UPDATE 
                SET name = EXCLUDED.name
                RETURNING id
            """, (entity_type, entity_name, slug, False))
            
            result = self.cur.fetchone()
            entity_id = result['id']
            
            # Add to cache
            self.entity_cache[normalized] = {
                'id': entity_id,
                'name': entity_name,
                'slug': slug,
                'entity_type': entity_type
            }
            
            return entity_id
        
        except Exception as e:
            logger.error(f"Error creating entity '{entity_name}': {e}")
            self.conn.rollback()
            return None
    
    def link_roasters(self, post_id, extracted_roasters):
        """
        Link extracted roaster mentions to entities table
        
        Args:
            post_id: Original post ID
            extracted_roasters: List of dicts 
                [{'name': 'Blue Bottle', 'context': '...'}, ...]
        
        Returns:
            List of entity_ids
        """
        if not extracted_roasters:
            return []
        
        linked_entity_ids = []
        
        for roaster_data in extracted_roasters:
            roaster_name = roaster_data['name']
            entity_id = self.find_or_create_entity(roaster_name, entity_type='roaster')
            
            if entity_id:
                # Create coffee_mention record
                try:
                    self.cur.execute("""
                        INSERT INTO coffee_mentions (
                            post_id,
                            roaster_id,
                            mention_text,
                            mention_context,
                            confidence_score
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        post_id,
                        entity_id,
                        roaster_name,
                        roaster_data.get('context', '')[:200],
                        0.7  # Default confidence for roaster mentions
                    ))
                    
                    linked_entity_ids.append(entity_id)
                    logger.debug(f"Linked roaster: {roaster_name} (ID: {entity_id})")
                
                except Exception as e:
                    logger.error(f"Error linking roaster {roaster_name}: {e}")
                    continue
        
        return linked_entity_ids
    
    # =========================================================================
    # ORIGIN LINKING
    # =========================================================================
    
    def find_or_create_origin(self, country, region=None):
        """
        Find or create origin in origins table
        
        Args:
            country: Country name like "Ethiopia", "Colombia"
            region: Optional region like "Yirgacheffe", "Huila"
        
        Returns:
            origin_id (int)
        """
        # Create normalized name
        if region:
            normalized = f"{country.lower()}_{region.lower()}"
        else:
            normalized = f"{country.lower()}_general"
        
        # Check cache
        if normalized in self.origin_cache:
            return self.origin_cache[normalized]['id']
        
        # Create new origin
        try:
            self.cur.execute("""
                INSERT INTO origins (
                    country,
                    region,
                    normalized_name
                ) VALUES (%s, %s, %s)
                ON CONFLICT (normalized_name) DO UPDATE
                SET country = EXCLUDED.country
                RETURNING id
            """, (country.title(), region.title() if region else None, normalized))
            
            result = self.cur.fetchone()
            origin_id = result['id']
            
            # Add to cache
            self.origin_cache[normalized] = {
                'id': origin_id,
                'country': country,
                'region': region,
                'normalized_name': normalized
            }
            
            return origin_id
        
        except Exception as e:
            logger.error(f"Error creating origin {country}/{region}: {e}")
            self.conn.rollback()
            return None
    
    def link_origins(self, extracted_origins):
        """
        Link extracted origin mentions to origins table
        
        Args:
            extracted_origins: List of country names ['ethiopia', 'colombia']
        
        Returns:
            List of origin_ids
        """
        if not extracted_origins:
            return []
        
        linked_origin_ids = []
        
        for origin_name in extracted_origins:
            origin_id = self.find_or_create_origin(origin_name)
            if origin_id:
                linked_origin_ids.append(origin_id)
                logger.debug(f"Linked origin: {origin_name} (ID: {origin_id})")
        
        return linked_origin_ids
    
    # =========================================================================
    # MAIN PROCESSING
    # =========================================================================
    
    def get_unlinked_reviews(self, limit=100):
        """Get processed reviews that haven't been entity-linked yet"""
        self.cur.execute("""
            SELECT 
                pr.id,
                pr.post_id,
                pr.cleaned_text,
                pr.sentiment_score
            FROM processed_reviews pr
            WHERE NOT EXISTS (
                SELECT 1 FROM flavor_extractions fe 
                WHERE fe.processed_review_id = pr.id
            )
            ORDER BY pr.processed_at DESC
            LIMIT %s
        """, (limit,))
        
        return self.cur.fetchall()
    
    def _get_extracted_data(self, processed_review_id):
        """
        Get extracted data from NLP processor via nlp_extractions table
        
        Args:
            processed_review_id: ID from processed_reviews table
        
        Returns:
            Dict with extracted flavors, roasters, origins, etc.
            or None if no extraction found
        """
        try:
            self.cur.execute("""
                SELECT 
                    flavors,
                    roasters,
                    origins,
                    brew_methods,
                    process_methods,
                    price,
                    keywords
                FROM nlp_extractions
                WHERE processed_review_id = %s
            """, (processed_review_id,))
            
            row = self.cur.fetchone()
            
            if not row:
                logger.warning(f"No NLP extractions found for review {processed_review_id}")
                return None
            
            # Parse JSON fields if they're stored as strings
            def parse_json_field(field_value):
                if field_value is None:
                    return []
                if isinstance(field_value, str):
                    return json.loads(field_value)
                return field_value  # Already parsed by psycopg2
            
            extracted_data = {
                'flavors': parse_json_field(row['flavors']),
                'roasters': parse_json_field(row['roasters']),
                'origins': parse_json_field(row['origins']),
                'brew_methods': parse_json_field(row['brew_methods']),
                'process_methods': parse_json_field(row['process_methods']),
                'price': row['price'],
                'keywords': parse_json_field(row['keywords'])
            }
            
            logger.debug(f"Retrieved extraction for review {processed_review_id}: "
                        f"{len(extracted_data['flavors'])} flavors, "
                        f"{len(extracted_data['roasters'])} roasters")
            
            return extracted_data
        
        except Exception as e:
            logger.error(f"Error retrieving extracted data for {processed_review_id}: {e}")
            return None
    
    def link_single_review(self, review):
        """
        Process a single review and create all entity links
        
        Args:
            review: Dict with processed review data
        
        Returns:
            Dict with linking statistics
        """
        processed_review_id = review['id']
        post_id = review['post_id']
        
        logger.info(f"Linking review {post_id}...")
        
        stats = {
            'flavors_linked': 0,
            'roasters_linked': 0,
            'origins_linked': 0,
            'products_linked': 0
        }
        
        try:
            # Get extracted data
            extracted_data = self._get_extracted_data(processed_review_id)
            
            if not extracted_data:
                logger.warning(f"No extracted data found for review {processed_review_id}")
                return stats
            
            # Link flavors
            flavor_ids = self.link_flavors(
                processed_review_id,
                post_id,
                extracted_data.get('flavors', [])
            )
            stats['flavors_linked'] = len(flavor_ids)
            
            # Link roasters
            roaster_ids = self.link_roasters(
                post_id,
                extracted_data.get('roasters', [])
            )
            stats['roasters_linked'] = len(roaster_ids)
            
            # Link origins
            origin_ids = self.link_origins(
                extracted_data.get('origins', [])
            )
            stats['origins_linked'] = len(origin_ids)
            
            # Update processed_reviews with linked entity IDs
            self.cur.execute("""
                UPDATE processed_reviews
                SET mentioned_flavor_ids = %s,
                    mentioned_roaster_ids = %s,
                    mentioned_origins = %s
                WHERE id = %s
            """, (
                flavor_ids,
                roaster_ids,
                [str(oid) for oid in origin_ids],  # Store as text array
                processed_review_id
            ))
            
            return stats
        
        except Exception as e:
            logger.error(f"Error linking review {post_id}: {e}", exc_info=True)
            self.conn.rollback()
            return stats
    
    def process_batch(self, batch_size=50):
        """Process a batch of unlinked reviews"""
        reviews = self.get_unlinked_reviews(limit=batch_size)
        
        if not reviews:
            logger.info("No unlinked reviews to process")
            return 0
        
        logger.info(f"Processing {len(reviews)} unlinked reviews...")
        
        total_stats = {
            'flavors_linked': 0,
            'roasters_linked': 0,
            'origins_linked': 0,
            'products_linked': 0
        }
        
        for i, review in enumerate(reviews, 1):
            logger.info(f"Processing {i}/{len(reviews)}")
            
            stats = self.link_single_review(review)
            
            # Accumulate stats
            for key in total_stats:
                total_stats[key] += stats[key]
            
            # Commit every 10 reviews
            if i % 10 == 0:
                self.conn.commit()
                logger.info(f"Committed batch at {i} reviews")
        
        # Final commit
        self.conn.commit()
        
        logger.info(f"Linking complete: {total_stats}")
        return len(reviews)
    
    def run(self, total_limit=None):
        """Run entity linking until no more unlinked reviews"""
        total_processed = 0
        batch_size = 50
        
        while True:
            processed = self.process_batch(batch_size)
            total_processed += processed
            
            if processed == 0:
                break
            
            if total_limit and total_processed >= total_limit:
                logger.info(f"Reached limit of {total_limit} reviews")
                break
        
        logger.info(f"Total reviews linked: {total_processed}")
        return total_processed
    
    def close(self):
        """Clean up resources"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()


# =============================================================================
# AGGREGATION LAYER
# =============================================================================

class DataAggregator:
    """Aggregates linked data into summary metrics"""
    
    def __init__(self):
        self.conn = psycopg2.connect(DATABASE_URL)
        self.cur = self.conn.cursor(cursor_factory=RealDictCursor)
    
    def update_product_metrics(self):
        """Update aggregated metrics for all products"""
        logger.info("Updating product metrics...")
        
        # Update avg_rating, review_count, mention_count for each product
        self.cur.execute("""
            UPDATE coffee_products cp
            SET 
                total_ratings = COALESCE(review_counts.count, 0),
                avg_rating = COALESCE(review_counts.avg_rating, 0),
                total_reviews = COALESCE(review_counts.count, 0),
                mention_count = COALESCE(mention_counts.count, 0),
                updated_at = NOW()
            FROM (
                SELECT 
                    product_id,
                    COUNT(*) as count,
                    AVG(overall_rating) as avg_rating
                FROM product_reviews
                WHERE overall_rating IS NOT NULL
                GROUP BY product_id
            ) AS review_counts
            LEFT JOIN (
                SELECT 
                    coffee_product_id,
                    COUNT(*) as count
                FROM coffee_mentions
                WHERE coffee_product_id IS NOT NULL
                GROUP BY coffee_product_id
            ) AS mention_counts ON review_counts.product_id = mention_counts.coffee_product_id
            WHERE cp.id = review_counts.product_id
        """)
        
        updated = self.cur.rowcount
        self.conn.commit()
        logger.info(f"Updated metrics for {updated} products")
        return updated
    
    def update_flavor_popularity(self):
        """Update flavor mention counts and rankings"""
        logger.info("Updating flavor popularity...")
        
        self.cur.execute("""
            UPDATE flavor_terms ft
            SET 
                total_mentions = COALESCE(counts.mention_count, 0)
            FROM (
                SELECT 
                    flavor_term_id,
                    COUNT(*) as mention_count
                FROM flavor_extractions
                WHERE confidence_score > 0.5
                GROUP BY flavor_term_id
            ) AS counts
            WHERE ft.id = counts.flavor_term_id
        """)
        
        updated = self.cur.rowcount
        self.conn.commit()
        logger.info(f"Updated {updated} flavor popularity metrics")
        return updated
    
    def update_entity_metrics(self):
        """Update metrics for roasters/entities"""
        logger.info("Updating entity metrics...")
        
        # Update product count and average rating for entities (roasters)
        self.cur.execute("""
            UPDATE entities e
            SET 
                product_count = COALESCE(counts.count, 0),
                avg_product_rating = COALESCE(counts.avg_rating, 0),
                total_reviews = COALESCE(review_counts.count, 0),
                updated_at = NOW()
            FROM (
                SELECT 
                    entity_id,
                    COUNT(*) as count,
                    AVG(avg_rating) as avg_rating
                FROM coffee_products
                WHERE entity_id IS NOT NULL
                GROUP BY entity_id
            ) AS counts
            LEFT JOIN (
                SELECT 
                    cm.roaster_id,
                    COUNT(DISTINCT cm.post_id) as count
                FROM coffee_mentions cm
                WHERE cm.roaster_id IS NOT NULL
                GROUP BY cm.roaster_id
            ) AS review_counts ON counts.entity_id = review_counts.roaster_id
            WHERE e.id = counts.entity_id
        """)
        
        updated = self.cur.rowcount
        self.conn.commit()
        logger.info(f"Updated metrics for {updated} entities")
        return updated
    
    def compute_flavor_rankings(self, period='month'):
        """
        Compute flavor rankings for a time period
        
        Args:
            period: 'week', 'month', 'year'
        """
        logger.info(f"Computing flavor rankings for period: {period}")
        
        # Determine date range
        if period == 'week':
            interval = '7 days'
        elif period == 'month':
            interval = '30 days'
        else:
            interval = '365 days'
        
        # Insert/update rankings
        self.cur.execute(f"""
            INSERT INTO flavor_rankings (
                flavor_id,
                period_start,
                period_end,
                period_type,
                mention_count,
                unique_products,
                avg_sentiment,
                positive_mentions,
                negative_mentions
            )
            SELECT 
                fe.flavor_term_id,
                CURRENT_DATE - INTERVAL '{interval}' as period_start,
                CURRENT_DATE as period_end,
                %s as period_type,
                COUNT(*) as mention_count,
                COUNT(DISTINCT pr.post_id) as unique_products,
                AVG(fe.sentiment) as avg_sentiment,
                COUNT(*) FILTER (WHERE fe.sentiment > 0.3) as positive_mentions,
                COUNT(*) FILTER (WHERE fe.sentiment < -0.3) as negative_mentions
            FROM flavor_extractions fe
            JOIN processed_reviews pr ON fe.processed_review_id = pr.id
            WHERE fe.created_at >= CURRENT_DATE - INTERVAL '{interval}'
              AND fe.confidence_score > 0.5
            GROUP BY fe.flavor_term_id
            ON CONFLICT (flavor_id, period_start, period_end, period_type)
            DO UPDATE SET
                mention_count = EXCLUDED.mention_count,
                avg_sentiment = EXCLUDED.avg_sentiment,
                computed_at = NOW()
        """, (period,))
        
        # Add rankings
        self.cur.execute(f"""
            UPDATE flavor_rankings fr
            SET popularity_rank = ranked.rank
            FROM (
                SELECT 
                    id,
                    RANK() OVER (ORDER BY mention_count DESC) as rank
                FROM flavor_rankings
                WHERE period_type = %s
                  AND period_end = CURRENT_DATE
            ) AS ranked
            WHERE fr.id = ranked.id
        """, (period,))
        
        self.conn.commit()
        logger.info(f"Computed {period} flavor rankings")
    
    def run_all_aggregations(self):
        """Run all aggregation tasks"""
        logger.info("=== Starting aggregation ===")
        
        self.update_flavor_popularity()
        self.update_entity_metrics()
        self.compute_flavor_rankings('week')
        self.compute_flavor_rankings('month')
        
        logger.info("=== Aggregation complete ===")
    
    def close(self):
        """Clean up"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main entry point"""
    
    # Step 1: Entity Linking
    logger.info("=" * 60)
    logger.info("STEP 3: ENTITY LINKING")
    logger.info("=" * 60)
    
    linker = EntityLinker()
    
    try:
        # Process unlinked reviews
        linker.run(total_limit=500)
        
        # Show statistics
        linker.cur.execute("""
            SELECT 
                COUNT(DISTINCT fe.processed_review_id) as reviews_with_flavors,
                COUNT(DISTINCT cm.post_id) as reviews_with_roasters
            FROM processed_reviews pr
            LEFT JOIN flavor_extractions fe ON pr.id = fe.processed_review_id
            LEFT JOIN coffee_mentions cm ON pr.post_id = cm.post_id
        """)
        
        result = linker.cur.fetchone()
        print("\n=== Entity Linking Statistics ===")
        print(f"Reviews with flavors: {result['reviews_with_flavors']}")
        print(f"Reviews with roasters: {result['reviews_with_roasters']}")
    
    finally:
        linker.close()
    
    # Step 2: Aggregation
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4: AGGREGATION")
    logger.info("=" * 60)
    
    aggregator = DataAggregator()
    
    try:
        aggregator.run_all_aggregations()
        
        # Show results
        aggregator.cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE total_mentions > 0) as flavors_with_mentions
            FROM flavor_terms
        """)
        
        result = aggregator.cur.fetchone()
        print("\n=== Aggregation Results ===")
        print(f"Flavors with mentions: {result['flavors_with_mentions']}")
        
        # Top flavors
        aggregator.cur.execute("""
            SELECT ft.term, ft.total_mentions
            FROM flavor_terms ft
            WHERE ft.total_mentions > 0
            ORDER BY ft.total_mentions DESC
            LIMIT 10
        """)
        
        print("\n=== Top 10 Flavors ===")
        for row in aggregator.cur.fetchall():
            print(f"  {row['term']}: {row['total_mentions']} mentions")
    
    finally:
        aggregator.close()
    
    print("\n✅ Entity linking and aggregation complete!")


if __name__ == "__main__":
    main()