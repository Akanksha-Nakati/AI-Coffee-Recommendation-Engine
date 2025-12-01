"""
Enhanced NLP Processor - Works with raw_posts table
Extracts structured data for entity linking
"""

import os
import json
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import spacy
from textblob import TextBlob
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError("Set DATABASE_URL")

nlp = spacy.load("en_core_web_sm")

# Enhanced flavor lexicon with intensity markers
FLAVOR_KEYWORDS = {
    "chocolate", "chocolatey", "cocoa",
    "blueberry", "berry", "berries",
    "caramel", "toffee",
    "citrus", "lemon", "orange", "lime",
    "floral", "jasmine", "rose",
    "nutty", "almond", "hazelnut", "walnut",
    "earthy", "woody",
    "fruity", "fruit",
    "bright", "vibrant",
    "acidic", "acidity",
    "bitter", "bitterness",
    "sweet", "sweetness"
}

# Intensity indicators
INTENSITY_MARKERS = {
    'prominent': ['strong', 'intense', 'heavy', 'prominent', 'dominant', 'bold'],
    'moderate': ['moderate', 'balanced', 'medium', 'noticeable'],
    'subtle': ['subtle', 'light', 'hint', 'touch', 'slight', 'delicate']
}

BREW_KEYWORDS = {
    "pour over", "v60", "aeropress", "espresso", 
    "french press", "cold brew", "drip", "chemex", 
    "espresso machine", "moka pot", "siphon"
}

# Common roaster patterns (this should be expanded)
KNOWN_ROASTERS = {
    "blue bottle", "onyx", "counter culture", "stumptown",
    "intelligentsia", "verve", "heart", "coava",
    "klatch", "ritual", "sight glass", "george howell"
}

# Coffee origin countries
ORIGIN_COUNTRIES = {
    "ethiopia", "ethiopian", "kenya", "kenyan",
    "colombia", "colombian", "brazil", "brazilian",
    "guatemala", "guatemalan", "costa rica", "costa rican",
    "rwanda", "rwandan", "burundi", "burundian",
    "yemen", "yemeni", "tanzania", "tanzanian",
    "peru", "peruvian", "honduras", "honduran"
}

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor(cursor_factory=RealDictCursor)


def clean_text(text):
    """Clean and normalize text"""
    # Remove URLs
    text = re.sub(r'http\S+|www.\S+', '', text)
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def extract_flavors_with_context(text, doc):
    """
    Extract flavors with intensity, context, and confidence
    Returns: List of dicts with structure entity_linker expects
    """
    text_lower = text.lower()
    sentences = list(doc.sents)
    
    flavors = []
    
    for flavor in FLAVOR_KEYWORDS:
        if flavor not in text_lower:
            continue
        
        # Find which sentence contains the flavor
        context = ""
        for sent in sentences:
            if flavor in sent.text.lower():
                context = sent.text
                break
        
        # Determine intensity based on surrounding words
        intensity = 'moderate'  # default
        for level, markers in INTENSITY_MARKERS.items():
            for marker in markers:
                if marker in context.lower():
                    intensity = level
                    break
        
        # Get sentiment for the context
        sentiment = TextBlob(context).sentiment.polarity if context else 0.0
        
        # Confidence based on context length and sentiment clarity
        confidence = 0.6  # base confidence
        if context and len(context) > 20:
            confidence = 0.75
        if abs(sentiment) > 0.5:
            confidence = min(1.0, confidence + 0.1)
        
        flavors.append({
            'term': flavor,
            'intensity': intensity,
            'confidence': round(confidence, 2),
            'sentiment': round(sentiment, 2),
            'context': context[:200],  # Limit context length
            'is_primary': False  # Will be determined later
        })
    
    # Mark most mentioned flavors as primary
    if flavors:
        # Sort by confidence and mark top ones as primary
        flavors.sort(key=lambda x: x['confidence'], reverse=True)
        for i in range(min(3, len(flavors))):
            flavors[i]['is_primary'] = True
    
    return flavors


def extract_roasters(text):
    """
    Extract roaster mentions with context
    Returns: List of dicts with roaster info
    """
    text_lower = text.lower()
    roasters = []
    
    for roaster in KNOWN_ROASTERS:
        if roaster in text_lower:
            # Find context (sentence containing roaster)
            sentences = text.split('.')
            context = ""
            for sent in sentences:
                if roaster in sent.lower():
                    context = sent.strip()
                    break
            
            roasters.append({
                'name': roaster.title(),
                'context': context[:200]
            })
    
    # Also look for capitalized phrases that might be roasters
    # Pattern: 2-3 capitalized words
    pattern = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\s+Coffee\b'
    matches = re.findall(pattern, text)
    
    for match in matches:
        if match.lower() not in [r['name'].lower() for r in roasters]:
            roasters.append({
                'name': match,
                'context': ''
            })
    
    return roasters


def extract_origins(text):
    """
    Extract coffee origin countries
    Returns: List of country names
    """
    text_lower = text.lower()
    origins = []
    
    for origin in ORIGIN_COUNTRIES:
        if origin in text_lower:
            # Normalize to base country name
            country = origin.replace('n', '') if origin.endswith('n') else origin
            country = country.title()
            if country not in origins:
                origins.append(country)
    
    return origins


def extract_brew_methods(text):
    """Extract brewing methods mentioned"""
    text_lower = text.lower()
    methods = []
    
    for method in BREW_KEYWORDS:
        if method in text_lower:
            methods.append(method)
    
    return methods


def extract_price(text):
    """Extract price mentions"""
    # Pattern: $XX or $XX.XX
    pattern = r'\$\s?(\d+(?:\.\d{1,2})?)'
    matches = re.findall(pattern, text)
    
    if matches:
        try:
            return float(matches[0])
        except ValueError:
            return None
    return None


def extract_keywords(doc, top_n=20):
    """Extract key nouns and proper nouns"""
    keywords = []
    for token in doc:
        if token.pos_ in ('NOUN', 'PROPN') and not token.is_stop:
            keywords.append(token.lemma_.lower())
    
    # Count frequency and return top N
    from collections import Counter
    counter = Counter(keywords)
    return [word for word, count in counter.most_common(top_n)]


def process_single_post(post):
    """
    Process a single raw post through NLP pipeline
    Returns: processed_review_id and extracted_data dict
    """
    post_id = post['id']
    
    # raw_posts has proper columns (no JSON parsing needed)
    text = f"{post.get('title', '')}\n{post.get('body', '')}".strip()
    
    if not text or len(text) < 10:
        logger.warning(f"Skipping post {post_id}: insufficient text")
        return None, None
    
    logger.info(f"Processing post {post_id}...")
    
    # Clean text
    cleaned_text = clean_text(text)
    
    # Overall sentiment
    sentiment_score = TextBlob(cleaned_text).sentiment.polarity
    
    # SpaCy processing
    doc = nlp(cleaned_text.lower())
    word_count = len([token for token in doc if not token.is_space])
    
    # Extract entities
    flavors = extract_flavors_with_context(cleaned_text, doc)
    roasters = extract_roasters(text)  # Use original text for capitalization
    origins = extract_origins(cleaned_text)
    brew_methods = extract_brew_methods(cleaned_text)
    price = extract_price(text)
    keywords = extract_keywords(doc)
    
    # Insert into processed_reviews
    try:
        cur.execute("""
            INSERT INTO processed_reviews (
                post_id,
                cleaned_text,
                sentiment_score,
                language,
                word_count,
                processed_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (post_id) DO UPDATE SET
                cleaned_text = EXCLUDED.cleaned_text,
                sentiment_score = EXCLUDED.sentiment_score,
                processed_at = NOW()
            RETURNING id
        """, (
            post_id,
            cleaned_text,
            sentiment_score,
            'en',
            word_count,
            datetime.utcnow()
        ))
        
        result = cur.fetchone()
        processed_review_id = result['id']
        
        # Store extracted data in nlp_extractions table
        extracted_data = {
            'flavors': flavors,
            'roasters': roasters,
            'origins': origins,
            'brew_methods': brew_methods,
            'process_methods': [],  # TODO: Extract process methods
            'price': price,
            'keywords': keywords
        }
        
        cur.execute("""
            INSERT INTO nlp_extractions (
                processed_review_id,
                post_id,
                flavors,
                roasters,
                origins,
                brew_methods,
                process_methods,
                price,
                keywords
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (processed_review_id) DO UPDATE SET
                flavors = EXCLUDED.flavors,
                roasters = EXCLUDED.roasters,
                origins = EXCLUDED.origins,
                brew_methods = EXCLUDED.brew_methods,
                price = EXCLUDED.price,
                keywords = EXCLUDED.keywords
        """, (
            processed_review_id,
            post_id,
            json.dumps(flavors),
            json.dumps(roasters),
            json.dumps(origins),
            json.dumps(brew_methods),
            json.dumps([]),
            price,
            json.dumps(keywords)
        ))
        
        conn.commit()
        
        logger.info(f"✓ Processed post {post_id}: "
                   f"{len(flavors)} flavors, "
                   f"{len(roasters)} roasters, "
                   f"{len(origins)} origins")
        
        return processed_review_id, extracted_data
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing post {post_id}: {e}", exc_info=True)
        return None, None


def process_batch(limit=100):
    """Process a batch of unprocessed posts"""
    
    # Find posts that haven't been processed yet from raw_posts
    cur.execute("""
        SELECT rp.*
        FROM raw_posts rp
        LEFT JOIN processed_reviews pr ON rp.id = pr.post_id
        WHERE pr.id IS NULL
        ORDER BY rp.scraped_at DESC
        LIMIT %s
    """, (limit,))
    
    posts = cur.fetchall()
    
    if not posts:
        logger.info("No unprocessed posts found")
        return 0
    
    logger.info(f"Processing {len(posts)} posts...")
    
    processed_count = 0
    for post in posts:
        result_id, _ = process_single_post(post)
        if result_id:
            processed_count += 1
    
    logger.info(f"✓ Processed {processed_count}/{len(posts)} posts")
    return processed_count


def main():
    """Main entry point"""
    logger.info("=" * 60)
    logger.info("STEP 2: NLP PROCESSING")
    logger.info("=" * 60)
    
    total_processed = 0
    
    while True:
        count = process_batch(limit=50)
        total_processed += count
        
        if count == 0:
            break
    
    logger.info(f"\n✓ Total posts processed: {total_processed}")
    
    # Show statistics
    cur.execute("""
        SELECT 
            COUNT(*) as total_processed,
            AVG(sentiment_score)::NUMERIC(10,2) as avg_sentiment,
            COUNT(*) FILTER (WHERE sentiment_score > 0.3) as positive_reviews,
            COUNT(*) FILTER (WHERE sentiment_score < -0.3) as negative_reviews
        FROM processed_reviews
    """)
    
    stats = cur.fetchone()
    print("\n=== Processing Statistics ===")
    print(f"Total processed: {stats['total_processed']}")
    print(f"Average sentiment: {stats['avg_sentiment']}")
    print(f"Positive reviews: {stats['positive_reviews']}")
    print(f"Negative reviews: {stats['negative_reviews']}")
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()