import os
import psycopg2
from neo4j import GraphDatabase

# --- Load environment variables ---
DATABASE_URL = os.getenv("DATABASE_URL")
from neo4j import GraphDatabase

# Example for Neo4j Aura Cloud:
NEO4J_URI = "neo4j+s://9ff7922d.databases.neo4j.io:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "U-O3BvT-av42rUl6x083punjTUsatdmNJQ090m365Sc"

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD)
)

# --- Connect to PostgreSQL ---
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found. Set it using: export DATABASE_URL='postgresql://user:password@host:port/dbname'")

pg_conn = psycopg2.connect(DATABASE_URL)
pg_cursor = pg_conn.cursor()

# ✅ Use correct table name
pg_cursor.execute("SELECT raw_id, text, sentiment FROM processed_reviews WHERE sentiment IS NOT NULL;")

rows = pg_cursor.fetchall()

# --- Connect to Neo4j ---
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def load_to_neo4j(tx, record_id, text, sentiment):
    tx.run("""
        MERGE (r:Review {id: $id})
        SET r.text = $text, r.sentiment = $sentiment
        MERGE (s:Sentiment {type: $sentiment})
        MERGE (r)-[:HAS_SENTIMENT]->(s)
    """, id=record_id, text=text, sentiment=sentiment)

# --- Insert into Neo4j ---
with driver.session() as session:
    for record_id, text, sentiment in rows:
        session.execute_write(load_to_neo4j, record_id, text, sentiment)

# --- Close connections ---
pg_cursor.close()
pg_conn.close()
driver.close()

print(f"✅ Loaded {len(rows)} reviews with sentiment into Neo4j.")
