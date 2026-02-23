# ☕ AI Coffee Recommendation Engine

An AI-powered recommendation system that combines real menu data from Starbucks and Dunkin' with authentic Reddit community discussions to surface personalized coffee suggestions — not based on star ratings or sponsored content, but on how real people talk about coffee.

---

## What It Does

Tell it what you're in the mood for — *"something light and fruity for cold brew"* or *"a strong espresso drink that isn't too sweet"* — and it returns recommendations grounded in community sentiment, matched using semantic search over vector embeddings.

---

## Highlights

- **Multi-source data** — scrapes Starbucks and Dunkin' menus alongside Reddit discussions for a rich, blended knowledge base
- **Semantic search** — uses ChromaDB (or FAISS) vector indexes to match natural language queries to relevant community insights
- **Streamlit UI** — clean interface for recommendations and community-sourced coffee insights
- **Airflow orchestration** — scheduled DAGs keep scraped data and embeddings fresh

---

## Tech Stack

| Layer | Tools |
|---|---|
| Data Collection | Custom scrapers (Starbucks, Dunkin', Reddit/PRAW) |
| Embeddings & Search | ChromaDB, FAISS, Sentence Transformers |
| App Interface | Streamlit |
| Orchestration | Apache Airflow (Dockerized) |
| Language | Python 3.12 |

---

## Project Structure

```
AI-Coffee-Recommendation-Engine/
├── app/
│   ├── streamlit_app.py          # Main Streamlit UI
│   └── requirements.txt
├── local_scrapers/
│   ├── scrape_starbucks.py
│   ├── scrape_dunkin.py
│   └── scrape_reddit.py
├── data/
│   ├── embeddings/               # Embeddings + customization CSVs
│   └── vector_index/             # ChromaDB index files
├── airflow/                      # Docker Compose + DAG definitions
├── docs/
│   └── airflow_setup.md
├── build_chroma_index.py         # Build ChromaDB vector index
├── build_faiss_from_csv.py       # FAISS index builder (optional)
└── run_app.py                    # App entrypoint
```

---
## Author

**Akanksha Nakati**  
Data Engineer · AI/ML Enthusiast  
[GitHub](https://github.com/Akanksha-Nakati) · [LinkedIn](https://linkedin.com/in/akanksha-nakati)
