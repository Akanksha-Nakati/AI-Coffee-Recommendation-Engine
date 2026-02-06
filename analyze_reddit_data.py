# analyze_reddit_data.py

import json
from collections import Counter
from datetime import datetime

def analyze_reddit_data(filename):
    """Analyze Reddit scraping quality"""
    
    with open(filename, 'r') as f:
        discussions = json.load(f)
    
    print("📊 Reddit Data Quality Report")
    print("=" * 60)
    
    # Basic stats
    print(f"\n1️⃣ Basic Statistics:")
    print(f"   Total discussions: {len(discussions)}")
    print(f"   Total comments: {sum(len(d['top_comments']) for d in discussions)}")
    
    # Engagement metrics
    scores = [d['score'] for d in discussions]
    print(f"\n2️⃣ Engagement Metrics:")
    print(f"   Min score: {min(scores)}")
    print(f"   Max score: {max(scores)}")
    print(f"   Avg score: {sum(scores)/len(scores):.1f}")
    print(f"   Median score: {sorted(scores)[len(scores)//2]}")
    
    # High-quality posts (score >= 50)
    high_quality = [d for d in discussions if d['score'] >= 50]
    print(f"   High-quality posts (score >= 50): {len(high_quality)} ({len(high_quality)/len(discussions)*100:.1f}%)")
    
    # Content richness
    print(f"\n3️⃣ Content Richness:")
    with_text = [d for d in discussions if d['text']]
    print(f"   Posts with body text: {len(with_text)} ({len(with_text)/len(discussions)*100:.1f}%)")
    
    with_comments = [d for d in discussions if d['top_comments']]
    print(f"   Posts with comments: {len(with_comments)} ({len(with_comments)/len(discussions)*100:.1f}%)")
    
    avg_comments = sum(len(d['top_comments']) for d in discussions) / len(discussions)
    print(f"   Avg comments per post: {avg_comments:.1f}")
    
    # Drink mentions
    print(f"\n4️⃣ Drink Mention Analysis:")
    all_drinks = []
    for d in discussions:
        all_drinks.extend(d.get('mentioned_drinks', []))
    
    drink_counts = Counter(all_drinks)
    print(f"   Unique drinks mentioned: {len(drink_counts)}")
    print(f"   Total drink mentions: {len(all_drinks)}")
    print(f"   Avg mentions per post: {len(all_drinks)/len(discussions):.1f}")
    
    print(f"\n   Top 10 mentioned drinks:")
    for drink, count in drink_counts.most_common(10):
        print(f"      {drink}: {count}")
    
    # Temporal distribution
    print(f"\n5️⃣ Temporal Distribution:")
    post_dates = [datetime.fromisoformat(d['created_utc']) for d in discussions]
    oldest = min(post_dates)
    newest = max(post_dates)
    print(f"   Oldest post: {oldest.strftime('%Y-%m-%d')}")
    print(f"   Newest post: {newest.strftime('%Y-%m-%d')}")
    print(f"   Date range: {(newest - oldest).days} days")
    
    # Subreddit breakdown
    print(f"\n6️⃣ Subreddit Distribution:")
    subreddit_counts = Counter(d['subreddit'] for d in discussions)
    for sub, count in subreddit_counts.most_common():
        pct = count / len(discussions) * 100
        print(f"   r/{sub}: {count} ({pct:.1f}%)")
    
    # Recommendations for improvement
    print(f"\n7️⃣ Data Quality Assessment:")
    issues = []
    
    if len(discussions) < 500:
        issues.append("⚠️ Low total count - consider expanding search queries or subreddits")
    
    if len(high_quality) / len(discussions) < 0.3:
        issues.append("⚠️ Low engagement - many posts have low scores")
    
    if avg_comments < 5:
        issues.append("⚠️ Few comments - might want posts with more discussion")
    
    if len(with_text) / len(discussions) < 0.5:
        issues.append("⚠️ Many posts lack body text - less context for RAG")
    
    if issues:
        for issue in issues:
            print(f"   {issue}")
    else:
        print("   ✅ Data quality looks good!")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    import sys
    
    filename = sys.argv[1] if len(sys.argv) > 1 else 'data/raw/reddit/coffee_discussions_2026-02-05.json'
    analyze_reddit_data(filename)