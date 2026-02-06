# view_all_data.py

import json
import os
from glob import glob

def view_all_data():
    """View summary of all scraped data"""
    
    print("📦 Complete Data Inventory")
    print("=" * 60)
    
    # Café data
    print("\n☕ Café Menu Data:")
    
    cafe_files = {
        'Starbucks': glob('data/raw/starbucks/*.json'),
        'Dunkin': glob('data/raw/dunkin/*.json')
    }
    
    total_drinks = 0
    for cafe, files in cafe_files.items():
        if files:
            with open(files[0], 'r') as f:
                drinks = json.load(f)
                count = len(drinks)
                total_drinks += count
                print(f"   {cafe}: {count} drinks")
                
                # Sample drink
                if drinks:
                    sample = drinks[0]
                    print(f"      Sample: {sample['name']}")
        else:
            print(f"   {cafe}: No data yet")
    
    print(f"\n   Total drinks: {total_drinks}")
    
    # Reddit data
    print("\n💬 Reddit Discussion Data:")
    
    reddit_files = glob('data/raw/reddit/*.json')
    if reddit_files:
        with open(reddit_files[0], 'r') as f:
            discussions = json.load(f)
            print(f"   Discussions: {len(discussions)}")
            print(f"   Total comments: {sum(len(d['top_comments']) for d in discussions)}")
            
            # Sample discussion
            if discussions:
                sample = discussions[0]
                print(f"   Sample: {sample['title'][:60]}...")
    else:
        print("   No data yet")
    
    # Overall summary
    print(f"\n📊 Overall Summary:")
    print(f"   ✅ Café drinks collected: {total_drinks}")
    print(f"   ✅ Reddit discussions: {len(discussions) if reddit_files else 0}")
    print(f"   ✅ Total data points: {total_drinks + (len(discussions) if reddit_files else 0)}")
    
    # Readiness check
    print(f"\n🎯 Pipeline Readiness:")
    checks = {
        'Café data (100+)': total_drinks >= 100,
        'Reddit data (500+)': (len(discussions) if reddit_files else 0) >= 500,
        'Multiple cafés': len([c for c, f in cafe_files.items() if f]) >= 2
    }
    
    for check, passed in checks.items():
        status = "✅" if passed else "⚠️"
        print(f"   {status} {check}")
    
    if all(checks.values()):
        print(f"\n🚀 Ready for Databricks processing!")
    else:
        print(f"\n📝 Consider collecting more data before processing")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    view_all_data()